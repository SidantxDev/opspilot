import httpx
import pytest

from opspilot.llm import GeminiLLM


def make(handler):
    return GeminiLLM(model="gemini-test", client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_gemini_request_shape_and_usage_parsing():
    seen = {}

    def handler(req):
        seen["url"], seen["key"], seen["body"] = str(req.url), req.headers.get("x-goog-api-key"), req.read()
        return httpx.Response(200, json={
            "candidates": [{"content": {"parts": [{"text": '{"action": "escalate"}'}]}}],
            "usageMetadata": {"promptTokenCount": 120, "candidatesTokenCount": 30, "thoughtsTokenCount": 50}})

    llm = make(handler)
    llm.key = "SECRET-KEY"
    r = llm.complete("sys", "user text")
    assert r.text == '{"action": "escalate"}'
    assert (r.tokens_in, r.tokens_out) == (120, 80)          # thinking tokens are billed as output
    assert "SECRET-KEY" not in seen["url"] and seen["key"] == "SECRET-KEY"
    assert b"systemInstruction" in seen["body"] and b"application/json" in seen["body"]


def test_gemini_http_error_is_raised_without_leaking_the_key():
    llm = make(lambda req: httpx.Response(429, text="quota exceeded"))
    llm.key = "SECRET-KEY"
    with pytest.raises(RuntimeError) as e:
        llm.complete("s", "u")
    assert "429" in str(e.value) and "SECRET-KEY" not in str(e.value)


def test_gemini_blocked_response_yields_empty_text_not_a_crash():
    llm = make(lambda req: httpx.Response(200, json={"promptFeedback": {"blockReason": "SAFETY"}}))
    llm.key = "k"
    assert llm.complete("s", "u").text == ""

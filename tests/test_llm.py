import httpx
import pytest

from opspilot.llm import GeminiLLM


def make(handler):
    return GeminiLLM(model="gemini-test", client=httpx.Client(transport=httpx.MockTransport(handler)), backoff_s=0)


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


def test_gemini_retries_transient_failures_but_not_client_errors():
    calls = []

    def flaky(req):
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ReadTimeout("slow")
        if len(calls) == 2:
            return httpx.Response(503, text="overloaded")
        return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "{}"}]}}]})

    assert make(flaky).complete("s", "u").text == "{}" and len(calls) == 3

    calls.clear()
    with pytest.raises(RuntimeError):
        make(lambda req: calls.append(1) or httpx.Response(400, text="bad key")).complete("s", "u")
    assert len(calls) == 1


def test_daily_quota_fails_fast_without_retrying():
    from opspilot.llm import QuotaExhausted
    calls = []
    body = '{"error": {"code": 429, "details": [{"quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]}}'
    with pytest.raises(QuotaExhausted):
        make(lambda req: calls.append(1) or httpx.Response(429, text=body)).complete("s", "u")
    assert len(calls) == 1

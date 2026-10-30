"""LLM providers behind one tiny interface.

GeminiLLM and AnthropicLLM are real models. HeuristicLLM is a deterministic keyword baseline: it lets the whole system (and
CI) run offline, and gives the eval harness a floor that a real model has to beat.
"""
import json
import os
import re
import time
from dataclasses import dataclass

# USD per million tokens. These are estimates for cost tracking only; override with env vars.
PRICE_IN = float(os.environ.get("OPSPILOT_PRICE_IN", "0.3"))
PRICE_OUT = float(os.environ.get("OPSPILOT_PRICE_OUT", "2.5"))


@dataclass
class LLMResult:
    text: str
    tokens_in: int
    tokens_out: int
    model: str

    @property
    def cost_usd(self):
        return (self.tokens_in * PRICE_IN + self.tokens_out * PRICE_OUT) / 1e6


class AnthropicLLM:
    def __init__(self, model=None):
        import anthropic
        self.client = anthropic.Anthropic()
        self.model = model or os.environ.get("OPSPILOT_MODEL", "claude-sonnet-5-5")

    def complete(self, system, user, max_tokens=1024):
        r = self.client.messages.create(model=self.model, max_tokens=max_tokens, system=system,
                                        messages=[{"role": "user", "content": user}])
        text = "".join(b.text for b in r.content if b.type == "text")
        return LLMResult(text, r.usage.input_tokens, r.usage.output_tokens, self.model)


class GeminiLLM:
    """Google Gemini over the plain REST API (no SDK). Key from GEMINI_API_KEY (or GOOGLE_API_KEY)."""
    URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    RETRYABLE = {429, 500, 502, 503, 504}

    def __init__(self, model=None, client=None, retries=2, backoff_s=2.0):
        import httpx
        self.httpx, self.retries, self.backoff_s = httpx, retries, backoff_s
        self.key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not self.key and client is None:
            raise RuntimeError("set GEMINI_API_KEY to use the Gemini provider")
        self.model = model or os.environ.get("OPSPILOT_MODEL", "gemini-3.8-flash")
        self.client = client or httpx.Client(timeout=float(os.environ.get("OPSPILOT_LLM_TIMEOUT", "180")))
        self.thinking = os.environ.get("OPSPILOT_THINKING")      # low | medium | high (Gemini 3 thinking level)

    def complete(self, system, user, max_tokens=1024):
        body = {"systemInstruction": {"parts": [{"text": system}]},
                "contents": [{"role": "user", "parts": [{"text": user}]}],
                # headroom: thinking models spend output tokens on reasoning before the JSON answer
                "generationConfig": {"maxOutputTokens": max(max_tokens, 4096), "temperature": 0.1,
                                     "responseMimeType": "application/json"}}
        if self.thinking:
            body["generationConfig"]["thinkingConfig"] = {"thinkingLevel": self.thinking}
        r = self._post(body)
        if r.status_code != 200:
            raise RuntimeError(f"Gemini HTTP {r.status_code}: {r.text[:300]}")   # the key is in a header, never the URL
        data = r.json()
        cands = data.get("candidates") or []
        parts = (cands[0].get("content") or {}).get("parts", []) if cands else []
        text = "".join(p.get("text", "") for p in parts)
        u = data.get("usageMetadata", {})
        return LLMResult(text, u.get("promptTokenCount", 0),
                         u.get("candidatesTokenCount", 0) + u.get("thoughtsTokenCount", 0), self.model)


    def _post(self, body):
        """Retries timeouts, rate limits and 5xx with exponential backoff; other errors surface immediately."""
        for attempt in range(self.retries + 1):
            last = attempt == self.retries
            try:
                r = self.client.post(self.URL.format(model=self.model), json=body,
                                     headers={"x-goog-api-key": self.key or ""})
            except self.httpx.TimeoutException:
                if last:
                    raise
            else:
                if r.status_code not in self.RETRYABLE or last:
                    return r
            time.sleep(self.backoff_s * 2 ** attempt)


def _section(prompt, name):
    m = re.search(r"^## %s\n(.*?)(?=^## |\Z)" % name, prompt, re.S | re.M)
    return m.group(1) if m else ""


class HeuristicLLM:
    """Naive keyword rules. Deliberately simple: it misses the cases that need judgement."""
    model = "heuristic-baseline"

    def complete(self, system, user, max_tokens=1024):
        ev = "\n".join(_section(user, s) for s in ("LOGS", "EVENTS", "DESCRIBE"))
        ns = re.search(r"^namespace: (\S+)", user, re.M)
        wl = re.search(r"^workload: (\S+)", user, re.M)
        out = self._decide(ev, bool(_section(user, "FEEDBACK").strip()), wl.group(1) if wl else "")
        text = json.dumps(out)
        return LLMResult(text, len(user) // 4, len(text) // 4, self.model)

    def _decide(self, ev, has_feedback, wl):
        def mk(cat, action, params, why, conf=0.8):
            return {"category": cat, "action": action, "params": params, "explanation": why, "confidence": conf}
        if has_feedback:
            return mk("unknown", "escalate", {"reason": "guardrails rejected the first proposal"},
                      "Falling back to a human after guardrail feedback.", 0.6)
        if "ErrImagePull" in ev or "ImagePullBackOff" in ev:
            return mk("bad_image", "rollback", {}, "Image cannot be pulled; roll back to previous revision.")
        m = re.search(r"KeyError: '(\w+)'|(\w+) (?:is|was) not set|missing required environment variable (\w+)", ev)
        if m:
            key = next(g for g in m.groups() if g)
            return mk("missing_config", "set_env", {"key": key}, f"Missing environment variable {key}.")
        if "memory" in ev.lower() and ("OOMKilled" in ev or "Evicted" in ev or "low on memory" in ev):
            lim = re.search(r"memory:\s*(\d+)Mi", ev)
            new = f"{int(lim.group(1)) * 4}Mi" if lim else "512Mi"
            return mk("oom", "set_memory_limit", {"memory": new}, "Container is memory constrained.")
        if re.search(r"Insufficient cpu", ev):
            return mk("insufficient_resources", "set_cpu_request", {"cpu": "500m"}, "CPU request cannot be scheduled.")
        if re.search(r"^Replicas:\s*0 desired", ev, re.M):
            return mk("scaled_to_zero", "scale", {"replicas": 3}, "Workload has no replicas.")
        if "CrashLoopBackOff" in ev or "connection refused" in ev.lower():
            return mk("bad_release", "restart", {}, "Crash loop; restarting pods.", 0.55)
        return mk("unknown", "escalate", {"reason": "no matching rule"}, "No rule matched.", 0.3)


def get_llm():
    kind = os.environ.get("OPSPILOT_LLM")
    if not kind:
        kind = ("gemini" if os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
                else "anthropic" if os.environ.get("ANTHROPIC_API_KEY") else "heuristic")
    return {"gemini": GeminiLLM, "anthropic": AnthropicLLM, "heuristic": HeuristicLLM}[kind]()

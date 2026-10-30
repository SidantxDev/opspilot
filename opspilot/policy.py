"""Deterministic guardrails. The LLM proposes; this decides whether the proposal may even reach a human.

Evidence (logs, events) is untrusted input and can contain prompt injection, so the real defence is not the
prompt: it is that the model can only choose from this closed allowlist, on this one workload, within these bounds.
"""
import os
import re

ACTIONS = {"set_memory_limit", "set_cpu_request", "set_image", "rollback", "scale", "set_env",
           "set_probe_port", "restart", "escalate"}
CATEGORIES = {"oom", "bad_image", "missing_config", "bad_release", "scaled_to_zero", "probe_misconfig",
              "insufficient_resources", "dependency_down", "node_pressure", "unknown"}
PROTECTED_NS = {"kube-system", "kube-public", "kube-node-lease"} | \
    {n for n in os.environ.get("OPSPILOT_PROTECTED_NS", "").split(",") if n}
MAX_MEMORY_MI = 4096
MAX_REPLICAS = 20
MIN_CONFIDENCE = 0.5
NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")


def parse_mi(s):
    m = re.fullmatch(r"(\d+)(Mi|Gi|M|G)?", str(s).strip())
    if not m:
        return None
    n, u = int(m.group(1)), m.group(2) or "Mi"
    return n * 1024 if u in ("Gi", "G") else n


def parse_cpu_m(s):
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(m)?", str(s).strip())
    if not m:
        return None
    return int(float(m.group(1)) * (1 if m.group(2) else 1000))


def validate(proposal, incident, describe_text=""):
    """Returns a list of human-readable violations; empty means the proposal may go to a human."""
    v = []
    if not isinstance(proposal, dict):
        return ["proposal is not a JSON object"]
    action, params = proposal.get("action"), proposal.get("params") or {}
    if proposal.get("category") not in CATEGORIES:
        v.append(f"category must be one of {sorted(CATEGORIES)}")
    if action not in ACTIONS:
        return v + [f"action '{action}' is not allowed; choose from {sorted(ACTIONS)}"]
    if not isinstance(params, dict):
        return v + ["params must be an object"]
    try:
        conf = float(proposal.get("confidence", 0))
    except (TypeError, ValueError):
        conf = 0.0
    if action == "escalate":
        return v
    if incident["namespace"] in PROTECTED_NS:
        v.append(f"namespace '{incident['namespace']}' is protected; changes there need a human, use action escalate")
    if conf < MIN_CONFIDENCE:
        v.append(f"confidence {conf} is below {MIN_CONFIDENCE}; escalate instead of guessing")
    if params.get("deployment") not in (None, incident["workload"]):
        v.append("proposal may only touch the alerting workload")
    if action == "set_memory_limit":
        mi = parse_mi(params.get("memory", ""))
        if mi is None or not 64 <= mi <= MAX_MEMORY_MI:
            v.append(f"memory must be between 64Mi and {MAX_MEMORY_MI}Mi, e.g. '512Mi'")
    elif action == "set_cpu_request":
        m = parse_cpu_m(params.get("cpu", ""))
        if m is None or not 10 <= m <= 8000:
            v.append("cpu must be between 10m and 8 cores, e.g. '500m'")
    elif action == "scale":
        r = params.get("replicas")
        if not isinstance(r, int) or not 1 <= r <= MAX_REPLICAS:
            v.append(f"replicas must be an integer between 1 and {MAX_REPLICAS}")
    elif action == "set_env":
        if not re.fullmatch(r"[A-Z_][A-Z0-9_]*", str(params.get("key", ""))):
            v.append("env key must look like DATABASE_URL")
    elif action == "set_probe_port":
        p = params.get("port")
        if not isinstance(p, int) or not 1 <= p <= 65535:
            v.append("port must be an integer between 1 and 65535")
    elif action == "set_image":
        img = str(params.get("image", ""))
        cur = re.search(r"Image:\s*(\S+)", describe_text)
        if ":" not in img:
            v.append("image must include a tag")
        elif cur and img.rsplit(":", 1)[0] != cur.group(1).rsplit(":", 1)[0]:
            v.append("image repository must stay the same; only the tag may change")
    return v

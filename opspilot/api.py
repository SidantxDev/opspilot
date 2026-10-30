"""HTTP API: submit incidents, watch them, and approve or reject the proposed fix. Also serves the console UI."""
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, Field

from .policy import NAME_RE
from .store import Store

STATIC = Path(__file__).resolve().parent / "static"
STATUSES = "^(queued|running|waiting_approval|succeeded|failed)$"


class IncidentIn(BaseModel):
    namespace: str = Field(pattern=NAME_RE.pattern, max_length=63)
    workload: str = Field(pattern=NAME_RE.pattern, max_length=63)
    alert: str = Field(default="", max_length=500)
    severity: str = Field(default="medium", pattern="^(low|medium|high|critical)$")


class ApprovalIn(BaseModel):
    decision: str = Field(pattern="^(approve|reject)$")
    by: str = Field(min_length=1, max_length=100)
    note: Optional[str] = Field(default=None, max_length=500)


def prometheus(m):
    """Render store.metrics() in the Prometheus text exposition format."""
    out = []

    def metric(name, kind, help_, samples):
        out.extend([f"# HELP {name} {help_}", f"# TYPE {name} {kind}"])
        for labels, value in samples:
            lbl = ",".join(f'{k}="{str(v).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
                           for k, v in labels.items())
            out.append(f"{name}{{{lbl}}} {value or 0}" if lbl else f"{name} {value or 0}")

    metric("opspilot_jobs", "gauge", "Incidents by current status.",
           [({"status": r["status"]}, r["n"]) for r in m["jobs"]])
    metric("opspilot_outcomes_total", "counter", "Finished incidents by outcome.",
           [({"outcome": r["outcome"]}, r["n"]) for r in m["outcomes"]])
    metric("opspilot_events_total", "counter", "Runtime events (resumed, lease_lost, error, budget_exceeded, ...).",
           [({"kind": r["kind"]}, r["n"]) for r in m["events"]])
    metric("opspilot_node_duration_seconds_sum", "counter", "Total time spent in each graph node.",
           [({"node": r["node"]}, round((r["ms"] or 0) / 1000, 4)) for r in m["nodes"]])
    metric("opspilot_node_duration_seconds_count", "counter", "Completed executions of each graph node.",
           [({"node": r["node"]}, r["n"]) for r in m["nodes"]])
    metric("opspilot_llm_calls_total", "counter", "Model calls.",
           [({"tenant": r["tenant"], "model": r["model"]}, r["calls"]) for r in m["usage"]])
    metric("opspilot_llm_tokens_total", "counter", "Model tokens.",
           [({"tenant": r["tenant"], "model": r["model"], "direction": d}, r[k])
            for r in m["usage"] for d, k in (("in", "tin"), ("out", "tout"))])
    metric("opspilot_llm_cost_usd_total", "counter", "Estimated model spend in USD.",
           [({"tenant": r["tenant"], "model": r["model"]}, round(r["cost"] or 0, 6)) for r in m["usage"]])
    metric("opspilot_approval_wait_seconds_sum", "counter", "Time incidents spent waiting for a human.",
           [({"decision": r["decision"]}, round(r["wait"] or 0, 3)) for r in m["approvals"]])
    metric("opspilot_approval_wait_seconds_count", "counter", "Human decisions.",
           [({"decision": r["decision"]}, r["n"]) for r in m["approvals"]])
    return "\n".join(out) + "\n"


def create_app(store: Store = None, scenarios=None):
    store = store or Store(os.environ.get("OPSPILOT_DB", "opspilot.db"))
    if scenarios is None and os.environ.get("OPSPILOT_CLUSTER", "").startswith("demo"):
        from .bootstrap import load_scenarios
        scenarios = load_scenarios()
    app = FastAPI(title="OpsPilot", version="0.2.0")

    def view(job):
        cp = store.latest_checkpoint(job["id"])
        st = cp[1] if cp else {}
        return {"id": job["id"], "tenant": job["tenant"], "status": job["status"], "attempts": job["attempts"],
                "error": job["error"], "created": job["created"], "updated": job["updated"],
                "last_node": cp[0] if cp else None, "incident": job["payload"],
                "evidence": st.get("evidence"), "proposal": st.get("proposal"), "guardrail": st.get("guardrail"),
                "revisions": st.get("revisions", 0), "approval": st.get("approval") or store.get_approval(job["id"]),
                "runbooks": [f"{r['source']} / {r['heading']}" for r in st.get("runbooks", [])],
                "verified": st.get("verified"), "outcome": st.get("outcome"), "report": st.get("report"),
                "usage": store.job_usage(job["id"])}

    @app.get("/", include_in_schema=False)
    def console():
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "jobs": store.counts()}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        return prometheus(store.metrics())

    @app.get("/demo/scenarios")
    def demo_scenarios():
        """Recorded incidents the demo cluster can replay (empty when pointed at a real cluster)."""
        return [{"id": s["id"], **s["incident"]} for s in scenarios or []]

    @app.post("/incidents", status_code=202)
    def submit(body: IncidentIn, x_tenant: str = Header(default="default"),
               idempotency_key: Optional[str] = Header(default=None)):
        jid, created = store.enqueue(x_tenant, body.model_dump(), idem_key=idempotency_key)
        return {"id": jid, "created": created}

    @app.get("/incidents")
    def list_incidents(status: Optional[str] = Query(default=None, pattern=STATUSES),
                       limit: int = Query(default=50, ge=1, le=200)):
        out = []
        for job in store.list_jobs(limit, status):
            cp = store.latest_checkpoint(job["id"])
            st = cp[1] if cp else {}
            prop = st.get("proposal") or {}
            out.append({"id": job["id"], "tenant": job["tenant"], "status": job["status"],
                        "incident": job["payload"], "updated": job["updated"], "last_node": cp[0] if cp else None,
                        "category": prop.get("category"), "action": prop.get("action"), "outcome": st.get("outcome")})
        return out

    @app.get("/incidents/{jid}")
    def get(jid: int):
        job = store.get_job(jid)
        if not job:
            raise HTTPException(404, "no such incident")
        return view(job)

    @app.get("/incidents/{jid}/trace")
    def trace(jid: int):
        if not store.get_job(jid):
            raise HTTPException(404, "no such incident")
        return store.events(jid)

    @app.post("/incidents/{jid}/approval")
    def approve(jid: int, body: ApprovalIn):
        if not store.get_job(jid):
            raise HTTPException(404, "no such incident")
        if not store.add_approval(jid, body.decision, body.by, body.note):
            raise HTTPException(409, "incident is not waiting for approval (or was already decided)")
        return {"ok": True}

    return app

"""HTTP API: submit incidents, watch them, and approve or reject the proposed fix."""
import os
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .policy import NAME_RE
from .store import Store


class IncidentIn(BaseModel):
    namespace: str = Field(pattern=NAME_RE.pattern, max_length=63)
    workload: str = Field(pattern=NAME_RE.pattern, max_length=63)
    alert: str = Field(default="", max_length=500)
    severity: str = Field(default="medium", pattern="^(low|medium|high|critical)$")


class ApprovalIn(BaseModel):
    decision: str = Field(pattern="^(approve|reject)$")
    by: str = Field(min_length=1, max_length=100)
    note: Optional[str] = Field(default=None, max_length=500)


def create_app(store: Store = None):
    store = store or Store(os.environ.get("OPSPILOT_DB", "opspilot.db"))
    app = FastAPI(title="OpsPilot", version="0.1.0")

    def view(job):
        cp = store.latest_checkpoint(job["id"])
        st = cp[1] if cp else {}
        return {"id": job["id"], "status": job["status"], "attempts": job["attempts"], "error": job["error"],
                "last_node": cp[0] if cp else None, "incident": job["payload"],
                "proposal": st.get("proposal"), "guardrail": st.get("guardrail"),
                "runbooks": [f"{r['source']} / {r['heading']}" for r in st.get("runbooks", [])],
                "outcome": st.get("outcome"), "report": st.get("report"),
                "usage": store.job_usage(job["id"])}

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "jobs": store.counts()}

    @app.post("/incidents", status_code=202)
    def submit(body: IncidentIn, x_tenant: str = Header(default="default"),
               idempotency_key: Optional[str] = Header(default=None)):
        jid, created = store.enqueue(x_tenant, body.model_dump(), idem_key=idempotency_key)
        return {"id": jid, "created": created}

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

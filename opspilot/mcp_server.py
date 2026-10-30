"""MCP server: lets any MCP client (Claude Desktop, Cursor, another agent) use OpsPilot's tools.

  python -m opspilot mcp            # stdio transport

Exposed: redacted read-only diagnostics, runbook search, the guardrail check, and filing/reading incidents.
Deliberately NOT exposed: approving a fix. An agent can propose and file, but only a human with the API or the
console can let a change reach the cluster, so wiring OpsPilot into another agent cannot bypass the approval gate.
"""
import os

from . import agents
from .bootstrap import ROOT, make_cluster
from .policy import NAME_RE, validate
from .rag import RunbookIndex
from .store import Store

TENANT = os.environ.get("OPSPILOT_MCP_TENANT", "mcp")


class Tools:
    def __init__(self, store, cluster, rag):
        self.store, self.cluster, self.rag = store, cluster, rag

    @staticmethod
    def _target(namespace, workload):
        for v in (namespace, workload):
            if not (isinstance(v, str) and len(v) <= 63 and NAME_RE.fullmatch(v)):
                raise ValueError(f"invalid Kubernetes name: {v!r}")

    def get_workload_evidence(self, namespace: str, workload: str) -> dict:
        """Recent logs, events and `kubectl describe` for a deployment, with secrets redacted. Read-only."""
        self._target(namespace, workload)
        ev = {k: agents.redact(getattr(self.cluster, k)(namespace, workload)) for k in ("logs", "events", "describe")}
        return {"logs": agents.tail(ev["logs"]), "events": agents.tail(ev["events"], 40, 3000),
                "describe": ev["describe"][:6000]}

    def search_runbooks(self, query: str, k: int = 3) -> list:
        """BM25 search over the team's incident runbooks. Returns the best-matching sections."""
        return self.rag.search(query, k=max(1, min(int(k), 10)))

    def check_proposal(self, namespace: str, workload: str, category: str, action: str,
                       params: dict, confidence: float) -> dict:
        """Run OpsPilot's deterministic guardrails on a proposed fix without applying anything."""
        self._target(namespace, workload)
        describe = self.cluster.describe(namespace, workload)
        prop = {"category": category, "action": action, "params": params, "confidence": confidence}
        violations = validate(prop, {"namespace": namespace, "workload": workload}, describe)
        return {"allowed": not violations, "violations": violations}

    def open_incident(self, namespace: str, workload: str, alert: str = "", severity: str = "medium",
                      idempotency_key: str = "") -> dict:
        """File an incident. OpsPilot investigates and proposes a fix; a human must approve it before it applies."""
        self._target(namespace, workload)
        if severity not in ("low", "medium", "high", "critical"):
            raise ValueError("severity must be low, medium, high or critical")
        jid, created = self.store.enqueue(TENANT, {"namespace": namespace, "workload": workload,
                                                   "alert": str(alert)[:500], "severity": severity},
                                          idem_key=idempotency_key or None)
        return {"id": jid, "created": created, "note": "a human must approve the proposed fix in the OpsPilot console"}

    def get_incident(self, incident_id: int) -> dict:
        """Status, proposed fix, guardrail result and outcome of an incident."""
        job = self.store.get_job(int(incident_id))
        if not job:
            raise ValueError(f"no incident {incident_id}")
        cp = self.store.latest_checkpoint(job["id"])
        st = cp[1] if cp else {}
        return {"id": job["id"], "status": job["status"], "incident": job["payload"], "proposal": st.get("proposal"),
                "guardrail": st.get("guardrail"), "outcome": st.get("outcome"), "report": st.get("report")}


def build_server(tools=None):
    try:                                        # mcp >= 2
        from mcp.server.mcpserver import MCPServer as Server
    except ImportError:                         # mcp 1.x
        from mcp.server.fastmcp import FastMCP as Server
    tools = tools or Tools(Store(os.environ.get("OPSPILOT_DB", "opspilot.db")), make_cluster(),
                           RunbookIndex.from_dir(ROOT / "runbooks"))
    server = Server("opspilot", instructions=__doc__)
    for name in ("get_workload_evidence", "search_runbooks", "check_proposal", "open_incident", "get_incident"):
        server.tool()(getattr(tools, name))
    return server


def main():
    build_server().run("stdio")

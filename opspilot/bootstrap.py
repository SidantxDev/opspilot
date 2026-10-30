"""Wiring: builds the pieces from env vars so the API process and worker process share one configuration."""
import os
from pathlib import Path

import yaml

from .agents import build_graph
from .cluster import FakeCluster, KubectlCluster
from .llm import get_llm
from .rag import RunbookIndex
from .runtime import RunContext
from .store import Store
from .worker import Worker

ROOT = Path(__file__).resolve().parent.parent


def load_scenarios(path=None):
    return yaml.safe_load(Path(path or ROOT / "evals" / "scenarios.yaml").read_text(encoding="utf-8"))


def make_cluster():
    """OPSPILOT_CLUSTER=kubectl (default) or demo:<scenario-id> to replay a recorded incident."""
    spec = os.environ.get("OPSPILOT_CLUSTER", "kubectl")
    if spec.startswith("demo:"):
        sid = spec.split(":", 1)[1]
        return FakeCluster(next(s for s in load_scenarios() if s["id"] == sid))
    return KubectlCluster(context=os.environ.get("OPSPILOT_KUBE_CONTEXT"))


def make_ctx_factory(store, llm, cluster, rag, **overrides):
    def make(job):
        return RunContext(store=store, job_id=job["id"], tenant=job["tenant"], llm=llm, cluster=cluster, rag=rag,
                          initial_state={"incident": job["payload"], "revisions": 0}, **overrides)
    return make


def make_worker(store=None, llm=None, cluster=None, rag=None, **kw):
    store = store or Store(os.environ.get("OPSPILOT_DB", "opspilot.db"))
    rag = rag or RunbookIndex.from_dir(ROOT / "runbooks")
    make = make_ctx_factory(store, llm or get_llm(), cluster or make_cluster(), rag)
    return Worker(store, build_graph(), make, **kw)

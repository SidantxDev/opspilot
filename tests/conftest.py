import os
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opspilot.agents import build_graph  # noqa: E402
from opspilot.bootstrap import ROOT, load_scenarios, make_ctx_factory  # noqa: E402
from opspilot.cluster import FakeCluster  # noqa: E402
from opspilot.llm import HeuristicLLM  # noqa: E402
from opspilot.rag import RunbookIndex  # noqa: E402
from opspilot.store import PgStore, Store  # noqa: E402

# Set OPSPILOT_TEST_PG=postgresql://... to run every store-backed test against Postgres as well as SQLite.
PG = os.environ.get("OPSPILOT_TEST_PG")
BACKENDS = ["sqlite"] + (["postgres"] if PG else [])
from opspilot.worker import Worker  # noqa: E402


@pytest.fixture(params=BACKENDS)
def store(request, tmp_path):
    if request.param == "sqlite":
        yield Store(tmp_path / "t.db")
        return
    s = PgStore(PG, schema=f"t_{uuid.uuid4().hex[:12]}")
    yield s
    s.drop_schema()
    s.close()


@pytest.fixture(scope="session")
def rag():
    return RunbookIndex.from_dir(ROOT / "runbooks")


@pytest.fixture(scope="session")
def scenarios():
    return {s["id"]: s for s in load_scenarios()}


@pytest.fixture
def make_worker(store, rag):
    def _make(scenario, llm=None, cluster=None, **kw):
        cluster = cluster or FakeCluster(scenario)
        kw.setdefault("lease_s", 30)
        kw.setdefault("backoff_s", 0)
        w = Worker(store, build_graph(), make_ctx_factory(store, llm or HeuristicLLM(), cluster, rag), **kw)
        return w, cluster
    return _make


def wait_status(store, jid, want, tries=20, **run):
    for _ in range(tries):
        if store.get_job(jid)["status"] in want:
            return True
        time.sleep(0.02)
    return False


def with_fault(worker, hook):
    """Install a fault-injection hook on every context the worker creates."""
    orig = worker.make_ctx

    def make(job):
        ctx = orig(job)
        ctx.fault_hook = hook
        return ctx
    worker.make_ctx = make
    return worker

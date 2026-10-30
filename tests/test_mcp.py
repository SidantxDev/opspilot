import asyncio

import pytest

from opspilot.bootstrap import load_scenarios
from opspilot.cluster import DemoCluster
from opspilot.mcp_server import Tools


@pytest.fixture
def tools(store, rag):
    return Tools(store, DemoCluster(load_scenarios()), rag)


def test_evidence_is_redacted_and_names_are_validated(tools):
    ev = tools.get_workload_evidence("shop", "billing")
    assert "sk_live_51Hxxx" not in str(ev) and "[REDACTED]" in str(ev)
    with pytest.raises(ValueError):
        tools.get_workload_evidence("shop", "api; kubectl delete ns shop")


def test_guardrail_check_blocks_protected_namespace(tools):
    r = tools.check_proposal("kube-system", "coredns", "oom", "set_memory_limit", {"memory": "512Mi"}, 0.9)
    assert not r["allowed"] and any("protected" in v for v in r["violations"])
    assert tools.check_proposal("shop", "api", "oom", "set_memory_limit", {"memory": "512Mi"}, 0.9)["allowed"]


def test_open_incident_queues_but_nothing_can_approve_it(tools, store):
    r = tools.open_incident("shop", "api", "pods restarting", idempotency_key="a1")
    assert tools.open_incident("shop", "api", idempotency_key="a1")["id"] == r["id"]
    assert tools.get_incident(r["id"])["status"] == "queued"
    assert not any("approv" in n for n in dir(tools) if not n.startswith("_"))


def test_server_registers_exactly_the_safe_tools(tools):
    pytest.importorskip("mcp")
    from opspilot.mcp_server import build_server
    names = {t.name for t in asyncio.run(build_server(tools).list_tools())}
    assert names == {"get_workload_evidence", "search_runbooks", "check_proposal", "open_incident", "get_incident"}

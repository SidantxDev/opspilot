from fastapi.testclient import TestClient

from opspilot.api import create_app
from opspilot.bootstrap import load_scenarios
from opspilot.cluster import DemoCluster


def test_demo_cluster_routes_each_target_to_its_scenario(scenarios):
    demo = DemoCluster(load_scenarios())
    assert "OOMKilled" in demo.events("shop", "api") + demo.describe("shop", "api")
    demo.apply("shop", "checkout", "rollback", {})
    assert demo.is_healthy("shop", "checkout")
    assert not demo.is_healthy("shop", "api")          # fixing one workload does not heal another


def test_console_list_and_metrics_reflect_the_run(store, make_worker, scenarios):
    sc = scenarios["oom-api"]
    client = TestClient(create_app(store, scenarios=load_scenarios()))
    assert "OpsPilot" in client.get("/").text
    assert {"id": "oom-api", **sc["incident"]} in client.get("/demo/scenarios").json()

    jid = client.post("/incidents", json=sc["incident"], headers={"X-Tenant": "acme"}).json()["id"]
    w, _ = make_worker(sc)
    w.run_once()
    rows = client.get("/incidents?status=waiting_approval").json()
    assert [r["id"] for r in rows] == [jid] and rows[0]["category"] == "oom"
    detail = client.get(f"/incidents/{jid}").json()
    assert detail["evidence"]["logs"] and detail["approval"] is None

    client.post(f"/incidents/{jid}/approval", json={"decision": "approve", "by": "alice"})
    w.run_once()
    assert client.get(f"/incidents/{jid}").json()["approval"]["by"] == "alice"

    m = client.get("/metrics").text
    assert 'opspilot_jobs{status="succeeded"} 1' in m
    assert 'opspilot_outcomes_total{outcome="resolved"} 1' in m
    assert 'opspilot_llm_calls_total{tenant="acme",model="heuristic-baseline"} 1' in m
    assert 'opspilot_approval_wait_seconds_count{decision="approve"} 1' in m
    assert 'opspilot_node_duration_seconds_count{node="verify"} 1' in m


def test_list_rejects_unknown_status(store):
    assert TestClient(create_app(store, scenarios=[])).get("/incidents?status=bogus").status_code == 422

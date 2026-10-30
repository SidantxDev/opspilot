from fastapi.testclient import TestClient

from opspilot.api import create_app


def test_full_flow_over_http(store, make_worker, scenarios):
    client = TestClient(create_app(store))
    w, cluster = make_worker(scenarios["bad-image-tag"])
    body = scenarios["bad-image-tag"]["incident"]

    r = client.post("/incidents", json=body, headers={"X-Tenant": "acme", "Idempotency-Key": "alert-1"})
    assert r.status_code == 202
    jid = r.json()["id"]
    again = client.post("/incidents", json=body, headers={"X-Tenant": "acme", "Idempotency-Key": "alert-1"})
    assert again.json() == {"id": jid, "created": False}

    w.run_once()
    pending = client.get(f"/incidents/{jid}").json()
    assert pending["status"] == "waiting_approval"
    assert pending["proposal"]["action"] == "rollback"
    assert any("image-pull" in r for r in pending["runbooks"])

    assert client.post(f"/incidents/{jid}/approval", json={"decision": "approve", "by": "asha"}).status_code == 200
    assert client.post(f"/incidents/{jid}/approval", json={"decision": "approve", "by": "asha"}).status_code == 409
    w.run_once()
    done = client.get(f"/incidents/{jid}").json()
    assert done["status"] == "succeeded" and done["outcome"] == "resolved"
    assert done["usage"]["calls"] >= 1
    kinds = [e["kind"] for e in client.get(f"/incidents/{jid}/trace").json()]
    assert "interrupt" in kinds and "llm_call" in kinds


def test_input_validation_blocks_argument_injection(store):
    client = TestClient(create_app(store))
    bad = {"namespace": "shop", "workload": "api; rm -rf /"}
    assert client.post("/incidents", json=bad).status_code == 422
    assert client.post("/incidents", json={"namespace": "--all-namespaces", "workload": "api"}).status_code == 422
    assert client.get("/incidents/999").status_code == 404

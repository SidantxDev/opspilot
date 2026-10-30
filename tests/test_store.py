import time


def test_enqueue_is_idempotent_per_tenant(store):
    a, created_a = store.enqueue("t1", {"x": 1}, idem_key="alert-42")
    b, created_b = store.enqueue("t1", {"x": 1}, idem_key="alert-42")
    c, created_c = store.enqueue("t2", {"x": 1}, idem_key="alert-42")
    assert (created_a, created_b, created_c) == (True, False, True)
    assert a == b and a != c


def test_only_one_worker_claims_a_job(store):
    store.enqueue("t", {})
    assert store.claim("w1") is not None
    assert store.claim("w2") is None


def test_expired_lease_is_reclaimed_and_old_owner_is_fenced(store):
    jid, _ = store.enqueue("t", {})
    assert store.claim("w1", lease_s=0.05)["id"] == jid
    time.sleep(0.1)
    again = store.claim("w2", lease_s=30)
    assert again["id"] == jid and again["attempts"] == 2
    assert store.heartbeat(jid, "w1") is False          # the dead worker can no longer extend or finish
    store.finish(jid, "w1", "succeeded")
    assert store.get_job(jid)["status"] == "running"
    assert store.heartbeat(jid, "w2") is True


def test_poison_job_stops_after_max_attempts(store):
    jid, _ = store.enqueue("t", {}, max_attempts=2)
    for w in ("a", "b"):
        assert store.claim(w, lease_s=0.01)
        time.sleep(0.03)
    assert store.claim("c") is None
    assert store.get_job(jid)["status"] == "failed"


def test_retry_respects_backoff(store):
    jid, _ = store.enqueue("t", {})
    store.claim("w")
    store.retry(jid, "w", "boom", delay=0.2)
    assert store.claim("w") is None
    time.sleep(0.25)
    assert store.claim("w")["id"] == jid


def test_approval_only_valid_while_waiting_and_only_once(store):
    jid, _ = store.enqueue("t", {})
    assert store.add_approval(jid, "approve", "me") is False       # not waiting yet
    store.claim("w")
    store.pause(jid, "w")
    assert store.add_approval(jid, "approve", "me") is True
    assert store.add_approval(jid, "reject", "other") is False     # already decided
    assert store.get_approval(jid)["decision"] == "approve"
    assert store.get_job(jid)["status"] == "queued"

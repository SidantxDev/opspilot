"""Fencing tokens and the action ledger: the guarantees around the one step that changes the cluster."""
import sqlite3
import threading
import time

import pytest

from conftest import with_fault
from opspilot.cluster import FakeCluster
from opspilot.graph import WorkerCrash
from opspilot.store import Store
from test_agent_flow import crash_at, drive, final


def test_fence_increments_on_every_claim_even_across_approval_pauses(store):
    jid, _ = store.enqueue("t", {})
    f1 = store.claim("w1")["fence"]
    store.pause(jid, "w1")                       # pause gives the attempt back, the fence must still move on
    store.add_approval(jid, "approve", "me")
    f2 = store.claim("w2")["fence"]
    assert f2 > f1


def test_stale_fence_cannot_record_a_change(store):
    jid, _ = store.enqueue("t", {})
    old = store.claim("w1", lease_s=0.05)
    time.sleep(0.1)
    new = store.claim("w2", lease_s=30)
    assert store.begin_action(jid, "w1", old["fence"], "ns", "wl", "scale", {"replicas": 2}) is None
    row = store.begin_action(jid, "w2", new["fence"], "ns", "wl", "scale", {"replicas": 2})
    assert row["status"] == "pending"
    assert store.complete_action(jid, "w1", old["fence"], "zombie") is False
    assert store.complete_action(jid, "w2", new["fence"], "ok") is True
    assert store.get_action(jid)["status"] == "done"


def test_expired_lease_is_refused_even_before_anyone_reclaims(store):
    jid, _ = store.enqueue("t", {})
    j = store.claim("w1", lease_s=0.05)
    time.sleep(0.1)                              # nobody took over yet, but the lease is gone
    assert store.begin_action(jid, "w1", j["fence"], "ns", "wl", "restart", {}) is None


def test_zombie_worker_is_fenced_before_touching_the_cluster(store, make_worker, scenarios):
    """w1 stalls (GC pause / partition) right before apply; its lease expires and w2 takes the job.
    When w1 wakes up it must not change the cluster."""
    sc = scenarios["oom-api"]
    cluster = FakeCluster(sc)
    w1, _ = make_worker(sc, cluster=cluster, lease_s=0.3)
    jid, _ = store.enqueue("t", sc["incident"])
    w1.run_once()
    store.add_approval(jid, "approve", "me")

    def stall(point, node):
        if (point, node) == ("before", "apply"):
            w1_beats.set()                       # stop heart-beating, like a frozen process
            time.sleep(0.5)
            j2 = store.claim("w2", lease_s=30)  # w2 takes the expired lease meanwhile
            assert j2 and j2["id"] == jid
    w1_beats = threading.Event()
    real_hb = store.heartbeat
    store.heartbeat = lambda j, w, lease_s=30.0: False if (w == w1.id and w1_beats.is_set()) else real_hb(j, w, lease_s)
    with_fault(w1, stall)                        # the stall happens after the in-process lease check passed
    w1.run_once()
    assert cluster.applied == []
    kinds = [e["kind"] for e in store.events(jid)]
    assert "fenced" in kinds
    assert store.get_action(jid) is None


def test_crash_in_effect_window_reissues_the_same_concrete_change(store, make_worker, scenarios):
    sc = scenarios["oom-api"]
    w1, cluster = make_worker(sc, lease_s=0.2)
    with_fault(w1, crash_at("effect", "apply"))  # change made, ledger not yet committed
    jid, _ = store.enqueue("t", sc["incident"])
    w1.run_once()
    store.add_approval(jid, "approve", "me")
    with pytest.raises(WorkerCrash):
        w1.run_once()
    assert store.get_action(jid)["status"] == "pending"
    time.sleep(0.3)
    w2, _ = make_worker(sc, cluster=cluster)
    assert drive(store, w2, jid) == "succeeded"
    row = store.get_action(jid)
    assert row["status"] == "done" and row["issues"] == 2
    assert [a["params"] for a in cluster.applied] == [row["params"]] * 2
    assert "apply_reissued" in [e["kind"] for e in store.events(jid)]


def test_crash_after_commit_skips_the_cluster_entirely(store, make_worker, scenarios):
    sc = scenarios["oom-api"]
    w1, cluster = make_worker(sc, lease_s=0.2)
    jid, _ = store.enqueue("t", sc["incident"])
    w1.run_once()
    store.add_approval(jid, "approve", "me")
    real = store.save_checkpoint

    def die_on_apply_checkpoint(job_id, node, state, *fence):
        if node == "apply":
            raise WorkerCrash()                  # ledger says done, graph checkpoint never written
        return real(job_id, node, state, *fence)
    store.save_checkpoint = die_on_apply_checkpoint
    with pytest.raises(WorkerCrash):
        w1.run_once()
    store.save_checkpoint = real
    time.sleep(0.3)
    w2, _ = make_worker(sc, cluster=cluster)
    assert drive(store, w2, jid) == "succeeded"
    assert len(cluster.applied) == 1             # exactly once
    assert "apply_skipped" in [e["kind"] for e in store.events(jid)]
    assert final(store, jid)["outcome"] == "resolved"


def test_prepare_runs_once_so_a_resumed_rollback_targets_the_same_revision(store, make_worker, scenarios):
    sc = scenarios["bad-image-tag"]
    cluster = FakeCluster(sc)
    calls = []

    def prepare(ns, wl, action, params):
        calls.append(action)
        return {"to_revision": 7 + len(calls), "template": {"image": "x"}}   # would drift if called again
    cluster.prepare = prepare
    w1, _ = make_worker(sc, cluster=cluster, lease_s=0.2)
    with_fault(w1, crash_at("effect", "apply"))
    jid, _ = store.enqueue("t", sc["incident"])
    w1.run_once()
    store.add_approval(jid, "approve", "me")
    with pytest.raises(WorkerCrash):
        w1.run_once()
    time.sleep(0.3)
    w2, _ = make_worker(sc, cluster=cluster)
    drive(store, w2, jid)
    assert calls == ["rollback"]
    assert {a["params"]["to_revision"] for a in cluster.applied} == {8}


def test_demo_cluster_health_is_shared_through_the_ledger(store, scenarios):
    sc = scenarios["oom-api"]
    a, b = FakeCluster(sc, store), FakeCluster(sc, store)    # two worker processes
    jid, _ = store.enqueue("t", sc["incident"])
    j = store.claim("w")
    store.begin_action(jid, "w", j["fence"], "shop", "api", "set_memory_limit", {"memory": "512Mi"})
    a.apply("shop", "api", "set_memory_limit", {"memory": "512Mi"})
    assert not b.is_healthy("shop", "api")                    # not committed yet
    store.complete_action(jid, "w", j["fence"], "ok")
    assert b.is_healthy("shop", "api")


def test_old_database_is_migrated(tmp_path):
    p = tmp_path / "old.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE jobs(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, idem_key TEXT, "
              "payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0, "
              "max_attempts INTEGER NOT NULL DEFAULT 3, run_after REAL NOT NULL DEFAULT 0, lease_owner TEXT, "
              "lease_until REAL, error TEXT, created REAL, updated REAL, UNIQUE(tenant, idem_key))")
    c.execute("INSERT INTO jobs(tenant,payload) VALUES('t','{}')")
    c.commit()
    c.close()
    s = Store(p)
    assert s.claim("w")["fence"] == 1


def test_zombie_cannot_overwrite_the_final_checkpoint(store, make_worker, scenarios):
    """Found by the chaos run: a worker froze before a node, another worker finished the whole job, then the
    zombie woke up and wrote its stale checkpoint on top of the final one. Checkpoint writes are fenced now."""
    sc = scenarios["oom-api"]
    cluster = FakeCluster(sc)
    w1, _ = make_worker(sc, cluster=cluster, lease_s=0.3)
    jid, _ = store.enqueue("t", sc["incident"])
    frozen = threading.Event()
    real_hb = store.heartbeat
    store.heartbeat = lambda j, w, lease_s=30.0: False if (w == w1.id and frozen.is_set()) else real_hb(j, w, lease_s)

    def stall(point, node):
        if (point, node) == ("before", "retrieve"):
            frozen.set()
            time.sleep(0.5)
            w2, _ = make_worker(sc, cluster=cluster)
            drive(store, w2, jid)                 # w2 finishes everything while w1 is frozen
    with_fault(w1, stall)
    orig = w1.make_ctx

    def make(job):
        ctx = orig(job)
        ctx.check_lease = lambda: None            # the zombie does not know it lost the lease
        return ctx
    w1.make_ctx = make
    w1.run_once()
    assert store.get_job(jid)["status"] == "succeeded"
    assert final(store, jid)["outcome"] == "resolved"
    assert "fenced" in [e["kind"] for e in store.events(jid)]

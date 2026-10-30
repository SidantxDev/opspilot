import time

import pytest

from conftest import with_fault
from opspilot.cluster import FakeCluster
from opspilot.graph import WorkerCrash
from opspilot.llm import HeuristicLLM, LLMResult
from opspilot.policy import validate
from opspilot.rag import RunbookIndex
from opspilot.bootstrap import ROOT


def drive(store, worker, jid, decision="approve", max_steps=10):
    for _ in range(max_steps):
        worker.run_once()
        st = store.get_job(jid)["status"]
        if st == "waiting_approval":
            store.add_approval(jid, decision, "tester")
        elif st in ("succeeded", "failed"):
            return st
    raise AssertionError("did not finish")


def final(store, jid):
    return store.latest_checkpoint(jid)[1]


def test_happy_path_pauses_for_human_then_resolves(store, make_worker, scenarios):
    w, cluster = make_worker(scenarios["oom-api"])
    jid, _ = store.enqueue("t", scenarios["oom-api"]["incident"])
    w.run_once()
    assert store.get_job(jid)["status"] == "waiting_approval"
    assert cluster.applied == []                         # nothing touched the cluster before approval
    prop = final(store, jid)["proposal"]
    assert prop["action"] == "set_memory_limit"
    store.add_approval(jid, "approve", "sre-oncall")
    w.run_once()
    st = final(store, jid)
    assert store.get_job(jid)["status"] == "succeeded"
    assert st["outcome"] == "resolved" and len(cluster.applied) == 1


def test_rejection_never_touches_cluster(store, make_worker, scenarios):
    w, cluster = make_worker(scenarios["oom-api"])
    jid, _ = store.enqueue("t", scenarios["oom-api"]["incident"])
    drive(store, w, jid, decision="reject")
    assert final(store, jid)["outcome"] == "rejected" and cluster.applied == []


def crash_at(point, node):
    def hook(p, n):
        if (p, n) == (point, node):
            raise WorkerCrash()
    return hook


@pytest.mark.parametrize("node", ["investigate", "retrieve", "diagnose", "guardrails"])
def test_crash_after_node_resumes_without_redoing_it(store, make_worker, scenarios, node):
    sc = scenarios["oom-api"]
    w1, cluster = make_worker(sc, lease_s=0.2)
    with_fault(w1, crash_at("after", node))
    jid, _ = store.enqueue("t", sc["incident"])
    with pytest.raises(WorkerCrash):
        w1.run_once()
    assert store.get_job(jid)["status"] == "running"     # died holding the lease
    time.sleep(0.3)                                      # lease expires
    w2, _ = make_worker(sc, cluster=cluster)             # a different worker, same cluster
    assert drive(store, w2, jid) == "succeeded"
    assert final(store, jid)["outcome"] == "resolved"
    assert cluster.reads["logs"] == 1                    # investigate ran exactly once across both workers
    assert len(cluster.applied) == 1


def test_crash_after_apply_does_not_apply_twice(store, make_worker, scenarios):
    sc = scenarios["oom-api"]
    w1, cluster = make_worker(sc, lease_s=0.2)
    with_fault(w1, crash_at("before", "verify"))         # fix applied and checkpointed, worker dies before verify
    jid, _ = store.enqueue("t", sc["incident"])
    w1.run_once()                                        # pauses for approval
    store.add_approval(jid, "approve", "me")
    with pytest.raises(WorkerCrash):
        w1.run_once()
    assert len(cluster.applied) == 1
    time.sleep(0.3)
    w2, _ = make_worker(sc, cluster=cluster)
    assert drive(store, w2, jid) == "succeeded"
    assert len(cluster.applied) == 1
    assert final(store, jid)["outcome"] == "resolved"


def test_crash_between_effect_and_checkpoint_is_safe_because_apply_is_idempotent(store, make_worker, scenarios):
    sc = scenarios["oom-api"]
    cluster = FakeCluster(sc)
    real_apply = cluster.apply

    def apply_then_die(*a, **k):                         # the effect happens, then the process dies pre-checkpoint
        real_apply(*a, **k)
        raise WorkerCrash()
    cluster.apply = apply_then_die
    w1, _ = make_worker(sc, cluster=cluster, lease_s=0.2)
    jid, _ = store.enqueue("t", sc["incident"])
    w1.run_once()
    store.add_approval(jid, "approve", "me")
    with pytest.raises(WorkerCrash):
        w1.run_once()
    cluster.apply = real_apply
    time.sleep(0.3)
    w2, _ = make_worker(sc, cluster=cluster)
    assert drive(store, w2, jid) == "succeeded"
    assert len(cluster.applied) == 2                     # at-least-once: re-applied, which is a no-op declaratively
    assert len({(a["action"], str(a["params"])) for a in cluster.applied}) == 1
    assert final(store, jid)["outcome"] == "resolved"


class FlakyLLM:
    """Fails once with a transient error, then behaves."""
    model = "flaky"

    def __init__(self):
        self.calls, self.inner = 0, HeuristicLLM()

    def complete(self, system, user, max_tokens=1024):
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError("upstream 529")
        return self.inner.complete(system, user, max_tokens)


def test_transient_llm_error_is_retried_and_resumes(store, make_worker, scenarios):
    llm = FlakyLLM()
    w, cluster = make_worker(scenarios["oom-api"], llm=llm)
    jid, _ = store.enqueue("t", scenarios["oom-api"]["incident"])
    assert drive(store, w, jid) == "succeeded"
    job = store.get_job(jid)
    assert job["attempts"] >= 2 and final(store, jid)["outcome"] == "resolved"
    assert cluster.reads["logs"] == 1                    # investigation was not repeated on retry


class AlwaysBadLLM:
    model = "bad"

    def complete(self, system, user, max_tokens=1024):
        return LLMResult("sorry, I cannot help", 10, 5, "bad")


def test_garbage_model_output_ends_in_escalation_not_a_change(store, make_worker, scenarios):
    w, cluster = make_worker(scenarios["oom-api"], llm=AlwaysBadLLM())
    jid, _ = store.enqueue("t", scenarios["oom-api"]["incident"])
    assert drive(store, w, jid) == "succeeded"
    st = final(store, jid)
    assert st["outcome"] == "escalated" and cluster.applied == [] and st["revisions"] == 2


def test_tenant_budget_stops_the_run(store, make_worker, scenarios):
    w, _ = make_worker(scenarios["oom-api"])
    orig = w.make_ctx

    def make(job):
        ctx = orig(job)
        ctx.max_tokens_per_tenant = 1
        return ctx
    w.make_ctx = make
    store.add_usage("t", None, 100, 100, 0.0, "x")
    jid, _ = store.enqueue("t", scenarios["oom-api"]["incident"])
    w.run_once()
    job = store.get_job(jid)
    assert job["status"] == "failed" and "tokens" in job["error"]


def test_secrets_are_redacted_before_reaching_the_model(store, make_worker, scenarios):
    seen = []

    class Spy(HeuristicLLM):
        def complete(self, system, user, max_tokens=1024):
            seen.append(user)
            return super().complete(system, user, max_tokens)

    w, _ = make_worker(scenarios["secret-in-logs"], llm=Spy())
    jid, _ = store.enqueue("t", scenarios["secret-in-logs"]["incident"])
    drive(store, w, jid)
    assert seen and all("hunter2" not in p and "sk_live_51Hxxx" not in p for p in seen)


def test_guardrails_block_protected_namespace_and_bad_params():
    inc = {"namespace": "kube-system", "workload": "coredns"}
    assert validate({"category": "oom", "action": "set_memory_limit", "params": {"memory": "512Mi"},
                     "confidence": 0.9}, inc)
    ok = {"namespace": "shop", "workload": "api"}
    assert validate({"category": "oom", "action": "set_memory_limit", "params": {"memory": "64Gi"},
                     "confidence": 0.9}, ok)
    assert validate({"category": "oom", "action": "delete_namespace", "params": {}, "confidence": 1}, ok)
    assert validate({"category": "oom", "action": "scale", "params": {"replicas": 0}, "confidence": 0.9}, ok)
    assert validate({"category": "oom", "action": "set_memory_limit", "params": {"memory": "512Mi"},
                     "confidence": 0.2}, ok)
    assert not validate({"category": "oom", "action": "set_memory_limit", "params": {"memory": "512Mi"},
                         "confidence": 0.9}, ok)


def test_retrieval_finds_the_right_runbook(rag):
    cases = {"Last State Terminated OOMKilled Exit Code 137 memory limit": "oom-killed.md",
             "Failed to pull image manifest unknown ErrImagePull": "image-pull-failures.md",
             "Readiness probe failed connection refused port": "probe-misconfiguration.md",
             "FailedScheduling Insufficient cpu Pending": "unschedulable-pods.md"}
    for q, want in cases.items():
        assert rag.search(q, k=2)[0]["source"] == want, q

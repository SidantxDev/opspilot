"""resourceVersion preconditions: the API server, not just the database, enforces the fence.

FakeAPIServer stands in for kube-apiserver behind KubectlCluster._exec. It applies JSON patches (add/replace) and
does what the real one does with a patched resourceVersion: if it differs from the stored one, 409 Conflict.
The same flow runs against a real cluster in evals/minikube_preconditions.py.
"""
import copy
import json
import subprocess

import pytest

from opspilot.cluster import ANNOTATION, KubectlCluster, StaleWrite, build_patch
from opspilot.worker import Worker


def deployment():
    return {"metadata": {"name": "api", "resourceVersion": "100", "generation": 1, "annotations": {}},
            "spec": {"replicas": 1, "template": {"metadata": {"labels": {"app": "api"}}, "spec": {"containers": [
                {"name": "api", "image": "api:1", "resources": {"limits": {"memory": "48Mi"}},
                 "env": [{"name": "A", "value": "1"}],
                 "readinessProbe": {"httpGet": {"path": "/", "port": 81}}}]}}}}


def _unesc(p):
    return p.replace("~1", "/").replace("~0", "~")


def json_patch(doc, ops):
    doc = copy.deepcopy(doc)
    for op in ops:
        *parents, last = [_unesc(x) for x in op["path"].lstrip("/").split("/")]
        node = doc
        for k in parents:
            node = node[int(k)] if isinstance(node, list) else node[k]
        if isinstance(node, list):
            node[int(last)] = op["value"]
        elif op["op"] == "replace" and last not in node:
            raise KeyError(op["path"])
        else:
            node[last] = op["value"]
    return doc


class FakeAPIServer(KubectlCluster):
    def __init__(self, dep=None):
        super().__init__()
        self.dep, self.writes = dep or deployment(), 0

    def human_edit(self):
        self.dep["spec"]["replicas"] += 1
        self.dep["metadata"]["generation"] += 1
        self._bump()

    def controller_status_write(self):
        """What the deployment controller does all the time: new resourceVersion, same spec and generation."""
        self.dep.setdefault("status", {})["observedGeneration"] = self.dep["metadata"]["generation"]
        self.dep["metadata"]["annotations"]["deployment.kubernetes.io/revision"] = "2"
        self._bump()

    def _bump(self):
        self.dep["metadata"]["resourceVersion"] = str(int(self.dep["metadata"]["resourceVersion"]) + 1)

    def _exec(self, args, timeout=30):
        args = args[2:]  # drop "-n ns"
        if args[:2] == ["get", "deploy/api"]:
            return subprocess.CompletedProcess(args, 0, json.dumps(self.dep), "")
        if args[:2] == ["patch", "deploy/api"]:
            new = json_patch(self.dep, json.loads(args[args.index("-p") + 1]))
            if new["metadata"]["resourceVersion"] != self.dep["metadata"]["resourceVersion"]:
                return subprocess.CompletedProcess(args, 1, "", 'Error from server (Conflict): Operation cannot be '
                                                   'fulfilled on deployments.apps "api": the object has been modified')
            if new["spec"] != self.dep["spec"]:
                new["metadata"]["generation"] += 1
                self.writes += 1
            self.dep = new
            self._bump()
            return subprocess.CompletedProcess(args, 0, "deployment.apps/api patched", "")
        raise AssertionError(f"unexpected kubectl call {args}")


def c0(dep):
    return dep["spec"]["template"]["spec"]["containers"][0]


@pytest.mark.parametrize("action,params,check", [
    ("set_memory_limit", {"memory": "192Mi"}, lambda d: c0(d)["resources"]["limits"]["memory"] == "192Mi"),
    ("set_cpu_request", {"cpu": "100m"}, lambda d: c0(d)["resources"] == {"limits": {"memory": "48Mi"},
                                                                         "requests": {"cpu": "100m"}}),
    ("set_image", {"image": "api:2"}, lambda d: c0(d)["image"] == "api:2"),
    ("scale", {"replicas": 3}, lambda d: d["spec"]["replicas"] == 3),
    ("set_env", {"key": "A", "value": "2"}, lambda d: c0(d)["env"] == [{"name": "A", "value": "2"}]),
    ("set_probe_port", {"port": 80}, lambda d: c0(d)["readinessProbe"]["httpGet"]["port"] == 80),
    ("restart", {}, lambda d: "kubectl.kubernetes.io/restartedAt" in d["spec"]["template"]["metadata"]["annotations"]),
])
def test_every_action_is_one_pinned_patch_with_attribution(action, params, check):
    api = FakeAPIServer()
    concrete = api.prepare("shop", "api", action, params)
    assert concrete["resource_version"] == "100"
    api.apply("shop", "api", action, concrete, op_id="job-7-f1")
    assert check(api.dep) and api.writes == 1
    assert api.dep["metadata"]["annotations"][ANNOTATION] == "job-7-f1"   # in the same write, not a second call


def test_patches_are_declarative_so_reapplying_changes_nothing():
    dep = deployment()
    for action, params in [("set_memory_limit", {"memory": "1Gi"}), ("set_env", {"key": "B"}),
                           ("restart", {}), ("scale", {"replicas": 2})]:
        once = json_patch(dep, build_patch(dep, action, params, now="T"))
        assert json_patch(once, build_patch(dep, action, params, now="T")) == once


def test_reissue_after_crash_is_recognised_not_repeated():
    """The patch landed, the worker died before committing, the next owner re-issues the ledgered row."""
    api = FakeAPIServer()
    concrete = api.prepare("shop", "api", "scale", {"replicas": 3})
    api.apply("shop", "api", "scale", concrete, op_id="job-7-f1")
    out = api.apply("shop", "api", "scale", concrete, op_id="job-7-f2")      # new fence, same job
    assert out == "already applied by job-7-f1" and api.writes == 1


def test_zombie_late_write_is_rejected_by_the_api_server():
    """w1 recorded the intent and froze inside its kubectl call; w2 took over and applied the same row.
    w1's call finally reaches the server: the version moved on, so it cannot land a second time."""
    api = FakeAPIServer()
    concrete = api.prepare("shop", "api", "set_memory_limit", {"memory": "192Mi"})
    api.apply("shop", "api", "set_memory_limit", concrete, op_id="job-7-f2")   # w2
    api.apply("shop", "api", "set_memory_limit", concrete, op_id="job-7-f1")   # w1, late
    assert api.writes == 1 and api.dep["metadata"]["annotations"][ANNOTATION] == "job-7-f2"


def test_controller_status_writes_are_not_mistaken_for_a_conflict():
    """Found on minikube: right after a rollout the controller bumps resourceVersion without touching the spec.
    That must re-pin and land, not fail the job, and must keep the controller's own annotation."""
    api = FakeAPIServer()
    concrete = api.prepare("shop", "api", "set_image", {"image": "api:2"})
    api.controller_status_write()
    api.apply("shop", "api", "set_image", concrete, op_id="job-7-f1")
    ann = api.dep["metadata"]["annotations"]
    assert c0(api.dep)["image"] == "api:2" and api.writes == 1
    assert ann[ANNOTATION] == "job-7-f1" and ann["deployment.kubernetes.io/revision"] == "2"


def test_a_human_change_after_the_intent_is_never_overwritten():
    api = FakeAPIServer()
    concrete = api.prepare("shop", "api", "scale", {"replicas": 3})
    api.human_edit()                                           # someone scales it by hand meanwhile
    with pytest.raises(StaleWrite):
        api.apply("shop", "api", "scale", concrete, op_id="job-7-f1")
    assert api.dep["spec"]["replicas"] == 2 and api.writes == 0


def test_another_jobs_change_is_a_conflict_too():
    api = FakeAPIServer()
    mine = api.prepare("shop", "api", "scale", {"replicas": 3})
    other = api.prepare("shop", "api", "set_image", {"image": "api:2"})
    api.apply("shop", "api", "set_image", other, op_id="job-8-f1")
    with pytest.raises(StaleWrite):
        api.apply("shop", "api", "scale", mine, op_id="job-7-f1")


def test_unprepared_params_are_refused():
    with pytest.raises(ValueError):
        FakeAPIServer().apply("shop", "api", "scale", {"replicas": 3}, op_id="job-7-f1")


def test_stale_write_fails_the_job_without_retrying(store, rag, scenarios):
    """StaleWrite is permanent: backing off and re-sending the same pinned patch would only conflict again."""
    from opspilot.agents import build_graph
    from opspilot.bootstrap import make_ctx_factory
    from opspilot.llm import HeuristicLLM

    sc = scenarios["oom-api"]

    class Racing(FakeAPIServer):
        def logs(self, ns, wl): return sc["cluster"]["logs"]
        def events(self, ns, wl): return sc["cluster"]["events"]
        def describe(self, ns, wl): return sc["cluster"]["describe"]
        def wait_healthy(self, ns, wl, timeout=0): return True

        def prepare(self, ns, wl, action, params):
            out = super().prepare(ns, "api", action, params)
            self.human_edit()                                  # the race: a hand edit right after the intent
            return out

        def apply(self, ns, wl, action, params, op_id=None):
            return super().apply(ns, "api", action, params, op_id)

    api = Racing()
    w = Worker(store, build_graph(), make_ctx_factory(store, HeuristicLLM(), api, rag), lease_s=30, backoff_s=0)
    jid, _ = store.enqueue("t", sc["incident"], max_attempts=3)
    w.run_once()
    store.add_approval(jid, "approve", "me")
    w.run_once()
    job = store.get_job(jid)
    assert job["status"] == "failed" and "StaleWrite" in job["error"]
    assert api.writes == 0

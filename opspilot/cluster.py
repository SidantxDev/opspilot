"""Cluster access. FakeCluster replays recorded incidents (evals, tests, demos); KubectlCluster talks to a real one.

Every mutating call is idempotent, because a crashed worker may re-issue it. `prepare` turns a proposal into a
concrete change *once*, before the intent is written to the ledger; e.g. "rollback" becomes "set the pod template
to revision N's template", which is safe to repeat, where `kubectl rollout undo` twice would flip back to the bad
release.
"""
import json
import re
import subprocess
import time

from .policy import NAME_RE, parse_cpu_m, parse_mi
from .runtime import PermanentError

ANNOTATION = "opspilot.io/last-change"


class StaleWrite(PermanentError):
    """The deployment changed after the intent was recorded, and not by this job. Retrying the same patch cannot
    succeed, and overwriting a change a human or another controller made is exactly what must not happen."""


def _esc(key):
    return key.replace("~", "~0").replace("/", "~1")     # RFC 6901 JSON pointer escaping


def job_of(op_id):
    """'job-7-f3' -> 'job-7': every fence of one job carries the same single change."""
    return op_id.rsplit("-f", 1)[0] if op_id else None


def build_patch(dep, action, params, now=None):
    """Turn a proposal into a declarative JSON patch against this exact deployment object. Pure, so it is tested
    without a cluster. Re-applying the result is a no-op, which is what makes re-issue after a crash safe."""
    cs = dep["spec"]["template"]["spec"]["containers"]
    base = "/spec/template/spec/containers"
    ops = []
    if action in ("set_memory_limit", "set_cpu_request"):
        kind, res, val = (("limits", "memory", params["memory"]) if action == "set_memory_limit"
                          else ("requests", "cpu", params["cpu"]))
        for i, c in enumerate(cs):
            r = json.loads(json.dumps(c.get("resources") or {}))
            r.setdefault(kind, {})[res] = val
            ops.append({"op": "add", "path": f"{base}/{i}/resources", "value": r})
    elif action == "set_image":
        ops = [{"op": "add", "path": f"{base}/{i}/image", "value": params["image"]} for i in range(len(cs))]
    elif action == "rollback":
        ops = [{"op": "replace", "path": "/spec/template", "value": params["template"]}]
    elif action == "scale":
        ops = [{"op": "add", "path": "/spec/replicas", "value": int(params["replicas"])}]
    elif action == "set_env":
        key, val = params["key"], str(params.get("value", ""))
        for i, c in enumerate(cs):
            env = [e for e in (c.get("env") or []) if e.get("name") != key] + [{"name": key, "value": val}]
            ops.append({"op": "add", "path": f"{base}/{i}/env", "value": env})
    elif action == "set_probe_port":
        if not (cs[0].get("readinessProbe") or {}).get("httpGet"):
            raise ValueError("first container has no httpGet readiness probe")
        ops = [{"op": "replace", "path": f"{base}/0/readinessProbe/httpGet/port", "value": int(params["port"])}]
    elif action == "restart":   # what `kubectl rollout restart` does, with the timestamp fixed once, here
        meta = dict(dep["spec"]["template"].get("metadata") or {})
        meta["annotations"] = {**(meta.get("annotations") or {}),
                               "kubectl.kubernetes.io/restartedAt":
                                   now or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        ops = [{"op": "add", "path": "/spec/template/metadata", "value": meta}]
    else:
        raise ValueError(f"unsupported action {action}")
    return ops


def fix_matches(correct, action, params):
    """Does (action, params) satisfy a scenario's accepted-fix spec?"""
    if correct["action"] != action:
        return False
    for k, v in (correct.get("params") or {}).items():
        if str(params.get(k)) != str(v):
            return False
    if "min_mi" in correct and (parse_mi(params.get("memory", "")) or 0) < correct["min_mi"]:
        return False
    if "min_replicas" in correct and not (isinstance(params.get("replicas"), int)
                                          and params["replicas"] >= correct["min_replicas"]):
        return False
    if "max_cpu_m" in correct and not 0 < (parse_cpu_m(params.get("cpu", "")) or 10**9) <= correct["max_cpu_m"]:
        return False
    if "image_contains" in correct and correct["image_contains"] not in str(params.get("image", "")):
        return False
    return True


class FakeCluster:
    """Replays one scenario. Becomes healthy only if an accepted fix was applied."""

    def __init__(self, scenario, store=None):
        self.sc = scenario
        self.store = store     # optional: derive health from the shared action ledger (multi-process demo)
        self.applied = []      # every apply() call, in order (checked by evals for forbidden actions)
        self.reads = {"logs": 0, "events": 0, "describe": 0}

    def _read(self, kind):
        self.reads[kind] += 1
        return self.sc["cluster"][kind]

    def logs(self, ns, workload):
        return self._read("logs")

    def events(self, ns, workload):
        return self._read("events")

    def describe(self, ns, workload):
        return self._read("describe")

    def prepare(self, ns, workload, action, params):
        return params

    def apply(self, ns, workload, action, params, op_id=None):
        self.applied.append({"namespace": ns, "workload": workload, "action": action, "params": params,
                             "op_id": op_id})
        return f"applied {action} {params} to {ns}/{workload}"

    def is_healthy(self, ns, workload):
        if self.store is not None:   # the latest committed change wins, whichever worker process made it
            last = self.store.latest_action_for(ns, workload)
            return bool(last) and any(fix_matches(c, last["action"], last["params"])
                                      for c in self.sc.get("correct", []))
        return any(fix_matches(c, a["action"], a["params"])
                   for c in self.sc.get("correct", []) for a in self.applied)

    def wait_healthy(self, ns, workload, timeout=0):
        return self.is_healthy(ns, workload)


class DemoCluster:
    """Routes each (namespace, workload) to the recorded scenario for it, so a live demo can replay any incident."""

    def __init__(self, scenarios, store=None):
        self.by_target = {(s["incident"]["namespace"], s["incident"]["workload"]): FakeCluster(s, store)
                          for s in scenarios}

    def _for(self, ns, workload):
        c = self.by_target.get((ns, workload))
        if not c:
            raise LookupError(f"no recorded scenario for {ns}/{workload}")
        return c

    def logs(self, ns, workload):
        return self._for(ns, workload).logs(ns, workload)

    def events(self, ns, workload):
        return self._for(ns, workload).events(ns, workload)

    def describe(self, ns, workload):
        return self._for(ns, workload).describe(ns, workload)

    def prepare(self, ns, workload, action, params):
        return self._for(ns, workload).prepare(ns, workload, action, params)

    def apply(self, ns, workload, action, params, op_id=None):
        return self._for(ns, workload).apply(ns, workload, action, params, op_id)

    def is_healthy(self, ns, workload):
        return self._for(ns, workload).is_healthy(ns, workload)

    def wait_healthy(self, ns, workload, timeout=0):
        return self._for(ns, workload).wait_healthy(ns, workload, timeout)


class KubectlCluster:
    """Real cluster via the kubectl binary. Arguments are passed as a list (never through a shell) and names
    are validated first. Exercised end to end on minikube by evals/minikube_e2e.py."""

    def __init__(self, kubectl="kubectl", context=None):
        self.base = [kubectl] + (["--context", context] if context else [])

    def _exec(self, args, timeout=30):
        return subprocess.run(self.base + args, capture_output=True, text=True, timeout=timeout)

    def _run(self, args, timeout=30):
        p = self._exec(args, timeout)
        return (p.stdout + ("\n" + p.stderr if p.returncode else "")).strip()

    def _must(self, args, timeout=30):
        """Writes: a failure raises, so the worker retries (re-issuing the same ledgered change) instead of
        recording a change that never happened."""
        p = self._exec(args, timeout)
        if p.returncode:
            raise RuntimeError(f"kubectl {' '.join(args[:4])} failed: {p.stderr.strip()[:300]}")
        return p.stdout.strip()

    @staticmethod
    def _check(ns, workload):
        if not (NAME_RE.match(ns) and NAME_RE.match(workload)):
            raise ValueError("invalid namespace/workload name")

    def logs(self, ns, workload):
        self._check(ns, workload)
        cur = self._run(["-n", ns, "logs", f"deploy/{workload}", "--tail=100", "--all-containers"])
        prev = self._run(["-n", ns, "logs", f"deploy/{workload}", "--tail=50", "--previous"])
        return cur + ("\n--- previous container ---\n" + prev if prev and "Error" not in prev[:20] else "")

    def events(self, ns, workload):
        self._check(ns, workload)
        out = self._run(["-n", ns, "get", "events", "--sort-by=.lastTimestamp"])
        return "\n".join(l for l in out.splitlines() if workload in l or l.startswith("LAST"))

    def describe(self, ns, workload):
        self._check(ns, workload)
        dep = self._run(["-n", ns, "describe", f"deploy/{workload}"])
        pods = self._run(["-n", ns, "get", "pods", "-o", "name"])
        first = next((p for p in pods.splitlines() if f"/{workload}-" in p), None)
        return dep + ("\n" + self._run(["-n", ns, "describe", first]) if first else "")

    def _get(self, ns, workload):
        return json.loads(self._must(["-n", ns, "get", f"deploy/{workload}", "-o", "json"]))

    def prepare(self, ns, workload, action, params):
        """Resolve a proposal into one concrete JSON patch, pinned to the resourceVersion it was computed against,
        before it is written to the ledger. The pin is what lets the API server enforce the fence."""
        self._check(ns, workload)
        dep = self._get(ns, workload)
        if action == "rollback":
            def rev(o):
                return int(o["metadata"].get("annotations", {}).get("deployment.kubernetes.io/revision", 0))
            owned = [rs for rs in json.loads(self._must(["-n", ns, "get", "rs", "-o", "json"]))["items"]
                     if any(o.get("kind") == "Deployment" and o.get("name") == workload
                            for o in rs["metadata"].get("ownerReferences", []))]
            prev = max((rs for rs in owned if rev(rs) < rev(dep)), key=rev, default=None)
            if prev is None:
                raise ValueError(f"{ns}/{workload} has no earlier revision to roll back to")
            tpl = prev["spec"]["template"]
            tpl.get("metadata", {}).get("labels", {}).pop("pod-template-hash", None)
            params = {"to_revision": rev(prev), "template": tpl}
        return {**params, "patch": build_patch(dep, action, params),
                "generation": dep["metadata"]["generation"],
                "resource_version": dep["metadata"]["resourceVersion"],
                "has_annotations": bool(dep["metadata"].get("annotations"))}

    def apply(self, ns, workload, action, params, op_id=None, max_repins=5):
        """One PATCH carrying the change, the attribution annotation and a resourceVersion precondition.

        resourceVersion moves on *any* write, including the deployment controller's status updates, so the
        thing actually pinned is metadata.generation, which only moves when the spec changes:
        - Object unchanged since prepare: the patch lands, the version moves on, so only one writer can win.
        - 409 Conflict, spec unchanged (a status or annotation write): re-pin to the new version and send again.
          The server still checks every attempt.
        - 409 Conflict, spec changed, annotation names this job: our change already landed (an earlier attempt
          that crashed before committing, or the other side of a zombie race). Report it, do not write again.
        - 409 Conflict, spec changed by anyone else: StaleWrite. Fail the job rather than clobber the change."""
        self._check(ns, workload)
        if "patch" not in params:
            raise ValueError("change must be prepared into a concrete patch first")
        rv, has_ann = params["resource_version"], params.get("has_annotations")
        for _ in range(max_repins + 1):
            ops = [{"op": "replace", "path": "/metadata/resourceVersion", "value": rv}]
            if op_id and NAME_RE.match(op_id):    # attribution on the object itself: "who changed this?"
                ops.append({"op": "add", "path": f"/metadata/annotations/{_esc(ANNOTATION)}", "value": op_id}
                           if has_ann else {"op": "add", "path": "/metadata/annotations", "value": {ANNOTATION: op_id}})
            p = self._exec(["-n", ns, "patch", f"deploy/{workload}", "--type=json", "-p",
                            json.dumps(ops + params["patch"])])
            if not p.returncode:
                return p.stdout.strip()
            err = p.stderr.strip()
            if "the object has been modified" not in err and "Conflict" not in err:
                raise RuntimeError(f"kubectl patch failed: {err[:300]}")
            cur = self._get(ns, workload)["metadata"]
            last = (cur.get("annotations") or {}).get(ANNOTATION)
            if op_id and job_of(last) == job_of(op_id):
                return f"already applied by {last}"
            if cur["generation"] != params["generation"]:
                raise StaleWrite(f"{ns}/{workload} spec changed after generation {params['generation']} "
                                 f"(last change: {last or 'not by OpsPilot'}); refusing to overwrite it")
            rv, has_ann = cur["resourceVersion"], bool(cur.get("annotations"))
        raise RuntimeError(f"{ns}/{workload}: still conflicting after {max_repins} re-pins")

    def is_healthy(self, ns, workload):
        return "successfully rolled out" in self._run(
            ["-n", ns, "rollout", "status", f"deploy/{workload}", "--timeout=5s"])

    def wait_healthy(self, ns, workload, timeout=60):
        end = time.time() + timeout
        while time.time() < end:
            if self.is_healthy(ns, workload):
                return True
            time.sleep(3)
        return False

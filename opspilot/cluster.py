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

    def _run(self, args, timeout=30):
        p = subprocess.run(self.base + args, capture_output=True, text=True, timeout=timeout)
        return (p.stdout + ("\n" + p.stderr if p.returncode else "")).strip()

    def _must(self, args, timeout=30):
        """Writes: a failure raises, so the worker retries (re-issuing the same ledgered change) instead of
        recording a change that never happened."""
        p = subprocess.run(self.base + args, capture_output=True, text=True, timeout=timeout)
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

    def prepare(self, ns, workload, action, params):
        """Resolve a proposal into a concrete, repeatable change before it is written to the ledger."""
        self._check(ns, workload)
        if action != "rollback":
            return params

        def rev(o):
            return int(o["metadata"].get("annotations", {}).get("deployment.kubernetes.io/revision", 0))
        dep = json.loads(self._must(["-n", ns, "get", f"deploy/{workload}", "-o", "json"]))
        owned = [rs for rs in json.loads(self._must(["-n", ns, "get", "rs", "-o", "json"]))["items"]
                 if any(o.get("kind") == "Deployment" and o.get("name") == workload
                        for o in rs["metadata"].get("ownerReferences", []))]
        prev = max((rs for rs in owned if rev(rs) < rev(dep)), key=rev, default=None)
        if prev is None:
            raise ValueError(f"{ns}/{workload} has no earlier revision to roll back to")
        tpl = prev["spec"]["template"]
        tpl.get("metadata", {}).get("labels", {}).pop("pod-template-hash", None)
        return {"to_revision": rev(prev), "template": tpl}

    def apply(self, ns, workload, action, params, op_id=None):
        self._check(ns, workload)
        d = f"deploy/{workload}"
        n = ["-n", ns]
        if action == "set_memory_limit":
            out = self._must(n + ["set", "resources", d, f"--limits=memory={params['memory']}"])
        elif action == "set_cpu_request":
            out = self._must(n + ["set", "resources", d, f"--requests=cpu={params['cpu']}"])
        elif action == "set_image":
            out = self._must(n + ["set", "image", d, f"*={params['image']}"])
        elif action == "rollback" and "template" in params:   # declarative: same template however often it runs
            patch = [{"op": "replace", "path": "/spec/template", "value": params["template"]}]
            out = self._must(n + ["patch", d, "--type=json", "-p", json.dumps(patch)])
        elif action == "rollback":
            raise ValueError("rollback must be prepared into a concrete revision first")
        elif action == "scale":
            out = self._must(n + ["scale", d, f"--replicas={int(params['replicas'])}"])
        elif action == "set_env":
            out = self._must(n + ["set", "env", d, f"{params['key']}={params.get('value', '')}"])
        elif action == "set_probe_port":
            patch = [{"op": "replace", "path": "/spec/template/spec/containers/0/readinessProbe/httpGet/port",
                      "value": int(params["port"])}]
            out = self._must(n + ["patch", d, "--type=json", "-p", json.dumps(patch)])
        elif action == "restart":
            out = self._must(n + ["rollout", "restart", d])
        else:
            raise ValueError(f"unsupported action {action}")
        if op_id and NAME_RE.match(op_id):    # attribution on the object itself: "who changed this?"
            self._must(n + ["annotate", d, f"opspilot.io/last-change={op_id}", "--overwrite"])
        return out

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

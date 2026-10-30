"""Cluster access. FakeCluster replays recorded incidents (evals, tests, demos); KubectlCluster talks to a real one.

Every mutating call is idempotent (declarative set/scale/undo-to-previous), because a crashed worker may repeat it.
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

    def __init__(self, scenario):
        self.sc = scenario
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

    def apply(self, ns, workload, action, params):
        self.applied.append({"namespace": ns, "workload": workload, "action": action, "params": params})
        return f"applied {action} {params} to {ns}/{workload}"

    def is_healthy(self, ns, workload):
        return any(fix_matches(c, a["action"], a["params"])
                   for c in self.sc.get("correct", []) for a in self.applied)

    def wait_healthy(self, ns, workload, timeout=0):
        return self.is_healthy(ns, workload)


class KubectlCluster:
    """Real cluster via the kubectl binary. Arguments are passed as a list (never through a shell) and names
    are validated first. NOTE: written against kubectl docs; run it against a throwaway cluster (kind/minikube)
    before trusting it."""

    def __init__(self, kubectl="kubectl", context=None):
        self.base = [kubectl] + (["--context", context] if context else [])

    def _run(self, args, timeout=30):
        p = subprocess.run(self.base + args, capture_output=True, text=True, timeout=timeout)
        return (p.stdout + ("\n" + p.stderr if p.returncode else "")).strip()

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

    def apply(self, ns, workload, action, params):
        self._check(ns, workload)
        d = f"deploy/{workload}"
        n = ["-n", ns]
        if action == "set_memory_limit":
            return self._run(n + ["set", "resources", d, f"--limits=memory={params['memory']}"])
        if action == "set_cpu_request":
            return self._run(n + ["set", "resources", d, f"--requests=cpu={params['cpu']}"])
        if action == "set_image":
            return self._run(n + ["set", "image", d, f"*={params['image']}"])
        if action == "rollback":
            return self._run(n + ["rollout", "undo", d])
        if action == "scale":
            return self._run(n + ["scale", d, f"--replicas={int(params['replicas'])}"])
        if action == "set_env":
            return self._run(n + ["set", "env", d, f"{params['key']}={params.get('value', '')}"])
        if action == "set_probe_port":
            patch = [{"op": "replace", "path": "/spec/template/spec/containers/0/readinessProbe/httpGet/port",
                      "value": int(params["port"])}]
            return self._run(n + ["patch", d, "--type=json", "-p", json.dumps(patch)])
        if action == "restart":
            return self._run(n + ["rollout", "restart", d])
        raise ValueError(f"unsupported action {action}")

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

"""End-to-end on a real cluster: break workloads on purpose, let OpsPilot fix them through KubectlCluster.

  minikube start
  python evals/minikube_e2e.py [--context minikube]

Uses the heuristic model so the run is deterministic and free; the point is the real kubectl path (evidence
collection, guardrails against live `describe` output, ledgered changes, verification by rollout status).
Everything happens in the throwaway namespace `opspilot-e2e`, which is deleted and recreated first.
"""
import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from opspilot.agents import build_graph  # noqa: E402
from opspilot.bootstrap import ROOT, make_ctx_factory  # noqa: E402
from opspilot.cluster import KubectlCluster  # noqa: E402
from opspilot.graph import WorkerCrash  # noqa: E402
from opspilot.llm import HeuristicLLM  # noqa: E402
from opspilot.rag import RunbookIndex  # noqa: E402
from opspilot.store import Store  # noqa: E402
from opspilot.worker import Worker  # noqa: E402

NS = "opspilot-e2e"
GOOD, BAD = "nginx:1.27-alpine", "nginx:0.0.0-does-not-exist"


def deployment(name, image, replicas=1, command=None, memory=None, probe_port=None):
    c = {"name": name, "image": image}
    if command:
        c["command"] = ["sh", "-c", command]
    if memory:
        c["resources"] = {"limits": {"memory": memory}}
    if probe_port:
        c["readinessProbe"] = {"httpGet": {"path": "/", "port": probe_port}, "periodSeconds": 2}
    return {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name, "namespace": NS},
            "spec": {"replicas": replicas, "selector": {"matchLabels": {"app": name}},
                     "template": {"metadata": {"labels": {"app": name}}, "spec": {"containers": [c]}}}}


CASES = [
    # name, manifest, image to break it with afterwards (None = broken from the start), symptom to wait for,
    # expected outcome, check on the live object afterwards
    ("api-oom", deployment("api-oom", "busybox:1.36", command="dd if=/dev/zero of=/dev/null bs=150M count=1 "
                           "&& echo warmed && sleep 3600", memory="48Mi"),
     None, "OOMKilled", "resolved",
     lambda d: d["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"] == "192Mi"),
    ("checkout", deployment("checkout", GOOD), BAD, "ImagePullBackOff|ErrImagePull", "resolved",
     lambda d: d["spec"]["template"]["spec"]["containers"][0]["image"] == GOOD),
    ("search", deployment("search", GOOD, replicas=0), None, None, "resolved",
     lambda d: d["spec"]["replicas"] == 3),
    # a known miss of the keyword baseline: it reads "connection refused" as a crash and restarts, which is
    # harmless but cannot fix a wrong probe port. Verification must catch that (fix_failed, never "resolved").
    ("web-probe", deployment("web-probe", GOOD, probe_port=8080), None, "Readiness probe failed", "fix_failed",
     lambda d: d["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"]["port"] == 8080),
]


class Kube:
    def __init__(self, context):
        self.base = ["kubectl", "--context", context]

    def __call__(self, *args, stdin=None, check=True):
        p = subprocess.run(self.base + list(args), input=stdin, capture_output=True, text=True, timeout=180)
        if check and p.returncode:
            raise RuntimeError(f"kubectl {' '.join(args)}: {p.stderr.strip()}")
        return p.stdout

    def deploy(self, name):
        return json.loads(self("-n", NS, "get", "deploy", name, "-o", "json"))


def wait_for(fn, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        if fn():
            return True
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {what}")


def setup(k, name, manifest, break_image, symptom):
    k("apply", "-f", "-", stdin=json.dumps(manifest))
    if break_image:
        k("-n", NS, "rollout", "status", f"deploy/{name}", "--timeout=180s")
        k("-n", NS, "set", "image", f"deploy/{name}", f"{name}={break_image}")
    if symptom:
        import re
        wait_for(lambda: re.search(symptom, k("-n", NS, "describe", "pods", "-l", f"app={name}", check=False)
                                   + k("-n", NS, "get", "events", check=False)), 180, f"{name}: {symptom}")


def run_incident(store, cluster, rag, name, fault=None, llm=None):
    worker = Worker(store, build_graph(), make_ctx_factory(store, llm or HeuristicLLM(), cluster, rag, verify_timeout=180),
                    lease_s=0.5 if fault else 30, backoff_s=0)
    if fault:
        orig = worker.make_ctx

        def make(job):
            ctx = orig(job)
            ctx.fault_hook = fault
            return ctx
        worker.make_ctx = make
    jid, _ = store.enqueue("e2e", {"namespace": NS, "workload": name, "alert": f"{name} unhealthy"})
    crashed = False
    for _ in range(15):
        try:
            worker.run_once()
        except WorkerCrash:
            crashed = True
            time.sleep(0.7)                    # lease expires; a fresh worker takes over below
            worker = Worker(store, build_graph(),
                            make_ctx_factory(store, llm or HeuristicLLM(), cluster, rag, verify_timeout=180), backoff_s=0)
            continue
        st = store.get_job(jid)["status"]
        if st == "waiting_approval":
            store.add_approval(jid, "approve", "e2e")
        elif st in ("succeeded", "failed"):
            break
    return jid, crashed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--context", default="minikube")
    a = ap.parse_args()
    k = Kube(a.context)
    k("delete", "namespace", NS, "--ignore-not-found", "--wait=true")
    k("create", "namespace", NS)
    cases = CASES + [("checkout-crash", deployment("checkout-crash", GOOD), BAD, "ImagePullBackOff|ErrImagePull",
                      "resolved", lambda d: d["spec"]["template"]["spec"]["containers"][0]["image"] == GOOD)]
    for name, manifest, brk, symptom, *_ in cases:
        print(f"breaking {name} ...", flush=True)
        setup(k, name, manifest, brk, symptom)

    cluster, rag = KubectlCluster(context=a.context), RunbookIndex.from_dir(ROOT / "runbooks")
    results, ok_all = [], True
    with tempfile.TemporaryDirectory() as d:
        store = Store(Path(d) / "e2e.db")
        for name, _, _, _, expected, check in cases:
            t0 = time.time()
            fault = None
            if name == "checkout-crash":      # die right after the rollback hit the cluster, before the ledger commit
                fired = []

                def fault(point, node):
                    if (point, node) == ("effect", "apply") and not fired:
                        fired.append(1)
                        raise WorkerCrash()
            jid, crashed = run_incident(store, cluster, rag, name, fault)
            state = (store.latest_checkpoint(jid) or (None, {}))[1]
            dep = k.deploy(name)
            ledger = store.get_action(jid)
            row = {"workload": name, "action": (state.get("proposal") or {}).get("action"),
                   "outcome": state.get("outcome"), "expected": expected, "live_check": bool(check(dep)),
                   "annotation": dep["metadata"].get("annotations", {}).get("opspilot.io/last-change"),
                   "ledger_issues": ledger and ledger["issues"], "crashed_mid_apply": crashed,
                   "seconds": round(time.time() - t0, 1), "error": store.get_job(jid)["error"]}
            if name == "checkout-crash":
                hist = k("-n", NS, "rollout", "history", f"deploy/{name}")
                row["revisions"] = len([l for l in hist.splitlines() if l.strip()[:1].isdigit()])
                row["ok"] = (row["outcome"] == expected and row["live_check"] and crashed
                             and ledger["issues"] == 2 and row["revisions"] == 2)
            else:
                row["ok"] = row["outcome"] == expected and row["live_check"]
            ok_all &= row["ok"]
            results.append(row)
            print(json.dumps(row), flush=True)
    out = HERE / "results" / f"minikube-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps({"context": a.context, "results": results,
                               "kubectl": k("version", "--client", "-o", "json", check=False)[:400]}, indent=2))
    print(f"\n{sum(r['ok'] for r in results)}/{len(results)} passed; wrote {out.relative_to(ROOT)}")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()

"""The fence, enforced by the real API server: resourceVersion preconditions on minikube.

  minikube start
  python evals/minikube_preconditions.py [--context minikube]

Three races that the database fence alone cannot stop, because they happen inside the one `kubectl` call:
  zombie   w2 applies the ledgered change, then frozen w1's identical call finally arrives -> no second write
  reissue  the change landed, the worker died before committing, the next owner re-sends it -> recognised
  human    someone edits the deployment after the intent was recorded -> StaleWrite, their edit survives
Writes are counted with metadata.generation, which the API server bumps on every spec change.
"""
import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from minikube_e2e import GOOD, NS, Kube, deployment  # noqa: E402
from opspilot.bootstrap import ROOT  # noqa: E402
from opspilot.cluster import ANNOTATION, KubectlCluster, StaleWrite  # noqa: E402


def gen(k, name):
    return k.deploy(name)["metadata"]["generation"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--context", default="minikube")
    a = ap.parse_args()
    k, cl = Kube(a.context), KubectlCluster(context=a.context)
    k("delete", "namespace", NS, "--ignore-not-found", "--wait=true")
    k("create", "namespace", NS)
    for n in ("zombie", "reissue", "human"):
        k("apply", "-f", "-", stdin=json.dumps(deployment(n, GOOD)))
    results = []

    # zombie: both workers hold the same ledger row (same pinned resourceVersion), w2 wins, w1 arrives late
    plan = cl.prepare(NS, "zombie", "scale", {"replicas": 2})
    g0 = gen(k, "zombie")
    cl.apply(NS, "zombie", "scale", plan, op_id="job-1-f2")
    late = cl.apply(NS, "zombie", "scale", plan, op_id="job-1-f1")
    d = k.deploy("zombie")
    results.append({"race": "zombie", "late_call": late, "writes": d["metadata"]["generation"] - g0,
                    "annotation": d["metadata"]["annotations"].get(ANNOTATION),
                    "ok": d["metadata"]["generation"] - g0 == 1 and late.startswith("already applied")
                    and d["metadata"]["annotations"].get(ANNOTATION) == "job-1-f2"})

    # reissue: the same job re-sends its ledgered change under a new fence after a crash
    plan = cl.prepare(NS, "reissue", "set_image", {"image": "nginx:1.27"})
    g0 = gen(k, "reissue")
    cl.apply(NS, "reissue", "set_image", plan, op_id="job-2-f1")
    again = cl.apply(NS, "reissue", "set_image", plan, op_id="job-2-f2")
    d = k.deploy("reissue")
    results.append({"race": "reissue", "second_call": again, "writes": d["metadata"]["generation"] - g0,
                    "ok": d["metadata"]["generation"] - g0 == 1 and again.startswith("already applied")})

    # human: a hand edit between the recorded intent and the call must not be overwritten
    plan = cl.prepare(NS, "human", "scale", {"replicas": 3})
    k("-n", NS, "scale", "deploy/human", "--replicas=5")
    err = None
    try:
        cl.apply(NS, "human", "scale", plan, op_id="job-3-f1")
    except StaleWrite as e:
        err = str(e)
    d = k.deploy("human")
    results.append({"race": "human", "error": err, "replicas": d["spec"]["replicas"],
                    "ok": err is not None and d["spec"]["replicas"] == 5
                    and ANNOTATION not in (d["metadata"].get("annotations") or {})})

    for r in results:
        print(json.dumps(r), flush=True)
    out = HERE / "results" / f"minikube-preconditions-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps({"context": a.context, "results": results}, indent=2))
    print(f"\n{sum(r['ok'] for r in results)}/{len(results)} passed; wrote {out.relative_to(ROOT)}")
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()

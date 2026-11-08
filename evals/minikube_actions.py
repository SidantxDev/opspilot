"""Real-cluster coverage for the actions minikube_e2e.py does not reach: set_image, set_cpu_request, set_env,
set_probe_port.

  minikube start
  python evals/minikube_actions.py [--context minikube]
  python evals/minikube_actions.py --llm gemini    # real model diagnoses real broken workloads (needs GEMINI_API_KEY)

The keyword baseline cannot choose these fixes, so a scripted "oracle" model returns the correct proposal for each
workload. That is intentional: this script tests the actuation path (guardrails against live `describe` output,
ledgered `kubectl` change, verification by rollout status, attribution annotation), not diagnosis quality, which
evals/run_evals.py measures. Everything runs in the throwaway namespace `opspilot-e2e`.
"""
import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from minikube_e2e import BAD, CASES as BASE_CASES, GOOD, NS, Kube, deployment, run_incident, setup  # noqa: E402
from opspilot.bootstrap import ROOT  # noqa: E402
from opspilot.cluster import KubectlCluster  # noqa: E402
from opspilot.llm import LLMResult, _section  # noqa: E402
from opspilot.rag import RunbookIndex  # noqa: E402
from opspilot.store import Store  # noqa: E402

ENV_APP = ('test -n "${APP_MODE+set}" || { echo "missing required environment variable APP_MODE"; exit 1; }; '
           'sleep 3600')


def container(d):
    return d["spec"]["template"]["spec"]["containers"][0]


class OracleLLM:
    """Returns the correct proposal for each workload. Stands in for a perfect diagnosis."""
    model = "scripted-oracle"

    def __init__(self, plans):
        self.plans = plans

    def complete(self, system, user, max_tokens=1024):
        import re
        wl = re.search(r"^workload: (\S+)", user, re.M).group(1)
        cat, action, params = self.plans[wl]
        text = json.dumps({"category": cat, "action": action, "params": params, "confidence": 0.9,
                           "explanation": f"scripted fix for {wl}"})
        return LLMResult(text, len(user) // 4, len(text) // 4, self.model)


def cpu_deploy(name):
    d = deployment(name, GOOD)
    container(d)["resources"] = {"requests": {"cpu": "64"}}      # no minikube node has 64 cores
    return d


# name, manifest, image to break with after rollout (None = broken from the start), symptom to wait for,
# scripted plan, check on the live object
CASES = [
    ("cpu-hog", cpu_deploy("cpu-hog"), None, "Insufficient cpu",
     ("insufficient_resources", "set_cpu_request", {"cpu": "100m"}),
     lambda d: container(d)["resources"]["requests"]["cpu"] == "100m"),
    ("needs-env", deployment("needs-env", "busybox:1.36", command=ENV_APP), None, "CrashLoopBackOff|Error",
     ("missing_config", "set_env", {"key": "APP_MODE"}),
     lambda d: any(e["name"] == "APP_MODE" for e in container(d).get("env", []))),
    ("bad-tag", deployment("bad-tag", BAD), None, "ImagePullBackOff|ErrImagePull",
     ("bad_image", "set_image", {"image": GOOD}),
     lambda d: container(d)["image"] == GOOD),
    ("probe-port", deployment("probe-port", GOOD, probe_port=8080), None, "Readiness probe failed",
     ("probe_misconfig", "set_probe_port", {"port": 80}),
     lambda d: container(d)["readinessProbe"]["httpGet"]["port"] == 80),
]


def real_model_cases():
    """Real-model mode: the four cases above plus the baseline cases, all expected to be fixed (web-probe too,
    which the keyword baseline cannot do). The model gets no hint about which action to use."""
    extra = [(n, m, b, sym, None, chk) for n, m, b, sym, _exp, chk in BASE_CASES]
    return CASES + extra


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--context", default="minikube")
    ap.add_argument("--llm", choices=["oracle", "gemini"], default="oracle")
    a = ap.parse_args()
    cases = CASES if a.llm == "oracle" else real_model_cases()
    k = Kube(a.context)
    k("delete", "namespace", NS, "--ignore-not-found", "--wait=true")
    k("create", "namespace", NS)
    for name, manifest, brk, symptom, *_ in cases:
        print(f"breaking {name} ...", flush=True)
        setup(k, name, manifest, brk, symptom)

    if a.llm == "gemini":
        from opspilot.llm import GeminiLLM
        llm = GeminiLLM()
    else:
        llm = OracleLLM({c[0]: c[4] for c in CASES})
    cluster, rag = KubectlCluster(context=a.context), RunbookIndex.from_dir(ROOT / "runbooks")
    results = []
    with tempfile.TemporaryDirectory() as d:
        store = Store(Path(d) / "actions.db")
        for name, _, _, _, plan, check in cases:
            t0 = time.time()
            jid, _ = run_incident(store, cluster, rag, name, llm=llm)
            state = (store.latest_checkpoint(jid) or (None, {}))[1]
            dep = k.deploy(name)
            row = {"workload": name, "action": (state.get("proposal") or {}).get("action"),
                   "expected_action": plan and plan[1], "outcome": state.get("outcome"), "live_check": bool(check(dep)),
                   "annotation": dep["metadata"].get("annotations", {}).get("opspilot.io/last-change"),
                   "seconds": round(time.time() - t0, 1), "error": store.get_job(jid)["error"]}
            row["ok"] = (row["outcome"] == "resolved" and (plan is None or row["action"] == plan[1])
                         and row["live_check"] and bool(row["annotation"]))
            results.append(row)
            print(json.dumps(row), flush=True)
    out = HERE / "results" / f"minikube-{a.llm}-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps({"context": a.context, "llm": getattr(llm, "model", a.llm), "results": results}, indent=2))
    print(f"\n{sum(r['ok'] for r in results)}/{len(results)} passed; wrote {out.relative_to(ROOT)}")
    sys.exit(0 if all(r["ok"] for r in results) else 1)


if __name__ == "__main__":
    main()

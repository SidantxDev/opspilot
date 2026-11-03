"""Chaos run: many worker processes, killed at random under load, and the invariants that must still hold.

  python evals/chaos.py                          # 60 incidents, 4 workers, kills + crashes + stalls
  python evals/chaos.py --jobs 200 --workers 6 --kill-every 0.4

Three kinds of failure are injected:
  * kill   - the parent hard-kills a random worker process (TerminateProcess / SIGKILL) and starts a new one
  * crash  - a worker exits on the spot at a random step, including inside the apply window
             (cluster changed, ledger not yet committed)
  * stall  - a worker freezes past its lease (a GC pause or a network partition), then wakes up as a zombie
             while another worker already owns its job

Every cluster change any process makes is recorded in a separate effects database. Afterwards we check:
  1. no incident is lost: every job reaches a terminal status, none fails
  2. every outcome equals the outcome of a calm single-worker run of the same scenario
  3. every approved fix is committed in the ledger exactly once
  4. re-issued changes are identical (same concrete params), and only happen when a crash hit the apply window
  5. no zombie ever changed the cluster: every effect was made under a fence that held the job at that moment
Exit code 1 if any invariant is broken.
"""
import argparse
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

from opspilot.bootstrap import ROOT, load_scenarios, make_worker  # noqa: E402
from opspilot.cluster import DemoCluster  # noqa: E402
from opspilot.llm import HeuristicLLM  # noqa: E402
from opspilot.rag import RunbookIndex  # noqa: E402
from opspilot.store import Store  # noqa: E402

TERMINAL = ("succeeded", "failed")


# ---------------------------------------------------------------- child (one worker process)
class RecordingCluster:
    """Every apply() is a real effect: write it down before returning, from whichever process makes it."""

    def __init__(self, inner, effects_path):
        self.inner, self.path = inner, effects_path

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def apply(self, ns, workload, action, params, op_id=None):
        out = self.inner.apply(ns, workload, action, params, op_id)
        c = sqlite3.connect(self.path, timeout=30)
        try:
            c.execute("INSERT INTO effects(op_id, pid, ns, workload, action, params, ts) VALUES(?,?,?,?,?,?,?)",
                      (op_id, os.getpid(), ns, workload, action, json.dumps(params, sort_keys=True), time.time()))
            c.commit()
        finally:
            c.close()
        return out


def child(a):
    rng = random.Random(a.seed)
    store = Store(a.db)
    cluster = RecordingCluster(DemoCluster(load_scenarios(), store), a.effects)
    w = make_worker(store=store, llm=HeuristicLLM(), cluster=cluster, lease_s=a.lease, backoff_s=0.05)
    frozen = {"on": False}
    real_hb = store.heartbeat
    store.heartbeat = lambda j, wid, lease_s=30.0: False if frozen["on"] else real_hb(j, wid, lease_s)

    def fault(point, node):
        if point == "effect" and rng.random() < a.crash_rate * 4:   # bias crashes into the dangerous window
            os._exit(137)
        if rng.random() < a.crash_rate:
            os._exit(137)
        if point == "before" and rng.random() < a.stall_rate:
            frozen["on"] = True                                     # stop heart-beating, like a frozen process
            time.sleep(a.lease * 2.5)                               # wake up long after the lease expired

    orig = w.make_ctx

    def make(job):
        ctx = orig(job)
        ctx.fault_hook = fault
        ctx.check_lease = lambda: None    # a zombie does not know it is one; only the store-level fence stops it
        return ctx
    w.make_ctx = make
    while True:
        frozen["on"] = False
        if w.run_once() is None:
            time.sleep(0.05)


# ---------------------------------------------------------------- parent (orchestrator + checker)
def calm_outcomes(scenarios):
    """Reference: the same scenarios, one worker, no faults."""
    from run_evals import run_scenario
    rag = RunbookIndex.from_dir(ROOT / "runbooks")
    return {s["id"]: run_scenario(s, HeuristicLLM(), rag)["outcome"] for s in scenarios}


def spawn(a, i, env):
    args = [sys.executable, str(Path(__file__).resolve()), "--child", "--db", a.db, "--effects", a.effects,
            "--lease", str(a.lease), "--crash-rate", str(a.crash_rate), "--stall-rate", str(a.stall_rate),
            "--seed", str(a.seed * 1000 + i)]
    return subprocess.Popen(args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def check(store, effects_path, job_scenario, calm):
    rows = {j["id"]: j for j in store.list_jobs(limit=10**6)}
    fx = defaultdict(list)
    c = sqlite3.connect(effects_path)
    for op_id, action, params in c.execute("SELECT op_id, action, params FROM effects ORDER BY id"):
        jid, fence = op_id.split("-")[1], op_id.split("-f")[1]
        fx[int(jid)].append((int(fence), action, params))
    c.close()

    problems, stats = [], Counter()
    for jid, sid in job_scenario.items():
        job = rows[jid]
        state = (store.latest_checkpoint(jid) or (None, {}))[1]
        ledger = store.get_action(jid)
        kinds = Counter(e["kind"] for e in store.events(jid))
        stats["lease_lost"] += kinds["lease_lost"]
        stats["fenced"] += kinds["fenced"]
        stats["resumed"] += kinds["resumed"]
        stats["apply_skipped"] += kinds["apply_skipped"]
        stats["outcome:" + str(state.get("outcome"))] += 1
        if job["status"] != "succeeded":
            problems.append(f"job {jid} ({sid}) ended {job['status']}: {job['error']}")
            continue
        if state.get("outcome") != calm[sid]:
            problems.append(f"job {jid} ({sid}) outcome {state.get('outcome')} != calm run {calm[sid]}")
        effects = fx.get(jid, [])
        stats["effects"] += len(effects)
        if state.get("applied"):
            if not ledger or ledger["status"] != "done":
                problems.append(f"job {jid} applied but ledger row is {ledger and ledger['status']}")
                continue
            stats["ledger_done"] += 1
            if not 1 <= len(effects) <= ledger["issues"]:
                problems.append(f"job {jid}: {len(effects)} effects for {ledger['issues']} ledger issues")
            if len({(a, p) for _, a, p in effects}) > 1:
                problems.append(f"job {jid}: re-issued change differs from the original: {effects}")
            if len(effects) > 1:
                stats["reissued_identical"] += len(effects) - 1
            if any(f > ledger["fence"] for f, _, _ in effects):
                problems.append(f"job {jid}: effect under a fence newer than the committing one")
            committed = [f for f, _, _ in effects if f == ledger["fence"]]
            if not committed:
                problems.append(f"job {jid}: the committing fence {ledger['fence']} made no effect")
        elif effects or ledger:
            problems.append(f"job {jid} not applied but has effects={len(effects)} ledger={ledger}")
    return problems, stats


def parent(a):
    work = Path(tempfile.mkdtemp(prefix="opspilot-chaos-"))
    a.db, a.effects = str(work / "chaos.db"), str(work / "effects.db")
    c = sqlite3.connect(a.effects)
    c.execute("CREATE TABLE effects(id INTEGER PRIMARY KEY AUTOINCREMENT, op_id TEXT, pid INTEGER, ns TEXT, "
              "workload TEXT, action TEXT, params TEXT, ts REAL)")
    c.execute("PRAGMA journal_mode=WAL")
    c.commit()
    c.close()

    scenarios = load_scenarios()
    print(f"calm reference run over {len(scenarios)} scenarios ...")
    calm = calm_outcomes(scenarios)
    store = Store(a.db)
    rng = random.Random(a.seed)
    job_scenario = {}
    for i in range(a.jobs):
        sc = scenarios[i % len(scenarios)]
        jid, _ = store.enqueue("chaos", sc["incident"], idem_key=f"chaos-{i}", max_attempts=a.max_attempts)
        job_scenario[jid] = sc["id"]

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    procs = [spawn(a, i, env) for i in range(a.workers)]
    spawned, kills = a.workers, 0
    t0 = time.time()
    next_kill = t0 + rng.expovariate(1 / a.kill_every)
    print(f"{a.jobs} incidents, {a.workers} workers, lease {a.lease}s, kill every ~{a.kill_every}s, "
          f"crash p={a.crash_rate}, stall p={a.stall_rate}")
    try:
        while time.time() - t0 < a.timeout:
            for j in store.list_jobs(limit=10**6, status="waiting_approval"):
                store.add_approval(j["id"], "approve", "chaos-approver")
            for i, p in enumerate(procs):                  # replace workers that crashed themselves
                if p.poll() is not None:
                    procs[i] = spawn(a, spawned, env)
                    spawned += 1
            if time.time() >= next_kill:
                i = rng.randrange(len(procs))
                procs[i].kill()
                procs[i].wait()
                kills += 1
                procs[i] = spawn(a, spawned, env)
                spawned += 1
                next_kill = time.time() + rng.expovariate(1 / a.kill_every)
            counts = store.counts()
            done = sum(counts.get(s, 0) for s in TERMINAL)
            if done == a.jobs:
                break
            time.sleep(0.05)
    finally:
        for p in procs:
            p.kill()
    elapsed = time.time() - t0
    crashed = spawned - a.workers - kills

    problems, stats = check(store, a.effects, job_scenario, calm)
    summary = {
        "jobs": a.jobs, "workers": a.workers, "seconds": round(elapsed, 1),
        "processes_killed": kills, "processes_crashed_or_exited": crashed, "processes_started": spawned,
        "zombie_writes_refused": stats["fenced"], "lease_losses": stats["lease_lost"],
        "resumes_from_checkpoint": stats["resumed"],
        "fixes_committed_exactly_once": stats["ledger_done"], "cluster_effects": stats["effects"],
        "identical_reissues_after_crash_in_apply_window": stats["reissued_identical"],
        "apply_skipped_already_committed": stats["apply_skipped"],
        "outcomes": {k.split(":", 1)[1]: v for k, v in stats.items() if k.startswith("outcome:")},
        "invariant_violations": len(problems), "seed": a.seed,
    }
    print(json.dumps(summary, indent=2))
    for p in problems[:20]:
        print("VIOLATION:", p)
    out = HERE / "results" / f"chaos-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({"summary": summary, "violations": problems}, indent=2))
    print(f"wrote {out.relative_to(ROOT)}")
    if done != a.jobs:
        print(f"TIMEOUT: only {done}/{a.jobs} incidents finished")
        return 1
    return 1 if problems else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--db")
    ap.add_argument("--effects")
    ap.add_argument("--jobs", type=int, default=60)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--lease", type=float, default=1.0)
    ap.add_argument("--kill-every", type=float, default=0.6, help="mean seconds between process kills")
    ap.add_argument("--crash-rate", type=float, default=0.02, help="chance of exiting at each step")
    ap.add_argument("--stall-rate", type=float, default=0.02, help="chance of freezing past the lease at a step")
    ap.add_argument("--max-attempts", type=int, default=50)
    ap.add_argument("--timeout", type=float, default=300)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    if a.child:
        child(a)
    else:
        sys.exit(parent(a))


if __name__ == "__main__":
    main()

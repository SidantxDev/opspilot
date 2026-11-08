"""Races that sequential tests cannot see. Each one runs against every backend (Postgres when OPSPILOT_TEST_PG is
set): on SQLite the db lock serializes them, on Postgres the row locks and SKIP LOCKED have to."""
import random
import threading
import time
from collections import Counter

from opspilot.store import open_store


def race(n, fn):
    """Start n threads behind a barrier so they really collide, return their results."""
    gate, out, errs = threading.Barrier(n), [None] * n, []

    def run(i):
        gate.wait()
        try:
            out[i] = fn(i)
        except Exception as e:  # noqa: BLE001
            errs.append(e)
    ts = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errs, errs
    return out


def test_concurrent_claimers_never_get_the_same_job(store):
    jobs = 40
    for i in range(jobs):
        store.enqueue("t", {"i": i})

    def drain(i):
        got = []
        while (j := store.claim(f"w{i}")) is not None:
            got.append(j["id"])
        return got
    claimed = [jid for got in race(8, drain) for jid in got]
    assert len(claimed) == jobs and len(set(claimed)) == jobs
    assert store.counts() == {"running": jobs}


def test_duplicate_alert_deliveries_create_one_job(store):
    ids = race(8, lambda i: store.enqueue("t", {"alert": 1}, idem_key="dup-alert"))
    assert len({jid for jid, _ in ids}) == 1
    assert Counter(created for _, created in ids) == {True: 1, False: 7}


def test_two_humans_racing_to_decide_only_one_decision_lands(store):
    jid, _ = store.enqueue("t", {})
    store.claim("w")
    store.pause(jid, "w")
    won = race(6, lambda i: store.add_approval(jid, "approve" if i % 2 else "reject", f"human{i}"))
    assert won.count(True) == 1
    assert store.get_job(jid)["status"] == "queued"


def test_reclaim_racing_a_zombie_commit_has_exactly_one_winner(store):
    """The fence check and the fence bump must be serialized: either the old owner commits its change and the new
    owner sees it as done, or the new owner wins and the old owner is refused. Never both, never neither."""
    rng = random.Random(3)
    outcomes = Counter()
    for round_ in range(30):
        jid, _ = store.enqueue("t", {}, idem_key=f"r{round_}")
        old = store.claim(f"old{round_}", lease_s=30)
        assert store.begin_action(jid, old["lease_owner"], old["fence"], "ns", "wl", "restart", {})
        store.heartbeat(jid, old["lease_owner"], lease_s=0.03)     # lease ends ~now: commit and reclaim collide
        jitter = rng.uniform(0, 0.06)

        def go(i):
            if i == 0:
                time.sleep(jitter)
                return ("commit", store.complete_action(jid, old["lease_owner"], old["fence"], "ok"))
            while (j := store.claim(f"new{round_}", lease_s=30)) is None:
                time.sleep(0.001)
            return ("claim", j)
        res = dict(race(2, go))
        row, new = store.get_action(jid), res["claim"]
        assert new["id"] == jid and new["fence"] == old["fence"] + 1
        assert (row["status"] == "done") == res["commit"]
        if res["commit"]:
            assert row["finished"] <= new["updated"]      # the commit landed before the fence moved, never after
        outcomes[res["commit"]] += 1
        store.finish(jid, new["lease_owner"], "succeeded")
    assert outcomes[True] and outcomes[False], outcomes  # both orders actually happened


def test_backend_is_picked_from_the_url(tmp_path):
    s = open_store(tmp_path / "x.db")
    assert type(s).__name__ == "Store"

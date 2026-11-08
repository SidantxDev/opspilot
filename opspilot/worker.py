"""Worker: claims a job under a lease, heartbeats while running, and maps every outcome onto a job status."""
import os
import threading
import time
import uuid

from .graph import LeaseLost
from .runtime import BudgetExceeded, PermanentError


class Worker:
    def __init__(self, store, graph, make_ctx, worker_id=None, lease_s=30.0, backoff_s=2.0):
        self.store, self.graph, self.make_ctx = store, graph, make_ctx
        self.id = worker_id or f"w-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.lease_s, self.backoff_s = lease_s, backoff_s

    def run_once(self):
        """Process at most one job. Returns the job id handled, or None if the queue was empty."""
        job = self.store.claim(self.id, self.lease_s)
        if not job:
            return None
        jid, stop, lost = job["id"], threading.Event(), threading.Event()

        def beat():
            while not stop.wait(self.lease_s / 3):
                if not self.store.heartbeat(jid, self.id, self.lease_s):
                    lost.set()
                    return

        threading.Thread(target=beat, daemon=True).start()
        ctx = self.make_ctx(job)
        ctx.lease_ok = lambda: not lost.is_set()
        ctx.worker_id, ctx.fence = self.id, job["fence"]
        try:
            if self.graph.run(ctx) == "interrupted":
                self.store.pause(jid, self.id)
            else:
                self.store.finish(jid, self.id, "succeeded")
        except LeaseLost:
            self.store.add_event(jid, "lease_lost", data={"worker": self.id})
        except BudgetExceeded as e:        # retrying cannot help
            self.store.add_event(jid, "budget_exceeded", data={"error": str(e)})
            self.store.finish(jid, self.id, "failed", str(e))
        except PermanentError as e:        # e.g. StaleWrite: the same patch would conflict again
            err = f"{type(e).__name__}: {e}"
            self.store.add_event(jid, "error", data={"error": err, "attempt": job["attempts"], "permanent": True})
            self.store.finish(jid, self.id, "failed", err)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            self.store.add_event(jid, "error", data={"error": err, "attempt": job["attempts"]})
            if job["attempts"] >= job["max_attempts"]:
                self.store.finish(jid, self.id, "failed", err)
            else:
                self.store.retry(jid, self.id, err, self.backoff_s * 2 ** (job["attempts"] - 1))
        finally:
            stop.set()
        return jid

    def run_forever(self, poll_s=1.0):
        while True:
            if self.run_once() is None:
                time.sleep(poll_s)

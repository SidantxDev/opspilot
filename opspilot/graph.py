"""Minimal durable state-graph engine.

Each node is `fn(state, ctx) -> dict of updates`. After every node the merged state is checkpointed, so a job
that is re-claimed after a crash continues from the next node. A node may raise Interrupt to pause the whole
run (human-in-the-loop); the node is re-executed on resume, so it must read the human's answer from the store.

Delivery semantics: a node's side effects are at-least-once (a crash between the effect and its checkpoint
re-runs the node), so side-effecting nodes must be idempotent.
"""
import time

END = "__end__"


class Interrupt(Exception):
    """Pause the run until a human acts."""


class LeaseLost(Exception):
    """Another worker owns this job now; stop without touching it."""


class WorkerCrash(BaseException):
    """Fault injection: simulates the process being killed (BaseException so nothing handles it)."""


class Graph:
    def __init__(self, entry):
        self.entry = entry
        self.nodes = {}
        self.edges = {}

    def node(self, name):
        def deco(fn):
            self.nodes[name] = fn
            return fn
        return deco

    def edge(self, src, dst):
        """dst: a node name, END, or a callable(state) -> node name."""
        self.edges[src] = dst

    def _next(self, node, state):
        dst = self.edges.get(node, END)
        return dst(state) if callable(dst) else dst

    def run(self, ctx):
        store, job = ctx.store, ctx.job_id
        cp = store.latest_checkpoint(job)
        if cp:
            last, state = cp
            node = self._next(last, state)
            store.add_event(job, "resumed", last, {"next": node})
        else:
            state, node = dict(ctx.initial_state), self.entry
        while node != END:
            ctx.check_lease()
            ctx.fault("before", node)
            store.add_event(job, "node_start", node)
            t0 = time.monotonic()
            try:
                updates = self.nodes[node](state, ctx) or {}
            except Interrupt as i:
                store.add_event(job, "interrupt", node, {"reason": str(i)})
                return "interrupted"
            state = {**state, **updates}
            store.save_checkpoint(job, node, state)
            store.add_event(job, "node_end", node, {"ms": round((time.monotonic() - t0) * 1000, 1),
                                                    "updated": sorted(updates)})
            ctx.fault("after", node)
            node = self._next(node, state)
        return "done"

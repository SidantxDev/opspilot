"""Per-run context handed to every graph node: store, tools, LLM access with budget + usage accounting."""
import os
from dataclasses import dataclass, field
from typing import Any, Callable

from .graph import LeaseLost


class PermanentError(Exception):
    """Retrying cannot help; the worker fails the job instead of backing off."""


class BudgetExceeded(PermanentError):
    pass


@dataclass
class RunContext:
    store: Any
    job_id: int
    tenant: str
    llm: Any
    cluster: Any
    rag: Any
    initial_state: dict
    max_tokens_per_tenant: int = int(os.environ.get("OPSPILOT_TENANT_TOKEN_BUDGET", "2000000"))
    verify_timeout: float = 60.0
    max_revisions: int = 2
    lease_ok: Callable[[], bool] = lambda: True
    worker_id: Any = None      # set by the Worker; None (direct graph runs) skips fencing
    fence: int = 0
    fault_hook: Callable[[str, str], None] = lambda point, node: None
    config: dict = field(default_factory=dict)

    def check_lease(self):
        if not self.lease_ok():
            raise LeaseLost()

    def fault(self, point, node):
        self.fault_hook(point, node)

    def complete(self, node, system, user, max_tokens=1024):
        """The only way nodes talk to the model: enforces the tenant budget and records every call."""
        spent = self.store.tenant_spend(self.tenant)["tokens"]
        if spent >= self.max_tokens_per_tenant:
            raise BudgetExceeded(f"tenant '{self.tenant}' used {spent} tokens (cap {self.max_tokens_per_tenant})")
        r = self.llm.complete(system, user, max_tokens)
        self.store.add_usage(self.tenant, self.job_id, r.tokens_in, r.tokens_out, r.cost_usd, r.model)
        self.store.add_event(self.job_id, "llm_call", node,
                             {"model": r.model, "tokens_in": r.tokens_in, "tokens_out": r.tokens_out,
                              "cost_usd": round(r.cost_usd, 6)})
        return r.text

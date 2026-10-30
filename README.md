# OpsPilot

A durable, multi-step incident responder for Kubernetes. An alert comes in, OpsPilot gathers evidence from the
cluster, retrieves the relevant runbook, has an LLM propose a fix, checks that fix against deterministic
guardrails, **waits for a human to approve it**, applies it, verifies the rollout, and writes a report.

The interesting part is not the LLM call. It is everything around it that makes an agent safe to point at
production:

- **Durable execution.** Every step is checkpointed to SQLite. Kill the worker at any point and another one
  resumes from the last completed step, without redoing it and without applying a fix twice.
- **Leases and fencing.** Workers claim jobs under a heartbeated lease. A crashed worker's job is reclaimed, and
  the old worker is fenced off so it cannot write results after losing the lease.
- **Human in the loop.** The run parks in `waiting_approval` (no worker held, no attempt consumed) until someone
  approves or rejects through the API.
- **Guardrails outside the model.** The model can only pick from a closed set of actions, on the alerting
  workload only, within numeric bounds. Protected namespaces and low confidence force an escalation. Prompt
  injection in pod logs cannot widen what the system is able to do.
- **Evals that gate CI.** 12 recorded incidents are replayed end to end through the real runtime and scored on
  diagnosis, outcome, safety, cost, and latency.

## Architecture

```
            POST /incidents                     POST /incidents/{id}/approval
                  |                                        |
                  v                                        v
   +-----------------------------+        +-------------------------------+
   |  FastAPI (api.py)           |        |  SQLite store (store.py)      |
   |  validation, idempotency,   |------->|  jobs (leases, attempts)      |
   |  tenant header              |        |  checkpoints, events, usage   |
   +-----------------------------+        |  approvals                    |
                                          +-------------------------------+
                                                   ^        |
                                   claim / heartbeat|        | resume from checkpoint
                                                   |        v
                                          +-------------------------------+
                                          |  Worker(s) (worker.py)        |
                                          |  runs the graph (graph.py)    |
                                          +-------------------------------+

  investigate --> retrieve --> diagnose --> guardrails --(violations, revisions left)--> diagnose
   logs/events     BM25 over     LLM, JSON      policy.py     |
   describe,       runbooks/     only                         |--(change proposed)--> approval --> apply --> verify --> report
   secrets                                                    |                       (interrupt)   kubectl   rollout
   redacted                                                   |--(escalate / out of revisions)---------------------> report
```

| Module | Responsibility |
|---|---|
| `store.py` | Job queue with leases, retries with exponential backoff, idempotent enqueue, checkpoints, event trace, approvals, per-tenant token usage |
| `worker.py` | Claims a job, heartbeats the lease, maps the run's result to a job status (succeeded, retry, failed, paused) |
| `graph.py` | Minimal state-graph engine: nodes, conditional edges, checkpoint after every node, resume, interrupts |
| `agents.py` | The incident-response nodes and the system prompt |
| `policy.py` | Deterministic guardrails: action allowlist, parameter bounds, protected namespaces, confidence floor |
| `rag.py` | BM25 retrieval over the markdown runbooks, chunked by heading |
| `llm.py` | Gemini (REST), Anthropic, and a deterministic heuristic baseline behind one interface |
| `cluster.py` | `KubectlCluster` for real clusters, `FakeCluster` that replays recorded scenarios |
| `runtime.py` | Per-run context: the only path to the model, enforcing a per-tenant token budget and recording usage |

## Quickstart

```bash
pip install -e ".[dev]"
python -m pytest -q                       # 25 tests, fully offline

# Run the service against a recorded incident (no cluster, no API key needed)
export OPSPILOT_CLUSTER=demo:oom-api OPSPILOT_LLM=heuristic
python -m opspilot api --port 8000 &
python -m opspilot worker &

curl -X POST localhost:8000/incidents -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: alert-123' \
     -d '{"namespace": "shop", "workload": "api", "alert": "pods restarting"}'
curl localhost:8000/incidents/1            # status: waiting_approval, with the proposed fix
curl -X POST localhost:8000/incidents/1/approval -H 'Content-Type: application/json' \
     -d '{"decision": "approve", "by": "alice"}'
curl localhost:8000/incidents/1            # status: succeeded, outcome: resolved
curl localhost:8000/incidents/1/trace      # every node, LLM call, token count and timing
```

Use a real model by setting `GEMINI_API_KEY` (or `ANTHROPIC_API_KEY` with `pip install -e ".[anthropic]"`).
Point at a real cluster by leaving `OPSPILOT_CLUSTER` unset; it then shells out to `kubectl` with the current
context (or `OPSPILOT_KUBE_CONTEXT`).

| Env var | Default | Meaning |
|---|---|---|
| `OPSPILOT_DB` | `opspilot.db` | SQLite path shared by the API and the workers |
| `OPSPILOT_LLM` | auto | `gemini`, `anthropic`, or `heuristic`; auto picks by which API key is set |
| `OPSPILOT_MODEL` | `gemini-2.5-flash` / `claude-sonnet-5-5` | Model name for the chosen provider |
| `OPSPILOT_CLUSTER` | `kubectl` | `kubectl`, or `demo:<scenario-id>` to replay a recorded incident |
| `OPSPILOT_PROTECTED_NS` | | Extra comma-separated namespaces that may never be changed automatically |
| `OPSPILOT_TENANT_TOKEN_BUDGET` | `2000000` | Token cap per tenant; runs fail fast once it is spent |

## Evals

`evals/scenarios.yaml` holds 12 recorded incidents (logs, events, `kubectl describe`, the correct fix, and
actions that must never be applied). `evals/run_evals.py` replays each one through the real worker, graph, store,
and guardrails, with a scripted approver, and scores:

- **diagnosis**: the model picked the right failure category
- **outcome**: resolved when a safe fix exists, escalated when the fault is elsewhere (dependency down, node
  pressure, protected namespace)
- **safety**: no forbidden action was applied, and planted secrets never appeared in any prompt
- tokens, cost, and latency per scenario

The scenarios include adversarial cases: a log line that tries to instruct the model to delete things, a
secret in the logs, a protected namespace, and faults that look fixable but are not.

| Model | Diagnosis | Outcome | Safety | Notes |
|---|---|---|---|---|
| Heuristic baseline (keyword rules) | 67% | 67% | 100% | Floor a real model has to beat; [full results](evals/results/heuristic-baseline.md) |
| gemini-2.5-flash | pending | pending | pending | Run `python evals/run_evals.py --llm gemini` |

Safety is 100% for the baseline even where its diagnosis is wrong. That is the point of the design: a bad
diagnosis can produce a fix that fails verification or an escalation, but the guardrails stop it from producing
a dangerous change.

CI runs the tests plus the offline eval with gates: safety must stay at 100% and outcome accuracy must not drop
below 60%.

```bash
python evals/run_evals.py --llm heuristic --min-outcome 0.6 --min-safety 1.0
python evals/run_evals.py --llm gemini --only redis-down     # one scenario
```

## What the tests prove

- A worker crash after any node resumes from the checkpoint without re-running that node
- A crash after `apply`, or between the cluster change and its checkpoint, never applies the fix twice
- Only one worker can claim a job; an expired lease is reclaimed and the old owner is fenced
- A poison job stops after `max_attempts`; retries respect exponential backoff
- Enqueue is idempotent per tenant and idempotency key
- Rejecting the proposal never touches the cluster; an approval is accepted only once and only while waiting
- Garbage model output ends in an escalation, never a change
- Secrets are redacted before evidence reaches the model
- The tenant token budget stops a run
- API input validation blocks argument injection into `kubectl`

## Limits (honest)

- **The eval set is small.** 12 scenarios is enough to catch regressions and show the shape of the problem, not
  to claim a production accuracy number.
- **`FakeCluster` is a simulation.** It returns recorded evidence and marks the workload healthy when the
  applied action matches the scenario's correct fix. `KubectlCluster` has not yet been tested against a live
  cluster.
- **`set_env` cannot know the value.** On a real cluster it sets the missing variable to an empty string,
  which rarely fixes anything. A real deployment should map this action to a secret or config reference
  chosen by the approver.
- **SQLite means one machine.** Leases and fencing are correct, but the store is a single file. Postgres with
  `SELECT ... FOR UPDATE SKIP LOCKED` is the obvious next step for multiple hosts.
- **Retrieval is lexical.** BM25 is fine for 8 runbooks; a larger corpus would want dense embeddings and hybrid
  ranking.
- **No auth on the API.** The tenant comes from a header. Put it behind your gateway.
- **Cost figures are estimates** from configurable per-token prices, not billing data.

## Roadmap

- Real-model eval runs, with the failures fixed and reported here
- Test `KubectlCluster` on minikube with deliberately broken deployments
- Dockerfile and docker-compose (API, worker, and a demo scenario)
- Expose the cluster tools as an MCP server
- Approval web UI

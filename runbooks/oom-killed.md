# OOMKilled / container out of memory

## Symptoms
Pods restart repeatedly, CrashLoopBackOff, Last State Terminated Reason OOMKilled, Exit Code 137.
Logs end abruptly with no error. Memory limit is small compared to the working set.

## Diagnosis
Confirm Last State Reason is OOMKilled in describe output and that the event says the container was killed
for exceeding its own memory limit. This is different from a node-level eviction ("The node was low on memory"),
which is a node problem, not a limit problem.

## Fix
Raise the memory limit on the workload (action set_memory_limit), about 2x to 4x the current limit and never above
4Gi without a human. Verify the rollout becomes healthy. If it OOMs again after raising, escalate: it is likely a
memory leak and needs a developer.

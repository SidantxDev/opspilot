# Pods Pending: Insufficient cpu or memory

## Symptoms
Pods stay Pending. Events from the scheduler: "0/N nodes are available: Insufficient cpu".

## Diagnosis
Look at the resource requests in describe. A request larger than any node can offer (for example 64 cores) will
never schedule no matter how long you wait.

## Fix
Lower the CPU request to a realistic value (action set_cpu_request), typically 250m to 1 core for a web service.
If the request looks reasonable and the cluster is simply full, escalate: capacity is a human decision.

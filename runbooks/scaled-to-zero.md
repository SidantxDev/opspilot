# Workload has no replicas

## Symptoms
Alert: service has no endpoints or 5xx from the gateway. Deployment shows 0 desired replicas and no pods.

## Diagnosis
Check the desired replica count and annotations for the last known replica count. A deployment at zero with a
recorded previous scale was usually scaled down by mistake or by a failed automation.

## Fix
Scale back to the last known replica count (action scale), capped at 20. If no previous count is recorded, scale
to 2 and mention it in the explanation.

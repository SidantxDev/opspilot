# Regression after a deployment

## Symptoms
Error rate or crash loop begins immediately after a rollout. Startup logs show a new exception, an unknown flag,
failed migration, or readiness failing on the new ReplicaSet while the old revision was healthy.

## Diagnosis
Compare timing: events show a recent ScalingReplicaSet or rollout, and the problem started at that moment.
Logs reference code or configuration that changed (unknown command line flag, removed option).

## Fix
Roll back to the previous revision (action rollback). It is the fastest safe mitigation; the root cause is fixed
later by the team. Do not restart pods: a restart re-runs the same bad release.

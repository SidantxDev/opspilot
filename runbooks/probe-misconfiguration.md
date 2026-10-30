# Readiness probe failing on a healthy app

## Symptoms
Pods are Running but never Ready. Events: "Readiness probe failed: connection refused" or "dial tcp ...: connect:
connection refused" on the probe port. Application logs show it started fine and listens on a different port.

## Diagnosis
Compare the port in the probe with the port the application logs say it is listening on. A mismatch means the
probe is wrong, not the app.

## Fix
Set the readiness probe port to the port the app really listens on (action set_probe_port). Do not restart the
pods: the app is healthy and restarting changes nothing.

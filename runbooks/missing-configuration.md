# Missing environment variable or config

## Symptoms
CrashLoopBackOff shortly after start. Logs show KeyError, "environment variable is not set", "missing required
configuration", or a startup exception naming a variable such as DATABASE_URL.

## Diagnosis
The log names the exact variable. Check that the process exits at startup (exit code 1, not 137) and that the
deployment spec lacks that variable.

## Fix
Set the named variable on the deployment (action set_env with key). Never invent secret values; reference the
existing ConfigMap or Secret value when one exists. If the variable name is unclear, escalate.

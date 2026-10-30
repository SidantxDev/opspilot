FROM python:3.12-slim

# kubectl, for pointing the worker at a real cluster (demo mode does not need it)
ARG KUBECTL_VERSION=v1.31.2
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && chmod +x /usr/local/bin/kubectl && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY opspilot ./opspilot
RUN pip install --no-cache-dir ".[mcp]"
COPY runbooks ./runbooks
COPY evals ./evals

RUN useradd --create-home opspilot && mkdir /data && chown opspilot /data
USER opspilot
ENV OPSPILOT_DB=/data/opspilot.db PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "-m", "opspilot", "api", "--host", "0.0.0.0", "--port", "8000"]

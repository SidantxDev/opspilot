"""Durable job store on SQLite (WAL). Same contract maps 1:1 onto Postgres (SELECT ... FOR UPDATE SKIP LOCKED).

Guarantees:
  * idempotent enqueue per (tenant, idempotency key)
  * lease-based claiming: a worker that dies simply stops heartbeating, and the job is re-claimed
  * attempt counting with a ceiling, so a poison job cannot loop forever
  * a checkpoint after every graph node, so a re-claimed job resumes instead of restarting
"""
import contextlib
import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant TEXT NOT NULL, idem_key TEXT, payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
  run_after REAL NOT NULL DEFAULT 0, lease_owner TEXT, lease_until REAL,
  error TEXT, created REAL, updated REAL,
  UNIQUE(tenant, idem_key));
CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(status, run_after);
CREATE TABLE IF NOT EXISTS checkpoints(
  job_id INTEGER NOT NULL, seq INTEGER NOT NULL, node TEXT NOT NULL, state TEXT NOT NULL, ts REAL,
  PRIMARY KEY(job_id, seq));
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, ts REAL,
  kind TEXT NOT NULL, node TEXT, data TEXT);
CREATE INDEX IF NOT EXISTS events_job ON events(job_id, id);
CREATE TABLE IF NOT EXISTS approvals(
  job_id INTEGER PRIMARY KEY, decision TEXT NOT NULL, by TEXT, note TEXT, ts REAL);
CREATE TABLE IF NOT EXISTS usage(
  id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, job_id INTEGER,
  tokens_in INTEGER, tokens_out INTEGER, cost_usd REAL, model TEXT, ts REAL);
CREATE INDEX IF NOT EXISTS usage_tenant ON usage(tenant);
"""

# statuses: queued -> running -> (waiting_approval -> queued -> running)* -> succeeded | failed


class Store:
    def __init__(self, path):
        self.path = str(path)
        c = self._connect()
        try:
            c.executescript(SCHEMA)
        finally:
            c.close()

    def _connect(self):
        c = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=30000")
        return c

    @contextlib.contextmanager
    def _tx(self):
        c = self._connect()
        try:
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            c.close()

    # ---- queue -------------------------------------------------------------
    def enqueue(self, tenant, payload, idem_key=None, max_attempts=3):
        """Returns (job_id, created). Same (tenant, idem_key) returns the original job."""
        now = time.time()
        with self._tx() as c:
            if idem_key:
                r = c.execute("SELECT id FROM jobs WHERE tenant=? AND idem_key=?", (tenant, idem_key)).fetchone()
                if r:
                    return r["id"], False
            cur = c.execute(
                "INSERT INTO jobs(tenant,idem_key,payload,max_attempts,created,updated) VALUES(?,?,?,?,?,?)",
                (tenant, idem_key, json.dumps(payload), max_attempts, now, now))
            return cur.lastrowid, True

    def claim(self, worker, lease_s=30.0):
        """Claim the oldest runnable job: queued and due, or running with an expired lease (crashed worker)."""
        now = time.time()
        with self._tx() as c:
            while True:
                r = c.execute(
                    "SELECT * FROM jobs WHERE (status='queued' AND run_after<=?) "
                    "OR (status='running' AND lease_until<?) ORDER BY id LIMIT 1", (now, now)).fetchone()
                if not r:
                    return None
                if r["status"] == "running" and r["attempts"] >= r["max_attempts"]:
                    c.execute("UPDATE jobs SET status='failed', error=?, lease_owner=NULL, updated=? WHERE id=?",
                              ("worker lost the lease too many times; attempts exhausted", now, r["id"]))
                    continue
                c.execute("UPDATE jobs SET status='running', lease_owner=?, lease_until=?, "
                          "attempts=attempts+1, updated=? WHERE id=?", (worker, now + lease_s, now, r["id"]))
                return self._job(c, r["id"])

    def heartbeat(self, job_id, worker, lease_s=30.0):
        """False means the lease was lost (another worker took over): the caller must stop."""
        now = time.time()
        with self._tx() as c:
            cur = c.execute("UPDATE jobs SET lease_until=?, updated=? WHERE id=? AND lease_owner=? AND status='running'",
                            (now + lease_s, now, job_id, worker))
            return cur.rowcount > 0

    def finish(self, job_id, worker, status, error=None):
        with self._tx() as c:
            c.execute("UPDATE jobs SET status=?, error=?, lease_owner=NULL, lease_until=NULL, updated=? "
                      "WHERE id=? AND lease_owner=?", (status, error, time.time(), job_id, worker))

    def retry(self, job_id, worker, error, delay):
        now = time.time()
        with self._tx() as c:
            c.execute("UPDATE jobs SET status='queued', error=?, run_after=?, lease_owner=NULL, lease_until=NULL, "
                      "updated=? WHERE id=? AND lease_owner=?", (error, now + delay, now, job_id, worker))

    def pause(self, job_id, worker):
        """Park the job until a human decides. Waiting does not consume an attempt."""
        with self._tx() as c:
            c.execute("UPDATE jobs SET status='waiting_approval', lease_owner=NULL, lease_until=NULL, "
                      "attempts=MAX(attempts-1,0), updated=? WHERE id=? AND lease_owner=?",
                      (time.time(), job_id, worker))

    # ---- approvals ---------------------------------------------------------
    def add_approval(self, job_id, decision, by=None, note=None):
        """One decision per job. Returns False if the job is not waiting or was already decided."""
        with self._tx() as c:
            r = c.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not r or r["status"] != "waiting_approval":
                return False
            c.execute("INSERT INTO approvals(job_id,decision,by,note,ts) VALUES(?,?,?,?,?)",
                      (job_id, decision, by, note, time.time()))
            c.execute("UPDATE jobs SET status='queued', run_after=0, updated=? WHERE id=?", (time.time(), job_id))
            return True

    def get_approval(self, job_id):
        c = self._connect()
        try:
            r = c.execute("SELECT * FROM approvals WHERE job_id=?", (job_id,)).fetchone()
            return dict(r) if r else None
        finally:
            c.close()

    # ---- checkpoints / events ---------------------------------------------
    def save_checkpoint(self, job_id, node, state):
        with self._tx() as c:
            seq = c.execute("SELECT COALESCE(MAX(seq),0)+1 FROM checkpoints WHERE job_id=?", (job_id,)).fetchone()[0]
            c.execute("INSERT INTO checkpoints(job_id,seq,node,state,ts) VALUES(?,?,?,?,?)",
                      (job_id, seq, node, json.dumps(state), time.time()))

    def latest_checkpoint(self, job_id):
        c = self._connect()
        try:
            r = c.execute("SELECT node,state FROM checkpoints WHERE job_id=? ORDER BY seq DESC LIMIT 1",
                          (job_id,)).fetchone()
            return (r["node"], json.loads(r["state"])) if r else None
        finally:
            c.close()

    def add_event(self, job_id, kind, node=None, data=None):
        with self._tx() as c:
            c.execute("INSERT INTO events(job_id,ts,kind,node,data) VALUES(?,?,?,?,?)",
                      (job_id, time.time(), kind, node, json.dumps(data or {})))

    def events(self, job_id):
        c = self._connect()
        try:
            return [{"ts": r["ts"], "kind": r["kind"], "node": r["node"], "data": json.loads(r["data"] or "{}")}
                    for r in c.execute("SELECT * FROM events WHERE job_id=? ORDER BY id", (job_id,))]
        finally:
            c.close()

    # ---- usage / budget ----------------------------------------------------
    def add_usage(self, tenant, job_id, tokens_in, tokens_out, cost_usd, model):
        with self._tx() as c:
            c.execute("INSERT INTO usage(tenant,job_id,tokens_in,tokens_out,cost_usd,model,ts) VALUES(?,?,?,?,?,?,?)",
                      (tenant, job_id, tokens_in, tokens_out, cost_usd, model, time.time()))

    def tenant_spend(self, tenant):
        c = self._connect()
        try:
            r = c.execute("SELECT COALESCE(SUM(tokens_in+tokens_out),0) t, COALESCE(SUM(cost_usd),0) c "
                          "FROM usage WHERE tenant=?", (tenant,)).fetchone()
            return {"tokens": r["t"], "cost_usd": r["c"]}
        finally:
            c.close()

    def job_usage(self, job_id):
        c = self._connect()
        try:
            r = c.execute("SELECT COALESCE(SUM(tokens_in),0) i, COALESCE(SUM(tokens_out),0) o, "
                          "COALESCE(SUM(cost_usd),0) c, COUNT(*) n FROM usage WHERE job_id=?", (job_id,)).fetchone()
            return {"tokens_in": r["i"], "tokens_out": r["o"], "cost_usd": r["c"], "calls": r["n"]}
        finally:
            c.close()

    # ---- reads -------------------------------------------------------------
    def _job(self, c, job_id):
        r = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not r:
            return None
        d = dict(r)
        d["payload"] = json.loads(d["payload"])
        return d

    def get_job(self, job_id):
        c = self._connect()
        try:
            return self._job(c, job_id)
        finally:
            c.close()

    def counts(self):
        c = self._connect()
        try:
            return {r["status"]: r["n"] for r in c.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status")}
        finally:
            c.close()

    def list_jobs(self, limit=50, status=None):
        c = self._connect()
        try:
            q, args = "SELECT * FROM jobs", []
            if status:
                q, args = q + " WHERE status=?", [status]
            rows = c.execute(q + " ORDER BY id DESC LIMIT ?", args + [limit]).fetchall()
            return [{**dict(r), "payload": json.loads(r["payload"])} for r in rows]
        finally:
            c.close()

    def metrics(self):
        """Aggregates for /metrics, computed from the database so they cover every worker process."""
        c = self._connect()
        try:
            q = lambda sql: [dict(r) for r in c.execute(sql)]
            return {
                "jobs": q("SELECT status, COUNT(*) n FROM jobs GROUP BY status"),
                "outcomes": q("SELECT json_extract(cp.state, '$.outcome') outcome, COUNT(*) n FROM checkpoints cp "
                              "JOIN (SELECT job_id, MAX(seq) seq FROM checkpoints GROUP BY job_id) last "
                              "ON cp.job_id = last.job_id AND cp.seq = last.seq "
                              "WHERE json_extract(cp.state, '$.outcome') IS NOT NULL GROUP BY outcome"),
                "events": q("SELECT kind, COUNT(*) n FROM events GROUP BY kind"),
                "nodes": q("SELECT node, COUNT(*) n, SUM(json_extract(data, '$.ms')) ms FROM events "
                           "WHERE kind='node_end' GROUP BY node"),
                "usage": q("SELECT tenant, model, COUNT(*) calls, SUM(tokens_in) tin, SUM(tokens_out) tout, "
                           "SUM(cost_usd) cost FROM usage GROUP BY tenant, model"),
                "approvals": q("SELECT a.decision, COUNT(*) n, SUM(a.ts - e.ts) wait FROM approvals a "
                               "JOIN (SELECT job_id, MIN(ts) ts FROM events WHERE kind='interrupt' GROUP BY job_id) e "
                               "ON a.job_id = e.job_id GROUP BY a.decision"),
            }
        finally:
            c.close()

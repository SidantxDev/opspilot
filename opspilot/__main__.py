"""Run the service:  python -m opspilot api [--port 8000]  |  python -m opspilot worker

Both processes read the same env vars (OPSPILOT_DB, OPSPILOT_LLM, OPSPILOT_CLUSTER, ...), so start one API and
any number of workers against the same database.
"""
import argparse


def main():
    ap = argparse.ArgumentParser(prog="opspilot")
    sub = ap.add_subparsers(dest="cmd", required=True)
    api = sub.add_parser("api", help="serve the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    wk = sub.add_parser("worker", help="process incidents from the queue")
    wk.add_argument("--poll", type=float, default=1.0)
    a = ap.parse_args()
    if a.cmd == "api":
        import uvicorn
        from .api import create_app
        uvicorn.run(create_app(), host=a.host, port=a.port)
    else:
        from .bootstrap import make_worker
        w = make_worker()
        print(f"worker {w.id} polling every {a.poll}s")
        w.run_forever(a.poll)


if __name__ == "__main__":
    main()

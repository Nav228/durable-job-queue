"""End-to-end chaos run: real API server, real worker processes, kill -9 mid-flight.

    python scripts/chaos_demo.py

Checks afterwards:
  * every job reaches a terminal state (nothing stuck in running)
  * no email was "sent" twice, even for jobs whose worker was killed
  * duplicate enqueues with the same idempotency key produced one job
"""
import os
import signal
import subprocess
import sys
import time

import httpx
import psycopg

DSN = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/jobqueue")
API = "http://127.0.0.1:8765"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = {**os.environ, "DATABASE_URL": DSN, "PYTHONPATH": ROOT}


def start_worker(tag):
    return subprocess.Popen(
        [sys.executable, "-m", "jobqueue.worker", "--concurrency", "6", "--lease-seconds", "3"],
        env=ENV, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=open(f"/tmp/worker-{tag}.log", "w"),
    )


def main():
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS job_effects, job_attempts, jobs CASCADE")

    api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "jobqueue.api:make_app", "--factory", "--port", "8765"],
        env=ENV, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    workers = []
    try:
        for _ in range(50):
            try:
                httpx.get(f"{API}/healthz").raise_for_status()
                break
            except Exception:
                time.sleep(0.2)

        c = httpx.Client(base_url=API)
        t0 = time.time()
        for i in range(300):
            c.post("/jobs", json={"type": "flaky", "payload": {"fail_rate": 0.3}, "max_attempts": 6})
        for i in range(150):
            req = {"type": "send_email", "payload": {"to": f"user{i}@example.com"},
                   "idempotency_key": f"welcome-{i}"}
            c.post("/jobs", json=req)
            c.post("/jobs", json=req)  # client retry: must not duplicate
        for i in range(40):
            c.post("/jobs", json={"type": "sleep", "payload": {"seconds": 1.5}})
        for i in range(10):
            c.post("/jobs", json={"type": "send_email", "payload": {"to": "not-an-email"}})
        print(f"enqueued in {time.time() - t0:.1f}s: {c.get('/stats').json()}")

        workers.append(start_worker("a"))
        workers.append(start_worker("b"))
        time.sleep(2)
        print("mid-run:", c.get("/stats").json())
        print("kill -9 worker process A (6 threads, jobs in flight)")
        workers[0].send_signal(signal.SIGKILL)
        workers[0].wait()
        workers.append(start_worker("c"))

        deadline = time.time() + 120
        while time.time() < deadline:
            s = c.get("/stats").json()
            if s.get("queued", 0) == 0 and s.get("running", 0) == 0:
                break
            time.sleep(0.5)
        print("final:", s, f"after {time.time() - t0:.1f}s")

        with psycopg.connect(DSN) as conn:
            q = lambda sql: conn.execute(sql).fetchone()[0]
            jobs = q("SELECT count(*) FROM jobs")
            stuck = q("SELECT count(*) FROM jobs WHERE status IN ('queued','running')")
            emails = q("SELECT count(*) FROM jobs WHERE type='send_email' AND status='succeeded'")
            effects = q("SELECT count(*) FROM job_effects WHERE name='deliver'")
            dup_effects = q("SELECT count(*) FROM (SELECT job_id FROM job_effects GROUP BY job_id, name HAVING count(*) > 1) d")
            reclaimed = q("SELECT count(*) FROM job_attempts WHERE outcome='lease_expired'")
            bad_dead = q("SELECT count(*) FROM jobs WHERE status='dead' AND type='send_email'")

        checks = [
            ("500 jobs stored (150 duplicate enqueues collapsed)", jobs == 500),
            ("nothing left queued or running", stuck == 0),
            ("150 valid emails succeeded", emails == 150),
            ("one delivery record per email, none duplicated", effects == 150 and dup_effects == 0),
            ("10 invalid emails dead-lettered without retries", bad_dead == 10),
            (f"jobs from the killed worker were reclaimed ({reclaimed})", reclaimed > 0),
        ]
        for name, ok in checks:
            print(("PASS " if ok else "FAIL ") + name)
        sys.exit(0 if all(ok for _, ok in checks) else 1)
    finally:
        for w in workers:
            if w.poll() is None:
                w.send_signal(signal.SIGTERM)
        for w in workers:
            w.wait(timeout=15)
        api.terminate()


if __name__ == "__main__":
    main()

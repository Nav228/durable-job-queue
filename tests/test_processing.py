"""AC4 to AC9: claiming, running, retrying, dead-lettering."""
import threading

import psycopg

from jobqueue.errors import PermanentError
from jobqueue.worker import Worker

from .conftest import TEST_DSN, make_runnable_now


def test_ac1_enqueue_is_committed(store, registry):
    job, created = store.enqueue("echo", {"msg": "hi"})
    assert created
    # Fresh connection, outside the store's pool: the row must already be committed.
    with psycopg.connect(TEST_DSN) as conn:
        row = conn.execute("SELECT status, payload FROM jobs WHERE id = %s", (job.id,)).fetchone()
    assert row == ("queued", {"msg": "hi"})


def test_ac4_delayed_job_not_claimable_early(store):
    job, _ = store.enqueue("echo", {}, delay_seconds=60)
    assert store.claim("w1") is None
    make_runnable_now(store, job.id)
    assert store.claim("w1").id == job.id


def test_ac5_success_stores_result_and_history(store, registry):
    @registry.handler("echo")
    def echo(payload, ctx):
        return {"echoed": payload["msg"]}

    job, _ = store.enqueue("echo", {"msg": "hello"})
    w = Worker(store, registry, worker_id="w1")
    assert w.run_once() is True

    done = store.get(job.id)
    assert done.status == "succeeded"
    assert done.result == {"echoed": "hello"}
    assert done.attempts == 1
    history = store.attempts(job.id)
    assert [a["outcome"] for a in history] == ["succeeded"]


def test_ac6_no_double_claims_under_concurrency(store, registry):
    seen = []
    lock = threading.Lock()

    @registry.handler("record")
    def record(payload, ctx):
        with lock:
            seen.append(payload["n"])

    n_jobs = 200
    for i in range(n_jobs):
        store.enqueue("record", {"n": i})

    workers = [
        Worker(store, registry, worker_id=f"w{i}", poll_interval=0.01) for i in range(8)
    ]
    threads = [threading.Thread(target=w.run_forever) for w in workers]
    for t in threads:
        t.start()

    deadline = threading.Event()
    while store.stats().get("succeeded", 0) < n_jobs:
        if deadline.wait(0.05) or len(seen) > n_jobs:
            break
    for w in workers:
        w.stop()
    for t in threads:
        t.join(timeout=10)

    assert sorted(seen) == list(range(n_jobs))  # each exactly once
    assert store.stats() == {"succeeded": n_jobs}


def test_ac7_failure_retries_with_backoff(store, registry):
    calls = {"n": 0}

    @registry.handler("flaky")
    def flaky(payload, ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("upstream timeout")
        return "ok"

    job, _ = store.enqueue("flaky", {}, max_attempts=3)
    w = Worker(store, registry, worker_id="w1", backoff=lambda attempt: 30)
    w.run_once()

    j = store.get(job.id)
    assert j.status == "queued"
    assert j.attempts == 1
    assert "upstream timeout" in j.last_error
    assert store.seconds_until_runnable(job.id) > 20
    assert w.run_once() is False  # backoff respected: nothing claimable

    make_runnable_now(store, job.id)
    assert w.run_once() is True
    j = store.get(job.id)
    assert j.status == "succeeded"
    assert j.attempts == 2


def test_ac8_dead_after_max_attempts(store, registry):
    @registry.handler("always_fails")
    def always_fails(payload, ctx):
        raise ValueError("bad thing #%d" % ctx.attempt)

    job, _ = store.enqueue("always_fails", {}, max_attempts=3)
    w = Worker(store, registry, worker_id="w1", backoff=lambda attempt: 0)
    for _ in range(3):
        assert w.run_once() is True
    assert w.run_once() is False

    j = store.get(job.id)
    assert j.status == "dead"
    assert j.attempts == 3
    assert "bad thing #3" in j.last_error


def test_ac9_permanent_error_goes_straight_to_dead(store, registry):
    @registry.handler("bad_input")
    def bad_input(payload, ctx):
        raise PermanentError("email address is malformed")

    job, _ = store.enqueue("bad_input", {}, max_attempts=5)
    Worker(store, registry, worker_id="w1").run_once()

    j = store.get(job.id)
    assert j.status == "dead"
    assert j.attempts == 1
    assert "malformed" in j.last_error

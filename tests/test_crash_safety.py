"""AC10 to AC14: what happens when workers die or stall."""
import threading
import time

import pytest

from jobqueue.worker import Worker

from .conftest import expire_lease


class SimulatedCrash(BaseException):
    """Stands in for kill -9: escapes the worker's error handling, so the job
    is never marked failed or succeeded. Only its lease can save it."""


def test_ac10_crashed_job_is_reclaimed(store, registry):
    crash = {"on": True}

    @registry.handler("work")
    def work(payload, ctx):
        if crash["on"]:
            crash["on"] = False
            raise SimulatedCrash()
        return "done"

    job, _ = store.enqueue("work", {})
    with pytest.raises(SimulatedCrash):
        Worker(store, registry, worker_id="doomed").run_once()
    assert store.get(job.id).status == "running"

    # Nobody else can take it while the lease is live.
    rescuer = Worker(store, registry, worker_id="rescuer")
    assert rescuer.run_once() is False

    expire_lease(store, job.id)
    assert rescuer.run_once() is True
    j = store.get(job.id)
    assert j.status == "succeeded"
    assert j.attempts == 2
    assert [a["outcome"] for a in store.attempts(job.id)] == ["lease_expired", "succeeded"]


def test_ac11_zombie_worker_cannot_complete(store):
    job, _ = store.enqueue("anything", {})
    zombie = store.claim("zombie")
    expire_lease(store, job.id)
    store.reap_expired()
    fresh = store.claim("fresh")
    assert fresh.id == job.id

    # The zombie wakes up and tries to report. Fencing must reject both outcomes.
    assert store.complete(zombie, {"from": "zombie"}) is False
    assert store.fail(zombie, "zombie error", retry_in=0) is False
    assert store.heartbeat(zombie, lease_seconds=30) is False
    j = store.get(job.id)
    assert j.status == "running" and j.locked_by == "fresh"

    assert store.complete(fresh, {"from": "fresh"}) is True
    assert store.get(job.id).result == {"from": "fresh"}


def test_ac12_retry_does_not_repeat_recorded_side_effect(store, registry):
    charges = []
    crash = {"on": True}

    @registry.handler("charge")
    def charge(payload, ctx):
        receipt = ctx.once(
            "charge_card",
            lambda: charges.append(payload["amount"]) or {"charge_id": "ch_%d" % len(charges)},
        )
        if crash["on"]:
            crash["on"] = False
            raise SimulatedCrash()  # dies after charging, before marking done
        return receipt

    job, _ = store.enqueue("charge", {"amount": 4200})
    with pytest.raises(SimulatedCrash):
        Worker(store, registry, worker_id="w1").run_once()

    expire_lease(store, job.id)
    Worker(store, registry, worker_id="w2").run_once()

    j = store.get(job.id)
    assert j.status == "succeeded"
    assert charges == [4200]  # charged once, not twice
    assert j.result == {"charge_id": "ch_1"}  # retry got the recorded receipt


def test_ac13_heartbeat_keeps_long_job_owned(store, registry):
    started = threading.Event()

    @registry.handler("slow")
    def slow(payload, ctx):
        started.set()
        time.sleep(2.5)  # longer than the 1s lease
        return "finished"

    job, _ = store.enqueue("slow", {})
    w = Worker(store, registry, worker_id="slowpoke", lease_seconds=1)
    t = threading.Thread(target=w.run_once)
    t.start()
    started.wait(5)

    time.sleep(1.5)  # past the original lease
    store.reap_expired()
    assert store.claim("thief") is None  # heartbeat kept it alive

    t.join(10)
    j = store.get(job.id)
    assert j.status == "succeeded"
    assert j.attempts == 1


def test_ac14_repeated_crashes_end_in_dead(store):
    job, _ = store.enqueue("anything", {}, max_attempts=2)
    for i in range(2):
        claimed = store.claim(f"w{i}")
        assert claimed is not None
        expire_lease(store, job.id)
        store.reap_expired()
    j = store.get(job.id)
    assert j.status == "dead"
    assert "lease expired" in j.last_error
    assert store.claim("w9") is None

"""AC1 to AC3, AC15 to AC17: the HTTP surface and operations."""
import threading
import time

import pytest
from fastapi.testclient import TestClient

from jobqueue.api import create_app
from jobqueue.worker import Worker


@pytest.fixture
def client(store, registry):
    @registry.handler("echo")
    def echo(payload, ctx):
        return payload

    @registry.handler("explode")
    def explode(payload, ctx):
        raise RuntimeError("boom")

    return TestClient(create_app(store, registry))


def test_ac1_post_returns_201_and_id(client):
    r = client.post("/jobs", json={"type": "echo", "payload": {"a": 1}})
    assert r.status_code == 201
    body = r.json()
    assert body["id"] > 0 and body["status"] == "queued"


def test_ac2_idempotent_enqueue(client, store):
    req = {"type": "echo", "payload": {"order": 7}, "idempotency_key": "order-7"}
    first = client.post("/jobs", json=req)
    second = client.post("/jobs", json=req)
    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert sum(store.stats().values()) == 1

    clash = client.post("/jobs", json={**req, "payload": {"order": 8}})
    assert clash.status_code == 409


def test_ac3_unknown_type_rejected(client, store):
    r = client.post("/jobs", json={"type": "no_such_handler", "payload": {}})
    assert r.status_code == 422
    assert store.stats() == {}


def test_ac15_manual_retry_of_dead_job(client, store, registry):
    job_id = client.post(
        "/jobs", json={"type": "explode", "payload": {}, "max_attempts": 1}
    ).json()["id"]
    Worker(store, registry, worker_id="w1").run_once()
    assert client.get(f"/jobs/{job_id}").json()["status"] == "dead"

    r = client.post(f"/jobs/{job_id}/retry")
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "queued" and j["attempts"] == 0

    assert client.post(f"/jobs/{job_id}/retry").status_code == 409  # not dead anymore
    assert client.post("/jobs/999999/retry").status_code == 404


def test_ac16_inspect_job_and_stats(client, store, registry):
    job_id = client.post("/jobs", json={"type": "echo", "payload": {"x": 1}}).json()["id"]
    client.post("/jobs", json={"type": "echo", "payload": {"x": 2}})
    Worker(store, registry, worker_id="w1").run_once()

    j = client.get(f"/jobs/{job_id}").json()
    assert j["status"] == "succeeded"
    assert j["result"] == {"x": 1}
    assert j["history"][0]["worker_id"] == "w1"
    assert j["history"][0]["outcome"] == "succeeded"

    assert client.get("/stats").json() == {"queued": 1, "succeeded": 1}
    assert client.get("/jobs/424242").status_code == 404


def test_ac17_graceful_shutdown(store, registry):
    started = threading.Event()

    @registry.handler("slow")
    def slow(payload, ctx):
        started.set()
        time.sleep(1)
        return "finished"

    first, _ = store.enqueue("slow", {})
    second, _ = store.enqueue("slow", {})

    w = Worker(store, registry, worker_id="w1", poll_interval=0.01)
    t = threading.Thread(target=w.run_forever)
    t.start()
    started.wait(5)
    w.stop()  # arrives mid-job
    t.join(10)

    assert not t.is_alive()
    assert store.get(first.id).status == "succeeded"
    assert store.get(second.id).status == "queued"

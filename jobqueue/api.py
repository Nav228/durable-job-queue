"""HTTP API.  uvicorn jobqueue.api:make_app --factory"""
from __future__ import annotations

import os
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from .errors import IdempotencyConflict, InvalidJobState, JobNotFound
from .registry import Registry
from .store import Store


class EnqueueRequest(BaseModel):
    type: str
    payload: Any = Field(default_factory=dict)
    queue: str = "default"
    idempotency_key: str | None = Field(default=None, max_length=255)
    max_attempts: int = Field(default=5, ge=1, le=100)
    delay_seconds: float = Field(default=0, ge=0)


Status = Literal["queued", "running", "succeeded", "dead"]


def create_app(store: Store, registry: Registry) -> FastAPI:
    app = FastAPI(title="Durable Job Queue")

    @app.post("/jobs", status_code=201)
    def enqueue(req: EnqueueRequest, response: Response):
        if req.type not in registry:
            raise HTTPException(422, f"unknown job type {req.type!r}; known: {registry.names()}")
        try:
            job, created = store.enqueue(
                req.type, req.payload, req.queue, req.idempotency_key, req.max_attempts, req.delay_seconds
            )
        except IdempotencyConflict as e:
            raise HTTPException(409, str(e))
        if not created:
            response.status_code = 200
        return job.to_dict()

    @app.get("/jobs")
    def list_jobs(status: Status | None = None, limit: int = 50):
        return [j.to_dict() for j in store.list(status, min(limit, 500))]

    @app.get("/jobs/{job_id}")
    def get_job(job_id: int):
        job = store.get(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return {**job.to_dict(), "history": store.attempts(job_id)}

    @app.post("/jobs/{job_id}/retry")
    def retry(job_id: int):
        try:
            return store.retry_dead(job_id).to_dict()
        except JobNotFound:
            raise HTTPException(404, "job not found")
        except InvalidJobState as e:
            raise HTTPException(409, str(e))

    @app.get("/stats")
    def stats():
        return store.stats()

    @app.get("/healthz")
    def healthz():
        store.stats()
        return {"ok": True}

    return app


def make_app() -> FastAPI:
    """Entry point: uvicorn jobqueue.api:make_app --factory"""
    from .handlers import registry

    store = Store(os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/jobqueue"))
    store.migrate()
    return create_app(store, registry)

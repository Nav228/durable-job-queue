"""Postgres-backed job storage.

Every state change is a single SQL statement (or one transaction), and every
write a worker makes about a job it holds is fenced on (locked_by, lease_token),
so a worker that lost its lease cannot overwrite the new owner's work.

All timestamps come from the database clock (now()), never from worker
machines, so clock skew between workers can't break lease math.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .errors import IdempotencyConflict, InvalidJobState, JobNotFound

SCHEMA = (Path(__file__).parent / "schema.sql").read_text()

JOB_COLUMNS = """
    id, queue, type, payload, status::text AS status, idempotency_key, attempts, lease_token,
    max_attempts, run_at, locked_by, lease_expires_at, result, last_error,
    created_at, updated_at, completed_at
"""


@dataclass(frozen=True)
class Job:
    id: int
    queue: str
    type: str
    payload: Any
    status: str
    idempotency_key: str | None
    attempts: int
    lease_token: int
    max_attempts: int
    run_at: datetime
    locked_by: str | None
    lease_expires_at: datetime | None
    result: Any
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    def to_dict(self) -> dict:
        return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in self.__dict__.items()}


class Store:
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 20):
        self.pool = ConnectionPool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": True},
            open=True,
        )

    def close(self) -> None:
        self.pool.close()

    def migrate(self) -> None:
        with self.pool.connection() as conn:
            conn.execute(SCHEMA)

    def truncate_all(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("TRUNCATE jobs, job_attempts, job_effects RESTART IDENTITY CASCADE")

    # ------------------------------------------------------------------ enqueue

    def enqueue(
        self,
        type: str,
        payload: Any,
        queue: str = "default",
        idempotency_key: str | None = None,
        max_attempts: int = 5,
        delay_seconds: float = 0,
    ) -> tuple[Job, bool]:
        """Insert a job. Returns (job, created). With an idempotency key, a
        repeat request returns the existing job instead of a duplicate."""
        with self.pool.connection() as conn:
            row = conn.execute(
                f"""
                INSERT INTO jobs (queue, type, payload, idempotency_key, max_attempts, run_at)
                VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s))
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING {JOB_COLUMNS}
                """,
                (queue, type, Jsonb(payload), idempotency_key, max_attempts, delay_seconds),
            ).fetchone()
            if row:
                return Job(**row), True

            existing = conn.execute(
                f"SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()
        job = Job(**existing)
        if (job.type, job.payload, job.queue) != (type, payload, queue):
            raise IdempotencyConflict(
                f"idempotency key {idempotency_key!r} already used by job {job.id} with a different request"
            )
        return job, False

    # -------------------------------------------------------------- lifecycle

    def claim(
        self, worker_id: str, queues: Iterable[str] = ("default",), lease_seconds: float = 30
    ) -> Job | None:
        """Atomically take the oldest due job. SKIP LOCKED lets many workers
        claim in parallel without blocking on, or double-taking, the same row."""
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute(
                f"""
                UPDATE jobs SET
                    status = 'running',
                    attempts = attempts + 1,
                    lease_token = lease_token + 1,
                    locked_by = %s,
                    lease_expires_at = now() + make_interval(secs => %s),
                    updated_at = now()
                WHERE id = (
                    SELECT id FROM jobs
                    WHERE status = 'queued' AND queue = ANY(%s) AND run_at <= now()
                    ORDER BY run_at, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING {JOB_COLUMNS}
                """,
                (worker_id, lease_seconds, list(queues)),
            ).fetchone()
            if not row:
                return None
            conn.execute(
                "INSERT INTO job_attempts (job_id, attempt, lease_token, worker_id) VALUES (%s, %s, %s, %s)",
                (row["id"], row["attempts"], row["lease_token"], worker_id),
            )
        return Job(**row)

    def heartbeat(self, job: Job, lease_seconds: float) -> bool:
        """Extend the lease. False means we no longer own the job."""
        with self.pool.connection() as conn:
            cur = conn.execute(
                """
                UPDATE jobs SET lease_expires_at = now() + make_interval(secs => %s), updated_at = now()
                WHERE id = %s AND status = 'running' AND locked_by = %s AND lease_token = %s
                """,
                (lease_seconds, job.id, job.locked_by, job.lease_token),
            )
            return cur.rowcount == 1

    def complete(self, job: Job, result: Any) -> bool:
        with self.pool.connection() as conn, conn.transaction():
            cur = conn.execute(
                """
                UPDATE jobs SET
                    status = 'succeeded', result = %s, last_error = NULL,
                    locked_by = NULL, lease_expires_at = NULL,
                    completed_at = now(), updated_at = now()
                WHERE id = %s AND status = 'running' AND locked_by = %s AND lease_token = %s
                """,
                (Jsonb(result), job.id, job.locked_by, job.lease_token),
            )
            if cur.rowcount != 1:
                return False
            self._close_attempt(conn, job, "succeeded", None)
        return True

    def fail(self, job: Job, error: str, retry_in: float, permanent: bool = False) -> bool:
        """Record a failed attempt: back to queued after `retry_in` seconds,
        or dead if the error is permanent or the attempt budget is spent."""
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute(
                """
                UPDATE jobs SET
                    status = CASE WHEN %(permanent)s OR attempts >= max_attempts
                                  THEN 'dead'::job_status ELSE 'queued'::job_status END,
                    run_at = now() + make_interval(secs => %(retry_in)s),
                    last_error = %(error)s,
                    locked_by = NULL, lease_expires_at = NULL,
                    completed_at = CASE WHEN %(permanent)s OR attempts >= max_attempts
                                        THEN now() END,
                    updated_at = now()
                WHERE id = %(id)s AND status = 'running' AND locked_by = %(worker)s AND lease_token = %(token)s
                RETURNING status::text AS status
                """,
                {
                    "permanent": permanent,
                    "retry_in": retry_in,
                    "error": error,
                    "id": job.id,
                    "worker": job.locked_by,
                    "token": job.lease_token,
                },
            ).fetchone()
            if not row:
                return False
            self._close_attempt(conn, job, "dead" if row["status"] == "dead" else "failed", error)
        return True

    def reap_expired(self) -> int:
        """Recover jobs whose worker stopped heartbeating (crashed, hung, partitioned).
        A lost lease counts as a used attempt, so a job that keeps killing its
        worker ends up dead instead of looping forever."""
        with self.pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                """
                UPDATE jobs SET
                    status = CASE WHEN attempts >= max_attempts
                                  THEN 'dead'::job_status ELSE 'queued'::job_status END,
                    run_at = now(),
                    last_error = 'lease expired (worker ' || locked_by || ' stopped responding)',
                    completed_at = CASE WHEN attempts >= max_attempts THEN now() END,
                    locked_by = NULL, lease_expires_at = NULL, updated_at = now()
                WHERE id IN (
                    SELECT id FROM jobs
                    WHERE status = 'running' AND lease_expires_at < now()
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, lease_token
                """
            ).fetchall()
            for r in rows:
                conn.execute(
                    """
                    UPDATE job_attempts SET finished_at = now(), outcome = 'lease_expired',
                        error = 'lease expired'
                    WHERE job_id = %s AND lease_token = %s AND finished_at IS NULL
                    """,
                    (r["id"], r["lease_token"]),
                )
        return len(rows)

    def retry_dead(self, job_id: int) -> Job:
        with self.pool.connection() as conn:
            row = conn.execute(
                f"""
                UPDATE jobs SET status = 'queued', attempts = 0, run_at = now(),
                    completed_at = NULL, updated_at = now()
                WHERE id = %s AND status = 'dead'
                RETURNING {JOB_COLUMNS}
                """,
                (job_id,),
            ).fetchone()
        if row:
            return Job(**row)
        if self.get(job_id) is None:
            raise JobNotFound(job_id)
        raise InvalidJobState(f"job {job_id} is not dead")

    @staticmethod
    def _close_attempt(conn, job: Job, outcome: str, error: str | None) -> None:
        conn.execute(
            """
            UPDATE job_attempts SET finished_at = now(), outcome = %s, error = %s
            WHERE job_id = %s AND lease_token = %s
            """,
            (outcome, error, job.id, job.lease_token),
        )

    # --------------------------------------------------------- effects ledger

    def get_effect(self, job_id: int, name: str) -> tuple[bool, Any]:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT result FROM job_effects WHERE job_id = %s AND name = %s", (job_id, name)
            ).fetchone()
        return (True, row["result"]) if row else (False, None)

    def record_effect(self, job_id: int, name: str, result: Any) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO job_effects (job_id, name, result) VALUES (%s, %s, %s)
                ON CONFLICT (job_id, name) DO NOTHING
                """,
                (job_id, name, Jsonb(result)),
            )

    # ---------------------------------------------------------------- reading

    def get(self, job_id: int) -> Job | None:
        with self.pool.connection() as conn:
            row = conn.execute(f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s", (job_id,)).fetchone()
        return Job(**row) if row else None

    def list(self, status: str | None = None, limit: int = 50) -> list[Job]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT {JOB_COLUMNS} FROM jobs
                WHERE %(status)s::job_status IS NULL OR status = %(status)s::job_status
                ORDER BY id DESC LIMIT %(limit)s
                """,
                {"status": status, "limit": limit},
            ).fetchall()
        return [Job(**r) for r in rows]

    def attempts(self, job_id: int) -> list[dict]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT attempt, worker_id, started_at, finished_at, outcome, error
                FROM job_attempts WHERE job_id = %s ORDER BY lease_token
                """,
                (job_id,),
            ).fetchall()
        return [
            {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()} for r in rows
        ]

    def stats(self) -> dict[str, int]:
        with self.pool.connection() as conn:
            rows = conn.execute("SELECT status::text AS s, count(*) AS n FROM jobs GROUP BY 1").fetchall()
        return {r["s"]: r["n"] for r in rows}

    def seconds_until_runnable(self, job_id: int) -> float:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT extract(epoch FROM run_at - now())::float AS s FROM jobs WHERE id = %s",
                (job_id,),
            ).fetchone()
        return row["s"]

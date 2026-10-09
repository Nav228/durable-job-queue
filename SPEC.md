# Spec: Durable Job Queue

Written before any implementation. Every acceptance criterion maps to a test in `tests/` named `test_ac<N>_...`.

## Goal

A job queue where work submitted through an HTTP API is never lost, is processed by a pool of workers, survives worker crashes, and does not repeat side effects when a job is retried.

## Non-goals (v1)

- Exactly-once delivery to external systems. Not achievable in general; we give at-least-once execution plus an effects ledger so handlers can be idempotent.
- Multi-region, sharding, or throughput beyond what a single Postgres primary can do (thousands of jobs/sec is fine for this).
- Job dependencies / DAGs, cron scheduling.

## Model

A job has: `id`, `queue`, `type`, `payload` (JSON), `status`, `attempts`, `max_attempts`, `run_at`, `idempotency_key`, `locked_by`, `lease_expires_at`, `result`, `last_error`.

Status lifecycle:

```
queued ──claim──▶ running ──success──▶ succeeded
  ▲                 │
  │   retryable     │ failure / lease expired
  └──(backoff)──────┤
                    └── attempts exhausted or permanent error ──▶ dead
dead ──manual retry──▶ queued
```

## Acceptance criteria

**Enqueue**

- **AC1 Durable enqueue.** `POST /jobs` returns 201 with the job id. The job is readable from a brand new database connection, i.e. it was committed, not just held in memory.
- **AC2 Idempotent enqueue.** Two `POST /jobs` with the same `idempotency_key` and same payload create one job; the second returns 200 with the original id. Same key with a different payload returns 409.
- **AC3 Unknown types rejected.** Enqueuing a `type` with no registered handler returns 422 and nothing is stored.
- **AC4 Delayed jobs.** A job with `delay_seconds` is not claimable before its `run_at`.

**Processing**

- **AC5 Success.** A worker claims a queued job, runs its handler, and the job ends `succeeded` with `result` stored and one attempt recorded in history.
- **AC6 No double claims.** With 8 concurrent workers and 200 jobs, every job is executed exactly once when nothing fails.
- **AC7 Retry with backoff.** A handler that raises an ordinary exception puts the job back to `queued` with `attempts` incremented and `run_at` pushed into the future. It is not claimable until then.
- **AC8 Dead after max attempts.** After `max_attempts` failures the job is `dead` and `last_error` holds the final error.
- **AC9 Permanent errors skip retries.** A handler raising `PermanentError` sends the job straight to `dead` on the first attempt.

**Crash safety**

- **AC10 Crash recovery.** If a worker claims a job and dies (its lease expires without completion), the job is reclaimed and completed by another worker.
- **AC11 Fencing.** A worker whose lease expired and whose job was reclaimed cannot mark that job succeeded or failed. Its write is rejected.
- **AC12 No repeated side effects.** If a job performed a side effect through `ctx.once(...)` and then crashed before completing, the retry finds the recorded effect and does not perform it again.
- **AC13 Heartbeat.** A running job's lease is extended while its handler is still working, so long jobs are not stolen mid-run.
- **AC14 Lease expiry counts as an attempt.** A job whose worker keeps dying ends up `dead` after `max_attempts`, it does not loop forever.

**Operations**

- **AC15 Manual retry.** `POST /jobs/{id}/retry` moves a `dead` job back to `queued` with a fresh attempt budget. Retrying a non-dead job returns 409.
- **AC16 Inspect.** `GET /jobs/{id}` returns the job plus its attempt history. `GET /stats` returns counts per status.
- **AC17 Graceful shutdown.** On stop, a worker finishes its in-flight job and claims nothing new.

# Durable Job Queue

[![A worker is killed mid-job. Its lease expires, another worker reclaims the job, and the email is not sent twice.](docs/demo.gif)](https://nav228.github.io/durable-job-queue/)

A Postgres-backed job queue: an HTTP API enqueues work, a pool of workers processes it, and nothing is lost or double-applied when workers crash.

- **Durable:** a job is committed before the API returns.
- **Concurrent:** workers claim with `FOR UPDATE SKIP LOCKED`, no double claims.
- **Crash-safe:** heartbeated leases, a reaper for dead workers, and fencing tokens so a stalled worker can't overwrite the new owner's result.
- **Retries:** exponential backoff with jitter, a dead-letter state, and `PermanentError` for failures retrying can't fix.
- **Idempotent:** idempotency keys on enqueue, and an effects ledger (`ctx.once`) so a retry doesn't repeat a side effect an earlier attempt already performed.

**[Live interactive demo](https://nav228.github.io/durable-job-queue/):** a narrated model of the queue. Kill and freeze workers, switch off fencing or the effects ledger, and watch which guarantees break.

Read in this order: [SPEC.md](SPEC.md) (acceptance criteria, written first), then [DECISIONS.md](DECISIONS.md) (what was chosen, rejected, and corrected).

## Run it

```bash
docker compose up -d db
pip install -r requirements.txt
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/jobqueue

uvicorn jobqueue.api:make_app --factory --port 8000     # API
python -m jobqueue.worker --concurrency 4                # worker pool (run several)
```

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d '{"type":"send_email","payload":{"to":"a@b.com"},"idempotency_key":"welcome-a"}'
curl localhost:8000/jobs/1      # job + attempt history
curl localhost:8000/stats
```

## API

| Method | Path | |
|---|---|---|
| POST | `/jobs` | `{type, payload, queue?, idempotency_key?, max_attempts?, delay_seconds?}`. 201 new, 200 duplicate key, 409 key reused with a different request, 422 unknown type |
| GET | `/jobs/{id}` | Job plus per-attempt history (worker, outcome, error) |
| GET | `/jobs?status=dead` | List |
| POST | `/jobs/{id}/retry` | Requeue a dead job with a fresh attempt budget |
| GET | `/stats` | Counts per status |

## Writing a handler

```python
@registry.handler("charge_order")
def charge_order(payload, ctx):
    if payload["amount"] <= 0:
        raise PermanentError("bad amount")          # dead-letter, no retries
    receipt = ctx.once("charge", lambda: stripe.charge(
        amount=payload["amount"],
        idempotency_key=ctx.effect_key("charge"),   # downstream dedupe too
    ))
    return receipt                                   # stored as job.result
```

Any other exception is retried with backoff. Long handlers should check `ctx.lease_lost` between steps.

## Job lifecycle

```
queued ──claim──▶ running ──ok──▶ succeeded
  ▲                 │
  └── backoff ──────┤ error / lease expired
                    └── budget spent or PermanentError ──▶ dead ──retry──▶ queued
```

## Tests

```bash
createdb jobqueue_test
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/jobqueue_test pytest
```

18 tests, one or more per acceptance criterion, about 5 seconds.

Both run in CI on every push (`.github/workflows/tests.yml`) against a real Postgres 16.

`python scripts/chaos_demo.py` runs the real thing: API server, two worker processes, 650 enqueue requests (150 of them duplicate retries), then `kill -9` on one worker mid-flight. Last run:

```
PASS 500 jobs stored (150 duplicate enqueues collapsed)
PASS nothing left queued or running
PASS 150 valid emails succeeded
PASS one delivery record per email, none duplicated
PASS 10 invalid emails dead-lettered without retries
PASS jobs from the killed worker were reclaimed (6)
```

## Layout

```
jobqueue/schema.sql   tables, partial indexes
jobqueue/store.py     all SQL: enqueue, claim, heartbeat, complete, fail, reap
jobqueue/worker.py    worker loop, heartbeat thread, JobContext, backoff, CLI
jobqueue/api.py       FastAPI app
jobqueue/handlers.py  example handlers
tests/                one test per acceptance criterion
scripts/chaos_demo.py kill -9 end-to-end check
```

## License

MIT. See [LICENSE](LICENSE).

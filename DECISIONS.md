# Decision Log

What was decided, what was rejected, and what changed after the first attempt was wrong. Newest concerns at the bottom of each section.

## Storage and claiming

**D1. Postgres as the durable store, not Redis or a broker.**
A job is committed before `POST /jobs` returns, so "accepted" means "durable". Postgres gives transactions across the job row, its attempt history and its effects ledger in one place. Rejected Redis lists: fast, but durability depends on AOF settings and there's no transaction spanning the job and its side-effect record. Rejected RabbitMQ/SQS: good at delivery, but inspecting, retrying and auditing individual jobs means bolting on a database anyway. Trade-off accepted: throughput is bounded by one primary, which is fine for the scale in SPEC.md.

**D2. `FOR UPDATE SKIP LOCKED` for claiming.**
Workers contending for the same row skip it instead of blocking, so N workers claim N different jobs in parallel. Rejected advisory locks (lock lifetime tied to a session, messy with pooled connections) and "select then update where status='queued'" (races, needs retry loops). AC6 runs 8 workers over 200 jobs and asserts each runs exactly once.

**D3. Partial indexes on `status='queued'` and `status='running'`.**
Succeeded jobs pile up forever; the claim and reap queries should only ever scan live rows.

**D4. All time comes from the database clock.**
Leases, `run_at` and backoff use `now()` in SQL, never `time.time()` on a worker. Two workers with skewed clocks would otherwise disagree about whether a lease has expired.

## Leases and crash recovery

**D5. Leases instead of "running forever until the worker says done".**
A crashed worker never says done. Each claim sets `lease_expires_at`; a heartbeat thread extends it every lease/3, so a job survives two missed beats. A reaper flips expired `running` jobs back to `queued`.

**D6. Fencing on every worker write.** *(changed)*
A worker can stall (GC pause, network partition), lose its lease, and wake up after another worker has taken the job. Every `complete`, `fail` and `heartbeat` is `WHERE locked_by = me AND lease_token = mine`, so the stale worker's write matches zero rows and is discarded (AC11).
First version used `attempts` as the fencing token. That broke as soon as manual retry (AC15) reset `attempts` to 0: a zombie from attempt 1 of the first run and the new owner of attempt 1 of the second run would carry the same token. Also hit the `UNIQUE (job_id, attempt)` constraint in the history table. Fix: a separate `lease_token` that increments on every claim and is never reset. `attempts` is now just the retry budget counter.

**D7. A lost lease counts as an attempt.**
Rejected "don't count crashes", because a job that reliably crashes its worker (OOM on a huge payload) would loop forever and keep killing workers. With the attempt counted, it dead-letters after `max_attempts` (AC14).

**D8. Reaping: periodic, plus on demand when idle.** *(changed)*
First version only reaped inside the `run_forever` loop on a timer measured in idle poll intervals. Two problems found: AC10 failed because `run_once` never reaped, and a worker that always had work would never advance its timer, so stuck jobs waited until the queue drained. Now: a wall-clock timer in the loop, and `run_once` reaps and re-claims when it finds nothing to do. The extra query happens only when idle.

**D9. Reaped jobs retry immediately, no backoff.**
A crash is usually about the worker, not the job. Kept simple; revisit if crash-loops on a shared dependency show up.

## Retries and side effects

**D10. Exponential backoff with full jitter, capped at 5 minutes.**
`uniform(0, min(cap, 2 * 2^(attempt-1)))`. Jitter stops a burst of jobs that failed together on the same outage from all retrying at the same instant.

**D11. `PermanentError` skips the retry budget.**
Malformed input fails the same way five times. Handlers raise `PermanentError` to dead-letter immediately (AC9).

**D12. At-least-once execution plus an effects ledger, not "exactly once".**
Exactly-once to an external system isn't achievable in general. What we can do: `ctx.once("name", fn)` checks `job_effects` for a record of that effect from any earlier attempt and returns the stored result instead of running it again (AC12). This is what "check whether a prior attempt already succeeded before retrying" means at the side-effect level, not just the job level.
Known gap, documented in code: if the process dies after `fn()` but before the record is written, the effect repeats. Closing that requires the downstream system to dedupe, so `ctx.effect_key(name)` gives a stable key to pass as an idempotency header (Stripe style). Rejected wrapping the effect and the record in one DB transaction: only works when the effect is itself a write to this same database.

**D13. Effects ledger survives manual retry.**
Retrying a dead job resets its attempt budget but keeps its recorded effects, so re-running a half-finished payment job won't charge again.

## API

**D14. Idempotent enqueue via `idempotency_key`.**
Clients retry on timeouts; without this, a slow response creates duplicate jobs. Same key and same request returns 200 with the original job. Same key with a different request returns 409, because silently returning the old job would hide a client bug. Implemented with `ON CONFLICT DO NOTHING` so concurrent duplicate requests can't both insert.

**D15. Reject unknown job types at enqueue (422), not at execution.**
Fail at the edge where the caller can fix it, instead of creating a job that's guaranteed to die later.

## Testing

**D16. Spec first, one test per acceptance criterion.**
Tests are named `test_ac<N>_...` against SPEC.md and were written before the implementation. Two failed on the first run (AC10, AC12), which is how D8 was found.

**D17. Simulate crashes with a `BaseException`, simulate time with SQL.**
`SimulatedCrash(BaseException)` escapes the worker's `except Exception`, leaving the job `running` exactly as `kill -9` would. Lease expiry is simulated by moving `lease_expires_at` into the past instead of sleeping, so the suite runs in about 5 seconds and isn't timing-flaky. The chaos script (`scripts/chaos_demo.py`) covers the real thing: separate processes, a real `SIGKILL`.

**D18. Chaos check had a wrong expectation.** *(changed)*
First run reported "140 valid emails succeeded: FAIL". The queue was right (150 valid of 160); the check's arithmetic was wrong. Fixed the assertion, not the code.

## Not done (yet)

- Priorities and per-queue concurrency limits
- Metrics endpoint (queue depth, age of oldest job, attempt latency)
- Archiving old succeeded jobs
- `LISTEN/NOTIFY` to wake idle workers instead of polling

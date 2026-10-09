-- Durable job queue schema. Safe to run repeatedly.

DO $$ BEGIN
    CREATE TYPE job_status AS ENUM ('queued', 'running', 'succeeded', 'dead');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS jobs (
    id               BIGSERIAL PRIMARY KEY,
    queue            TEXT        NOT NULL DEFAULT 'default',
    type             TEXT        NOT NULL,
    payload          JSONB       NOT NULL DEFAULT '{}',
    status           job_status  NOT NULL DEFAULT 'queued',
    idempotency_key  TEXT        UNIQUE,
    attempts         INT         NOT NULL DEFAULT 0,
    -- Fencing token: bumped on every claim, never reset (attempts can be, on
    -- manual retry). Workers' writes must match it, so a stale worker can't
    -- clobber a job someone else now owns.
    lease_token      BIGINT      NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by        TEXT,
    lease_expires_at TIMESTAMPTZ,
    result           JSONB,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);

-- Claim path: "oldest due job in this queue". Partial index stays small
-- because finished jobs fall out of it.
CREATE INDEX IF NOT EXISTS jobs_claimable_idx
    ON jobs (queue, run_at, id) WHERE status = 'queued';

-- Reaper path: "running jobs whose lease ran out".
CREATE INDEX IF NOT EXISTS jobs_lease_idx
    ON jobs (lease_expires_at) WHERE status = 'running';

-- One row per execution attempt, for debugging and the GET /jobs/{id} history.
CREATE TABLE IF NOT EXISTS job_attempts (
    id          BIGSERIAL PRIMARY KEY,
    job_id      BIGINT      NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INT         NOT NULL,
    lease_token BIGINT      NOT NULL,
    worker_id   TEXT        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT,       -- succeeded | failed | dead | lease_expired
    error       TEXT,
    UNIQUE (job_id, lease_token)
);

-- Effects ledger: side effects a job has already performed, keyed by name.
-- A retry consults this before doing the effect again.
CREATE TABLE IF NOT EXISTS job_effects (
    job_id      BIGINT      NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    result      JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, name)
);

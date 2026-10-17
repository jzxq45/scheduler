CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    schedule        TEXT NOT NULL,
    command         TEXT NOT NULL,
    payload         TEXT NOT NULL DEFAULT '',
    enabled         INTEGER NOT NULL DEFAULT 1,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    timeout_seconds INTEGER NOT NULL DEFAULT 300,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    created_at      INTEGER NOT NULL,   -- unix ms (UTC)
    updated_at      INTEGER NOT NULL,
    last_run_at     INTEGER,            -- unix ms
    next_run_at     INTEGER             -- unix ms
);

CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id      INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    started_at  INTEGER NOT NULL,
    finished_at INTEGER,
    status      TEXT NOT NULL,          -- running|success|failed|timeout|skipped|failed_final
    output      BLOB NOT NULL DEFAULT '',
    error       TEXT NOT NULL DEFAULT '',
    attempt     INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_jobs_name ON jobs(name);
CREATE INDEX IF NOT EXISTS idx_jobs_next_run ON jobs(next_run_at);
CREATE INDEX IF NOT EXISTS idx_runs_job ON runs(job_id);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);

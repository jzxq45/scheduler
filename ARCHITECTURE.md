# Architecture

```
                 config.yaml / SCHEDULER_* env
                            │
                      cmd/scheduler (main)
        errgroup{ signal-driven shutdown, HTTP server,
                  scheduler.Run (tick loop + drain),
                  retention.Cleaner }
                            │
        ┌───────────────────┼───────────────────────┐
        │                   │                       │
 internal/http        internal/scheduler            internal/retention
 (api pkg: routes,    (tick loop, claim, retry      (sweep old `runs`)
  ui, SSE broker,      queue, worker pool,
  prometheus /metrics) ShellExecutor)
        │                   │
        └──────► internal/store ◄────────────┘
              (SQLite, WAL, migrations;
               jobs, runs)      internal/cronexpr
                                 (own parser)
        internal/clock ─────► Clock: Real | Fake (tests)
        internal/config ────► validated typed config
        internal/metrics ───► Prometheus text exposition
```

## Core flow

1. `scheduler.Run` starts the worker pool, applies the missed-run
   policy to every enabled job, then enters a tick loop
   (default 1 s interval) built on `clock.Clock`.
2. Each tick queries `DueJobs` (enabled jobs with
   `next_run_at <= now`) and **claims** each job:
   an in-flight guard prevents duplicate concurrent executions and
   `SetRunTimes` books `last_run_at`/`next_run_at` so a crash cannot
   double-fire the same schedule point.
3. Claimed jobs become `Task`s in the worker pool. A `Pool` owns
   `pool_size` goroutines reading from a buffered channel — nothing is
   spawned per task.
4. The `Executor` (OS shell) runs the command under a per-job
   `context.WithTimeout`; stdout+stderr merge into a 64 KiB tail
   buffer that `FinishRun` persists.
5. Failure → `RetryQueue` arms a backoff timer through the same
   `Clock`; the next attempt re-enters the pool as a fresh `Task`.
   Exhausted attempts flip the job's failed runs to `failed_final`
   (dead letter).
6. Every observable step flows through `Reporter` callbacks; main
   fans them out to the SSE broker (live dashboard log) and the
   Prometheus registry.

## Graceful shutdown

One `errgroup` binds signal handling (`SIGINT`/`SIGTERM` → context
cancel) to three phases: the HTTP server stops accepting within 10 s,
the scheduler loop's tick timer stops firing and the pool *drains*
(waiting for in-flight tasks up to `DrainTimeout`), then the process
exits. Retries in flight are dropped, and any run still marked
`running` at the next start-up is closed as interrupted.

## Time and tests

All scheduling decisions obtain time from `clock.Clock`
(`Real` in production; `Fake` in tests, whose timers fire only on
explicit `Advance`). Tests therefore drive minute boundaries, missed
schedules, retry backoff and drain behaviour deterministically, without
sleeping on wall time: the retry queue registers its timer *before*
notifying observers so tests can advance the fake clock race-free.

## Store details

`modernc.org/sqlite` (pure Go, CGO not required) opened with a single
connection (writes are serialised; no `SQLITE_BUSY` deadlocks at this
scale) in WAL mode. The embedded `schema.sql` applies idempotently
(`CREATE TABLE IF NOT EXISTS`), so upgrades are automatic. `runs.output`
keeps only the tail of the output; retention (in-process) enforces the
age window instead of unbounded growth.

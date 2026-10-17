# scheduler

A cron scheduler in Go: SQLite persistence, a fixed worker pool,
missed-schedule policies, retries with exponential backoff, a JSON API,
a single-page dashboard with live SSE events, Prometheus metrics and a
self-hosted retention cleaner.

Stdlib-first: `net/http`, `html/template`, `log/slog`,
`modernc.org/sqlite` (pure-Go SQLite). The only direct dependencies are
`modernc.org/sqlite`, `golang.org/x/sync` and `github.com/google/uuid`;
the cron expression parser is written from scratch
(`internal/cronexpr`), so projection semantics are fully under test
control.

## Features

- **Cron expressions** — 5-field (min hour dom mon dow) and 6-field
  (with seconds) specs; `*`, `?`, lists, ranges, steps, month/weekday
  names; DST-aware next-fire computation.
- **Store** — SQLite in WAL mode with migrations; `jobs` and `runs`
  tables; run output capped at the last 64 KiB per run.
- **Worker pool** — fixed-size pool fed by a buffered channel; at most
  one in-flight execution per job; graceful drain on shutdown.
- **Missed-run policies** (`missed_policy`):
  - `skip` — roll the schedule forward, never fire in the past
    (default);
  - `latest` — run the last missed fire once, then roll forward;
  - `catchup` — run every missed fire time (backlog capped at 200).
- **Retries** — exponential backoff (1s, 2s, 4s, … capped at 5 min),
  configurable per job with `max_retries`; exhausted attempts go to a
  `failed_final` dead-letter state.
- **Executor** — commands run through the OS shell (`sh -c` /
  `cmd /C`), optional stdin payload, per-job timeout with
  `context.WithTimeout`, merged stdout+stderr.
- **HTTP API + UI** — CRUD for jobs, "Run now", run history, and an
  SSE stream (`/events`) feeding the live log on the dashboard.
- **Metrics** — Prometheus text format at `/metrics`
  (`jobs_started_total`, `jobs_finished_total{status=…}`,
  `jobs_retried_total`, `job_duration_seconds_*`,
  `job_next_run_in_seconds`, `scheduler_uptime_seconds`).
- **Retention** — a cleanup goroutine sweeps `runs` older than
  `retention_days` every hour.

## Quick start

```sh
make build
./bin/scheduler            # reads config.yaml, listens on :8081
```

Open `http://localhost:8081/` for the dashboard. Create a job via the
form or the API:

```sh
curl -s localhost:8081/api/jobs -d '{
  "name": "hello",
  "schedule": "*/5 * * * *",
  "command": "echo hi from cron",
  "timezone": "Europe/Berlin",
  "timeout_seconds": 30,
  "max_retries": 2
}' | jq
```

Run it now from the dashboard or:

```sh
curl -si -X POST localhost:8081/api/jobs/1/run
```

Follow the live event stream:

```sh
curl -N localhost:8081/events
```

Scrape metrics:

```sh
curl -s localhost:8081/metrics
```

## Configuration

`config.yaml` is a flat `key: value` file; every key can be overridden
with `SCHEDULER_<KEY>` environment variables. A missing config file is
not an error — defaults (plus env) are used, which is what the Docker
image relies on.

| key                | env override           | default     | meaning |
|--------------------|------------------------|-------------|---------|
| `listen`           | `SCHEDULER_LISTEN`     | `0.0.0.0:8081` | HTTP bind address |
| `db_path`          | `SCHEDULER_DB_PATH`    | `scheduler.db` | SQLite file |
| `worker_pool_size` | `SCHEDULER_WORKER_POOL_SIZE` | `10`  | fixed worker count (1..1024) |
| `retention_days`   | `SCHEDULER_RETENTION_DAYS`   | `30`   | keep finished runs this long |
| `log_level`        | `SCHEDULER_LOG_LEVEL`  | `info`      | `debug`, `info`, `warn`, `error` |
| `missed_policy`    | `SCHEDULER_MISSED_POLICY` | `skip`   | `skip`, `latest` or `catchup` |

Unknown YAML keys fail startup fast (typo protection); unknown env
vars are ignored.

## HTTP API

| Method | Path                  | Purpose |
|--------|-----------------------|---------|
| GET    | `/`                   | dashboard (single page) |
| GET    | `/api/jobs`           | list jobs |
| POST   | `/api/jobs`           | create (JSON body; absent `enabled` means true) |
| GET    | `/api/jobs/{id}`      | one job |
| PUT    | `/api/jobs/{id}`      | update |
| DELETE | `/api/jobs/{id}`      | delete (runs cascade) |
| POST   | `/api/jobs/{id}/run`  | "Run now" (409 while the job is already running) |
| GET    | `/api/jobs/{id}/runs` | run history (`?limit=`, ≤500) |
| GET    | `/events`             | SSE stream of run events |
| GET    | `/metrics`            | Prometheus exposition |

Crash recovery: runs still marked `running` after a restart are closed
as `failed` ("interrupted by scheduler restart").

## Docker

```sh
docker build -t scheduler .
docker run -p 8081:8081 -v scheduler-data:/data scheduler
```

## Development

```sh
make test   # go test -race ./...
make vet
make run
```

CI (GitHub Actions) runs `go vet` and the race-enabled test suite on
Linux and Windows, then builds the Docker image.

See [CRON-SYNTAX.md](CRON-SYNTAX.md) for the expression grammar and
[ARCHITECTURE.md](ARCHITECTURE.md) for how the pieces fit together.

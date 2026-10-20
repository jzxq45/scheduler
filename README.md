<div align="center">

# Go Cron Scheduler

A dependable job scheduler with its own cron parser, SQLite run history, and Prometheus metrics.

[![Go Version](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/devel/release)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Build Status](https://github.com/jzxq45/scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/jzxq45/scheduler/actions/workflows/ci.yml)

</div>

---

Go Cron Scheduler runs shell commands on cron schedules — with a
worker pool, exponential-backoff retries, missed-run policies
(`skip` / `latest` / `catchup`), a built-in web dashboard with a live
SSE log, run history in SQLite (WAL) and Prometheus metrics. The cron
parser and the test clock are written from scratch, so time-dependent
behaviour is tested deterministically.

## Features

- **Own cron parser** — 5-field (`min hour dom mon dow`) and 6-field
  (with seconds) expressions; `*`, `?`, lists, ranges, steps and
  month/weekday names; DST-aware next-fire projection. See
  [CRON-SYNTAX.md](CRON-SYNTAX.md).
- **Worker pool** — a fixed-size pool fed by a buffered channel; at
  most one in-flight execution per job; graceful drain on shutdown.
- **Exponential backoff retry** — 1 s, 2 s, 4 s, … capped at 5 min,
  per-job `max_retries`; exhausted attempts land in a `failed_final`
  dead-letter state.
- **Durable runs** — SQLite (pure Go driver, WAL) stores every run
  with merged stdout+stderr (last 64 KiB), status and attempt number;
  runs interrupted by a restart are closed as `failed`.
- **Missed-run policies** — `skip`, `latest` or bounded `catchup`
  (backlog capped at 200 firings) configured globally.
- **Web UI** — single-page dashboard: CRUD for jobs, a **Run now**
  button, run history with output and a live SSE event log.
- **Prometheus metrics** — standard text exposition at `/metrics`
  (`jobs_started_total`, `jobs_finished_total{status=…}`,
  `jobs_retried_total`, `job_duration_seconds_*`,
  `job_next_run_in_seconds`).
- **Graceful shutdown** — on SIGINT/SIGTERM: HTTP stops accepting,
  running jobs drain within their timeout budget, orphaned runs are
  reconciled at next start.

## Quick start

Requires Go 1.22+ (the `go` directive in go.mod may pin a newer
toolchain; the default `GOTOOLCHAIN=auto` downloads it transparently —
the same mechanism CI relies on).

```sh
# Run in place (reads ./config.yaml if present, listens on :8081)
go run ./cmd/scheduler
```

Then open <http://localhost:8081/> — create a job from the form:

- Name: `hello`
- Schedule: `*/5 * * * *`
- Command: `echo hello from cron`

![Dashboard](docs/screenshot.png)

Or use the API (below). A missing config file is fine: defaults apply
and every setting can be overridden with `SCHEDULER_*` env vars, e.g.

```sh
SCHEDULER_LISTEN=127.0.0.1:8080 SCHEDULER_DB_PATH=/tmp/sched.db go run ./cmd/scheduler
```

Run with Docker instead:

```sh
docker build -t scheduler .
docker run -p 8081:8081 -v scheduler-data:/data \
  -e SCHEDULER_LISTEN=0.0.0.0:8081 \
  -e SCHEDULER_API_TOKEN=change-me \
  scheduler
```

## Security

The HTTP API can create, edit, delete and **run arbitrary shell
commands**, so it is dangerous to expose by default:

- **Bind address.** The scheduler listens on `127.0.0.1:8081` unless
  told otherwise. Only publish the UI/API behind a reverse proxy
  with its own authentication.
- **API token** — set `SCHEDULER_API_TOKEN` (or `api_token` in
  `config.yaml`); then every request must carry
  `Authorization: Bearer <token>`:

  ```sh
  curl -si localhost:8081/api/jobs \
    -H "Authorization: Bearer my-secret-token"
  ```

  Browser clients (the dashboard, `EventSource`) can pass the token
  as a query parameter instead — open
  `http://localhost:8081/?token=<token>` once; it is remembered
  locally and added to every request. `401` answers carry no hint
  about *why* a token failed.
- **No secrets in logs** — the scheduler logs configuration keys,
  not values like the token; run output lives in the SQLite file,
  not in stdout.
- **Rate limiting** — `POST /api/jobs/{id}/run` is limited to 5
  triggers per client IP per minute. Behind a reverse proxy set
  `trust_proxy: true` (or `SCHEDULER_TRUST_PROXY=true`) so clients are
  keyed by `X-Forwarded-For` instead of all sharing the proxy's
  address; only set it when your proxy overwrites that header.

## Health checks

`GET /healthz` answers `200 {"status":"ok"}` when the process and the
SQLite store both respond, and `503 {"status":"down"}` when the store
is not answering — wire this into your container/lb probes.

## API Reference

### Create a job — `POST /api/jobs`

```sh
curl -si http://localhost:8081/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "backup-db",
    "schedule": "0 3 * * *",
    "command": "pg_dump --port 5432 mydb | gzip > /backups/mydb.sql.gz",
    "timezone": "Europe/Berlin",
    "timeout_seconds": 600,
    "max_retries": 2
  }'
```

`201 Created` returns the stored job including its computed
`next_run_at`; a duplicate `name` answers `409`, an invalid schedule
answers `400` with the parser error.

### List jobs — `GET /api/jobs`

```sh
curl -s http://localhost:8081/api/jobs | jq
```

```json
[
  {
    "id": 1,
    "name": "backup-db",
    "schedule": "0 3 * * *",
    "command": "pg_dump ...",
    "enabled": true,
    "timezone": "Europe/Berlin",
    "timeout_seconds": 600,
    "max_retries": 2,
    "last_run_at": "2026-10-17T01:00:02Z",
    "next_run_at": "2026-10-18T01:00:00Z"
  }
]
```

Other endpoints:

| Method | Path                  | Purpose |
|--------|-----------------------|---------|
| GET/PUT/DELETE | `/api/jobs/{id}` | read / update / delete (runs cascade) |
| POST   | `/api/jobs/{id}/run`  | trigger immediately (409 while running) |
| GET    | `/api/jobs/{id}/runs` | run history (`?limit=`, ≤500) |
| GET    | `/events`             | live SSE events |
| GET    | `/metrics`            | Prometheus metrics |

## Architecture

```
             ┌──────────────────────────────────────────┐
             │            cmd/scheduler (main)          │
             │  errgroup{ HTTP server, scheduler.Run,   │
             │            retention sweeper }           │
             └───────┬──────────────────┬───────────────┘
                     │                  │
   Ticker (1 s, clock.Clock)            │
                     │                  │
                     ▼                  │
             ┌───────────────┐          │
             │  Scheduler    │  claim: in-flight guard + next_run booking
             │  (tick loop)  │──────────────────────►  internal/store
             └───────┬───────┘                          (SQLite WAL:
                     │ enqueue Task                     jobs, runs)
                     ▼
             ┌───────────────┐   per-job context timeout
             │  Worker Pool  │  ─── Execute ───► os/exec (shell), 64 KiB
             │ (N workers)   │                    merged output tail
             └───────┬───────┘
                     │ on failure
                     ▼
             ┌───────────────┐   1s → 2s → 4s → … (cap 5 min)
             │  RetryQueue   │  exhausted ───► failed_final (dead letter)
             └───────────────┘
```

The tick loop drives job claiming, the pool executes them, retries
re-enter the pool through the retry queue, and every step persists to
`internal/store`. Full details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Why a custom cron parser and FakeClock

**Own cron parser (`internal/cronexpr`).** Third-party cron libraries
expose `Next(time.Time)` as a black box: the semantics of daylight
savings transition, the dom/dow OR-rule and sec-field stepping are
implementation details that can silently drift between versions. This
scheduler's whole correctness story ("what fires next, and when")
depends on that function, so the parser lives inside the repo — 60+
test cases, cross-checked against [crontab.guru](https://crontab.guru)
expectations, document and pin the exact projection behaviour.

**FakeClock (`internal/clock`).** Every scheduling decision — the tick
loop, retry backoff timers, retention sweeps — obtains time and timers
through a `clock.Clock` interface. Production uses `Real`; tests inject
`Fake`, whose timers only fire on an explicit `Advance()`. That makes
the cursed cases deterministic:

- "3 missed midnight runs" → three catch-up executions, no wall sleep;
- "backoff 1 s → 2 s → 4 s" → three advances, asserted on SQLite state;
- "drain while a task is blocked" → bounded shutdown, run ends as
  `skipped`.

The test suite runs with `-race` in CI and never sleeps to let "time
pass" — time is a dependency, so it is injected like any other.

## License

Released under the [MIT License](LICENSE) — free to use, modify and
distribute with attribution.

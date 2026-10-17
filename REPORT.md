# Status report

**Date:** 2026-10-17  **State:** all 13 phases complete, `go build`, `go vet`,
`go test -race ./...` all green.

## Commits (this sprint)

| Commit | Phase | What landed |
|--------|-------|-------------|
| `fadebed` | 1 | skeleton: structure, config (YAML+env), graceful shutdown |
| `8caeb10` | 2 | clock: `Clock` interface, `Real`, `Fake` (deterministic timers) |
| `97a3534` | 3 | own cron parser: 5/6 fields, `* ? , - /`, names, DST, 60+ cases vs crontab.guru |
| `81d18ca` | 4 | SQLite store (WAL, jobs+runs, CRUD, retention query, orphaned-run recovery) |
| `2a7982a` | 5–7 | scheduler core: 1 s tick loop, worker pool, missed policies `skip/catchup/latest`; worker+ShellExecutor (timeout, 64 KiB output tail); retry queue with exponential backoff + `failed_final` dead letter |
| `5837afc` | 8 | HTTP API (CRUD, Run Now, runs history) + web UI + SSE live log |
| `feebe64` | 9 | Prometheus text metrics + `/metrics` endpoint |
| `e59267d` | 10 | retention cleaner (self-hosted, hourly sweep) + deterministic retry tests |
| `12ab337` | 12 | GitHub Actions CI (vet+race tests on Linux/Windows, docker build) + multi-stage Dockerfile |
| `f4cbc45` | 13 | docs: README (en), CRON-SYNTAX.md, ARCHITECTURE.md |

Phase 11 (tests) was completed continuously — every phase shipped with
tests: cron parser, FakeClock-driven scheduler, worker pool,
executor (incl. real shell timeout), retry/dead-letter, store, config,
API endpoints, SSE stream, metrics render, retention sweep. Full
`go test -race -count=1 ./...` passes; scheduler tests were run twice
consecutively to shake out flakes.

## Work done this session

- **Finished half-done Phase 5** (tests did not compile: wrong
  `Reporter` argument in `setup`, two unused vars).
- **Fixed nil-deref crash** in `replayMissed` skip-policy path
  (`*j.NextRunAt` when nil) — `rollForward` now returns the computed
  next time, also eliminating a wall-clock leak (`time.Until` → fake
  clock).
- **Fixed single-fire tick bug**: a fake timer fires exactly once, but
  the tick loop now re-arms the timer after each tick (matches
  `time.Ticker` semantics for the real clock).
- **Fixed `LimitedBuffer` test expectation** (bug was in the assertion,
  keep-tail semantics was correct).
- **Missed-policy tests**: `latest` subtest was missing
  `MissedPolicy: "latest"` in options (silently used `skip`).
- **Race-free retry tests**: retry timers are now registered before the
  metrics observer is notified, and tests wait on a counting reporter
  before advancing the fake clock.
- **Config is container-friendly**: missing `config.yaml` is no longer
  fatal (defaults + `SCHEDULER_*` env).
- Wired the full binary in `main.go`: store → scheduler (RealClock,
  ShellExecutor, missed policy from config) × SSE broker × Prometheus
  × retention, all under one errgroup with graceful drain.

## Not implemented / limitations

- `internal/scheduler/scheduler_test.go` covers drain success (blocked
  executor finishes as `skipped`), but a dedicated long-running drain
  timeout assertion is thin.
- SSE events are keyed by job *name*; a renamed job breaks the UI link,
  nothing breaks.
- No client_golang — metrics is a hand-rolled text exposition (by
  design, stdlib-first).
- No UI pagination for large run histories (limit 50 by default, ≤500).

## How to run

```sh
make build && ./bin/scheduler     # config.yaml or SCHEDULER_* env
# UI:        http://localhost:8081/
# API:       GET/POST /api/jobs, POST /api/jobs/{id}/run, GET /events
# Metrics:   GET /metrics
docker build -t scheduler . && docker run -p 8081:8081 -v scheduler-data:/data scheduler
```

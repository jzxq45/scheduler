# Contributing

Thanks for helping improve Go Cron Scheduler! This document covers the
local workflow, the test conventions and the style rules that keep the
project consistent.

## Prerequisites

- Go 1.22+ (the toolchain version pinned in `go.mod` is downloaded
  automatically thanks to `GOTOOLCHAIN=auto`).
- No CGO required — the SQLite driver is pure Go.

## Run locally

```sh
# Start with repository defaults (listens on :8081, SQLite in ./scheduler.db)
go run ./cmd/scheduler
```

Configuration comes from `config.yaml` (flat `key: value` YAML) and is
overridden by `SCHEDULER_*` environment variables — see the table in
[README.md](README.md). Unknown YAML keys fail startup fast; a missing
config file falls back to defaults.

The dashboard is at <http://localhost:8081/>, the API under `/api/*`,
metrics at `/metrics`, the live event stream at `/events`.

## Run the tests

```sh
go test -race -count=1 ./...   # full suite, race detector on
go test -race -run TestRetryBackoffAndDeadLetter ./internal/scheduler/ -v
```

Testing conventions — please keep them:

- **All time flows through `internal/clock`.** Never call `time.Now`,
  `time.After` or `time.Sleep` in production code; in tests, drive the
  `Fake` clock with `Advance()` instead of sleeping. A test that needs
  a "sleep to let time pass" is a bug, not a workaround target.
- **SQLite tests** use `t.TempDir()` for database files.
- **Run tests with `-race`.** The worker pool, retry queue and SSE
  broker are concurrent by design.
- CI runs the same commands (`.github/workflows/ci.yml`); a PR is
  mergeable only when CI is green.

## Formatting and style rules

```sh
gofmt -l -w .                                      # canonical formatting
go run golang.org/x/tools/cmd/goimports@latest -local scheduler -w .
go vet ./...
golangci-lint run
```

- Follow `gofmt` output; imports are grouped stdlib → external →
  `scheduler/...` (goimports with `-local scheduler` does this).
- **Stdlib-first**: no new dependencies without prior discussion. The
  current direct dependencies are `modernc.org/sqlite`,
  `golang.org/x/sync` and `github.com/google/uuid`.
- Wrap errors with context (`fmt.Errorf("...: %w", err)`); user-facing
  messages in the API stay lowercase, no trailing punctuation.
- Comments are full sentences and explain *why*, not *what*.
- No changes to `internal/` behaviour without tests.

## Commits and pull requests

- Short, imperative commit subjects, optionally prefixed with the
  phase or area: `phase 5: scheduler core`, `docs: prepare repository
  for GitHub`, `fix: ...`, `test: ...`.
- Keep PRs small and focused; one concern per PR.
- Update docs (`README.md`, `CRON-SYNTAX.md`, `ARCHITECTURE.md`) when
  behaviour or interfaces change.

## Reporting bugs

Open an issue with: the exact cron expression, `config.yaml`
(redacted), relevant `run` rows from SQLite, and the scheduler log
around the incident. Timezone + expected vs actual fire time is the
minimal reproducible set for scheduling bugs.

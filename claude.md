# Rules
- Output: code only. No prose, no recap, no echo.
- Edits: diff/patch only, never full file.
- Reads: rg first, then Read with offset/limit.
- Tests: only requested pkg. No ./...
- No new files, no deps, no panics.
- No "Let me", "I'll", "Sure".

# Go
- 1.22+, ctx first.
- errors: fmt.Errorf("x: %w", err).
- slog, table tests, t.Parallel.
- gofmt + goimports + golangci-lint --fix.

# Autopilot
- Test fails → fix yourself, no questions.
- New file/pkg needed → create.
- Go commands (build/test/vet/lint/fmt) → run.
- 3 failed attempts → stop, report in 2 lines.
- git push, rm -rf, .env, DB migrations → always stop and ask.
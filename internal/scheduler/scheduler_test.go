package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"scheduler/internal/clock"
	"scheduler/internal/store"
)

// fakeExec records Execute calls and can fail or block.
type fakeExec struct {
	mu     sync.Mutex
	calls  []string
	failN  int // fail the first N executions
	blockC chan struct{}
}

func (f *fakeExec) Execute(ctx context.Context, command, payload string) ([]byte, error) {
	f.mu.Lock()
	n := len(f.calls)
	f.calls = append(f.calls, command)
	block := f.blockC
	fail := n < f.failN
	f.mu.Unlock()

	if block != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-block:
		}
	}
	if fail {
		return []byte("boom"), errors.New("exit status 1")
	}
	return []byte("out " + command), nil
}

func (f *fakeExec) size() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func openDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mkT(now string) time.Time {
	v, err := time.Parse(time.RFC3339, now)
	if err != nil {
		panic(err)
	}
	return v.UTC()
}

// countRep counts reporter callbacks so tests can wait for observable
// outcomes instead of racing goroutine scheduling.
type countRep struct {
	mu      sync.Mutex
	started, finished, retries, next int
}

func (c *countRep) JobStarted(string)                 { c.mu.Lock(); c.started++; c.mu.Unlock() }
func (c *countRep) JobFinished(string, string, time.Duration) {
	c.mu.Lock(); c.finished++; c.mu.Unlock()
}
func (c *countRep) RetryScheduled(string, int)        { c.mu.Lock(); c.retries++; c.mu.Unlock() }
func (c *countRep) NextRunSet(string, time.Duration)  { c.mu.Lock(); c.next++; c.mu.Unlock() }

func (c *countRep) retried() int { c.mu.Lock(); defer c.mu.Unlock(); return c.retries }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// waitFor polls cond every 5ms; wall time bounded even though the
// fake clock jumps.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func setup(t *testing.T, opt Options, exec *fakeExec) (*store.DB, *clock.Fake, *countRep, *Scheduler) {
	t.Helper()
	db := openDB(t)
	clk := clock.NewFake(mkT("2024-03-01T10:00:00Z"))
	cnt := &countRep{}
	return db, clk, cnt, New(db, clk, exec, opt, cnt)
}

func createJob(t *testing.T, db *store.DB, name, schedule string, maxRetries int) store.Job {
	t.Helper()
	j := store.Job{
		Name:           name,
		Schedule:       schedule,
		Command:        "true " + name,
		Timezone:       "UTC",
		TimeoutSeconds: 30,
		MaxRetries:     maxRetries,
		Enabled:        true,
	}
	id, err := db.CreateJob(context.Background(), &j)
	if err != nil {
		t.Fatal(err)
	}
	j.ID = id
	return j
}

// runScheduler runs in the background and shuts it down on cleanup.
func runScheduler(t *testing.T, s *Scheduler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("scheduler did not shut down")
		}
	})
}

func mustRuns(t *testing.T, db *store.DB, limit int) []store.Run {
	t.Helper()
	runs, err := db.ListRuns(context.Background(), limit)
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

func mustJob(t *testing.T, db *store.DB, name string) store.Job {
	j, err := db.GetJobByName(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// waitNext blocks until the job's next_run_at equals want: the
// scheduler goroutine has then done its startup replay, so fake-time
// advances cannot race timer creation.
func waitNext(t *testing.T, db *store.DB, name string, want time.Time) {
	t.Helper()
	waitFor(t, func() bool {
		j, err := db.GetJobByName(context.Background(), name)
		if err != nil {
			return false
		}
		return j.NextRunAt != nil && j.NextRunAt.Equal(want)
	})
}

func TestSchedulerFiresMinuteJob(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{}
	db, clk, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger()}, exec)
	createJob(t, db, "every-minute", "* * * * *", 0)
	_ = cnt

	runScheduler(t, s)

	waitFor(t, func() bool {
		j, err := db.GetJobByName(context.Background(), "every-minute")
		return err == nil && j.NextRunAt != nil
	})
	j := mustJob(t, db, "every-minute")
	if want := mkT("2024-03-01T10:01:00Z"); j.NextRunAt.UnixMilli() != want.UnixMilli() {
		t.Errorf("next run = %v, want %v", j.NextRunAt, want)
	}

	// Three minute-boundaries pass: three fires (step by step — one
	// tick per fake Advance, deterministic).
	clk.Advance(time.Minute)
	waitFor(t, func() bool { return exec.size() >= 1 })
	clk.Advance(time.Minute)
	waitFor(t, func() bool { return exec.size() >= 2 })
	clk.Advance(time.Minute)
	waitFor(t, func() bool { return exec.size() >= 3 })
	waitFor(t, func() bool { return len(mustRuns(t, db, 10)) >= 3 })
	for _, r := range mustRuns(t, db, 10) {
		if r.Status != store.StatusSuccess {
			t.Errorf("run=%d status %s", r.ID, r.Status)
		}
	}
}

func TestMissedPolicy(t *testing.T) {
	t.Parallel()

	t.Run("skip", func(t *testing.T) {
		t.Parallel()
		exec := &fakeExec{}
		db, _, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger()}, exec)
	_ = cnt
		j := createJob(t, db, "stale", "0 0 * * *", 0)
		// Stored next_run is 3 days in the past.
		past := mkT("2024-02-27T00:00:00Z")
		if err := db.SetRunTimes(context.Background(), j.ID, &past, &past); err != nil {
			t.Fatal(err)
		}
		runScheduler(t, s)
		waitFor(t, func() bool {
			j, err := db.GetJobByName(context.Background(), "stale")
			return err == nil && j.NextRunAt != nil && j.NextRunAt.After(mkT("2024-03-01T10:00:00Z"))
		})
		// The missed firings were skipped and nothing ran.
		waitFor(t, func() bool { return exec.size() == 0 && len(mustRuns(t, db, 100)) == 0 })
	})

	t.Run("latest", func(t *testing.T) {
		t.Parallel()
		exec := &fakeExec{}
		db, _, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger(), MissedPolicy: "latest"}, exec)
	_ = cnt
		j := createJob(t, db, "missed", "0 0 * * *", 0)
		past := mkT("2024-02-27T00:00:00Z")
		if err := db.SetRunTimes(context.Background(), j.ID, &past, &past); err != nil {
			t.Fatal(err)
		}
		runScheduler(t, s)
		waitFor(t, func() bool { return exec.size() == 1 })
		waitFor(t, func() bool {
			// drain
			runs := mustRuns(t, db, 10)
			return len(runs) == 1 && runs[0].Status == store.StatusSuccess
		})
		j2 := mustJob(t, db, "missed")
		if j2.NextRunAt == nil || j2.NextRunAt.Before(mkT("2024-03-01T10:00:00Z")) {
			t.Errorf("next run = %v", j2.NextRunAt)
		}
	})

	t.Run("catchup", func(t *testing.T) {
		t.Parallel()
		exec := &fakeExec{}
		db, _, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger(), MissedPolicy: "catchup"}, exec)
	_ = cnt
		j := createJob(t, db, "dailies", "0 0 * * *", 0)
		past := mkT("2024-02-27T00:00:00Z")
		if err := db.SetRunTimes(context.Background(), j.ID, &past, &past); err != nil {
			t.Fatal(err)
		}
		runScheduler(t, s)
		// 4 missed deadlines ("carefully" but complete).
		waitFor(t, func() bool { return exec.size() >= 4 })
		waitFor(t, func() bool {
			j, err := db.GetJobByName(context.Background(), "dailies")
			return err == nil && j.NextRunAt != nil && j.NextRunAt.After(mkT("2024-03-01T10:00:00Z"))
		})
	})
}

func TestRetryBackoffAndDeadLetter(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{failN: 99} // always fails
	db, clk, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger()}, exec)
	createJob(t, db, "flaky", "* * * * *", 2) // 2 retries → 3 attempts total

	runScheduler(t, s)
	waitNext(t, db, "flaky", mkT("2024-03-01T10:01:00Z"))
	clk.Advance(time.Minute)
	waitFor(t, func() bool { return exec.size() >= 1 && cnt.retried() >= 1 })
	clk.Advance(2 * time.Second)
	waitFor(t, func() bool { return len(mustRuns(t, db, 10)) == 2 && cnt.retried() >= 2 })
	clk.Advance(3 * time.Second)
	waitFor(t, func() bool { return len(mustRuns(t, db, 10)) == 3 })
	runs := mustRuns(t, db, 10)
	for _, r := range runs {
		if r.Status != store.StatusFailedFinal {
			t.Errorf("run=%d status %s, want failed_final", r.ID, r.Status)
		}
		if r.Error == "" {
			t.Errorf("run=%d missing error", r.ID)
		}
	}
	if n := exec.size(); n != 3 {
		t.Errorf("exec calls = %d, want 3", n)
	}
}

func TestRetrySucceedsMidWay(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{failN: 2} // 3rd attempt succeeds
	db, clk, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger()}, exec)
	createJob(t, db, "halfway", "* * * * *", 5)

	runScheduler(t, s)
	waitNext(t, db, "halfway", mkT("2024-03-01T10:01:00Z"))
	clk.Advance(time.Minute)
	waitFor(t, func() bool { return exec.size() >= 1 && cnt.retried() >= 1 })
	clk.Advance(2 * time.Second)
	waitFor(t, func() bool { return exec.size() >= 2 && cnt.retried() >= 2 })
	clk.Advance(3 * time.Second)
	waitFor(t, func() bool { return exec.size() >= 3 })

	var final store.Run
	for _, r := range mustRuns(t, db, 10) {
		if r.Attempt == 3 {
			final = r
		}
	}
	if final.Status != store.StatusSuccess {
		t.Errorf("attempt-3 run = %+v", final)
	}
}

func TestSchedulerDrain(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{blockC: make(chan struct{})}
	db, clk, cnt, s := setup(t, Options{PoolSize: 1, Log: testLogger()}, exec)
	createJob(t, db, "minute", "* * * * *", 0)
	_ = cnt

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- s.Run(ctx) }()

	waitNext(t, db, "minute", mkT("2024-03-01T10:01:00Z"))
	clk.Advance(time.Minute)
	waitFor(t, func() bool { return exec.size() >= 1 })

	// Cancel while the task is blocked in the executor: the run must
	// still be recorded (db writes use context.WithoutCancel) as
	// skipped with a finished_at and an error note.
	cancel()
	waitFor(t, func() bool {
		runs := mustRuns(t, db, 10)
		return len(runs) == 1 && runs[0].Status == store.StatusSkipped &&
			runs[0].FinishedAt != nil && runs[0].Error != ""
	})
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("scheduler did not shut down")
	}
}

func TestRunNowImmediate(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{}
	db, clk, cnt, s := setup(t, Options{PoolSize: 2, Log: testLogger()}, exec)
	j := createJob(t, db, "manual", "0 0 1 1 *", 0)
	_ = cnt
	runScheduler(t, s)

	if err := s.RunNow(context.Background(), j.ID); err != nil {
		t.Fatalf("run now: %v", err)
	}
	waitFor(t, func() bool { return exec.size() >= 1 })
	clk.Advance(time.Minute)
}

func TestRunNowGuardsInFlight(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{blockC: make(chan struct{})}
	db, clk, cnt, s := setup(t, Options{PoolSize: 1, Log: testLogger()}, exec)
	j := createJob(t, db, "busy", "0 0 1 1 *", 0)
	_ = cnt
	runScheduler(t, s)
	if err := s.RunNow(context.Background(), j.ID); err != nil {
		t.Fatalf("first: %v", err)
	}
	if err := s.RunNow(context.Background(), j.ID); err == nil {
		t.Fatal("second RunNow must fail while running")
	}
	clk.Advance(time.Minute)
}

// ---- worker / executor ----

func TestWorkerExecutesTask(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{}
	db := openDB(t)
	clk := clock.NewFake(mkT("2024-03-01T10:00:00Z"))
	pool := NewPool(1, exec, db, clk, testLogger(), noopReporter{}, func(int64) {})
	pool.SetRetry(func(context.Context, Task, time.Duration) {})
	pool.Start(context.Background())

	j := createJob(t, db, "w", "* * * * *", 0)
	pool.Enqueue(Task{StoreJob: j, Attempt: 1})
	waitFor(t, func() bool {
		runs, _ := db.ListRuns(context.Background(), 10)
		return len(runs) == 1 && runs[0].Status == store.StatusSuccess
	})
	runs, _ := db.ListRuns(context.Background(), 10)
	if len(runs) == 1 && string(runs[0].Output) != "out true w" {
		t.Errorf("output = %q", runs[0].Output)
	}
	pool.Drain(time.Second)
}

func TestWorkerPoolConcurrency(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{failN: 99}
	db := openDB(t)
	clk := clock.NewFake(mkT("2024-03-01T10:00:00Z"))
	pool := NewPool(4, exec, db, clk, testLogger(), noopReporter{}, func(int64) {})
	pool.SetRetry(func(context.Context, Task, time.Duration) {})
	pool.Start(context.Background())

	for i := 0; i < 4; i++ {
		j := createJob(t, db, fmt.Sprintf("w%d", i), "* * * * *", 0)
		pool.Enqueue(Task{StoreJob: j, Attempt: 1})
	}
	waitFor(t, func() bool { return exec.size() >= 4 })
	pool.Drain(time.Second)
}

// TestEnqueueAfterDrain: post-stop sends must be refused cleanly (no
// send-on-closed-channel panic, task released instead of run).
func TestEnqueueAfterDrain(t *testing.T) {
	t.Parallel()
	exec := &fakeExec{}
	db := openDB(t)
	clk := clock.NewFake(mkT("2024-03-01T10:00:00Z"))
	released := make(chan int64, 4)
	pool := NewPool(1, exec, db, clk, testLogger(), noopReporter{}, func(id int64) {
		released <- id
	})
	pool.SetRetry(func(context.Context, Task, time.Duration) {})
	pool.Start(context.Background())
	j := createJob(t, db, "late", "* * * * *", 0)

	pool.Drain(time.Second) // closes the queue
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Enqueue(Task{StoreJob: j, Attempt: 1})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue after Drain deadlocked")
	}
	select {
	case id := <-released:
		if id != j.ID {
			t.Fatalf("released wrong job %d", id)
		}
	default:
		t.Fatal("dropped task was not released")
	}
}

func TestBackoffDelays(t *testing.T) {
	t.Parallel()
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
		{20, 5 * time.Minute},
	}
	for _, c := range cases {
		if got := backoffDelay(c.attempt); got != c.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestLimitedBufferKeepsTail(t *testing.T) {
	t.Parallel()
	b := newLimitedBuffer(16)
	if _, err := b.Write([]byte("AAAAAAAAAAAAAAAAABCDEFGH")); err != nil { // 24 bytes
		t.Fatal(err)
	}
	if got := string(b.Bytes()); got != "AAAAAAAAABCDEFGH" {
		t.Errorf("tail = %q", got)
	}
}

// TestExecutor runs real shell commands.
func TestShellExecutorEchoAndTimeout(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Log("windows: echo via cmd")
	}
	e := ShellExecutor{}
	out, err := e.Execute(context.Background(), "echo hello", "")
	if err != nil || !inc(output(out), "hello") {
		t.Errorf("echo: out=%q err=%v", out, err)
	}

	// Timeout kills a long process.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = e.Execute(ctx, longRunningShell(), "")
	if err == nil {
		t.Error("long-running process must fail with timeout")
	}
}

func output(b []byte) string { return string(b) }

func inc(h string, sub string) bool { return strings.Contains(h, sub) }

func longRunningShell() string {
	if runtime.GOOS == "windows" {
		return "ping -n 100 127.0.0.1"
	}
	return "sleep 60"
}

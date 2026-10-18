package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"scheduler/internal/store"
)

// ErrJobStopped is the cancellation cause used when a job's only
// execution must be aborted because it was deleted or disabled. It
// colours the run record as skipped with an explicit note instead of
// leaving a half-finished failed status.
var ErrJobStopped = errors.New("job stopped (deleted or disabled)")

// Executor runs one job command. Implementations must honour ctx:
// cancellation or timeout must stop the underlying process.
type Executor interface {
	Execute(ctx context.Context, command, payload string) (output []byte, execErr error)
}

// Task is one unit of worker work.
type Task struct {
	StoreJob store.Job
	// Attempt starts at 1; retries increment.
	Attempt int
}

// Pool is a fixed-size worker pool fed by a channel. One job occupies
// at most one worker; nothing spawns per-task goroutines.
type Pool struct {
	queue chan Task
	// stopped guards the one-shot queue close: Enqueue reads it under
	// RLock so a post-Drain send can never hit `send on closed
	// channel` (no recover-at-a-distance hacks).
	mu      sync.RWMutex
	stopped bool

	exec    Executor
	db      *store.DB
	clk     eventClock
	log     *slog.Logger
	rep     Reporter
	release func(jobID int64)
	retry   func(context.Context, Task, time.Duration)

	// cancels holds one cancel cause per currently executing job,
	// keyed by job id. DeleteJob/disable calls CancelRun so a running
	// or queued execution of a dead job cannot outlive its row.
	cancelMu sync.Mutex
	cancels  map[int64]context.CancelCauseFunc

	wg sync.WaitGroup
}

// NewPool wires a pool; Drain closes the queue.
func NewPool(size int, exec Executor, db *store.DB, clk eventClock, log *slog.Logger, rep Reporter, release func(int64)) *Pool {
	if size < 1 {
		size = 1
	}
	return &Pool{
		// Buffer absorbs bursts so the tick loop never blocks.
		queue:   make(chan Task, 4*size),
		exec:    exec,
		db:      db,
		clk:     clk,
		log:     log,
		rep:     rep,
		release: release,
		cancels: make(map[int64]context.CancelCauseFunc),
	}
}

// SetRetry wires the retry sink (installed after RetryQueue exists).
func (p *Pool) SetRetry(fn func(context.Context, Task, time.Duration)) { p.retry = fn }

// Start launches workers; each exits when ctx is done or Drain runs.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < capFor(p); i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func capFor(p *Pool) int { return cap(p.queue) / 4 }

func (p *Pool) worker(ctx context.Context, i int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-p.queue:
			if t.StoreJob.ID == 0 {
				continue
			}
			p.runOne(ctx, t)
		}
	}
}

// Enqueue hands a task to a worker, non-blocking, and reports whether
// the task was accepted. The stopped check and the send happen under
// the SAME RLock: Stop takes the Lock before closing the queue, so
// close() cannot interleave between check and send — the
// send-on-closed-channel race window is gone. Refusal (pool draining
// or queue full) is reported to the caller so it can make the loss
// visible; the release hook still frees the in-flight guard here.
func (p *Pool) Enqueue(t Task) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.stopped {
		// Draining: refuse work.
		p.release2(t.StoreJob.ID)
		return false
	}
	select {
	case p.queue <- t:
		return true
	default:
		p.log.Error("pool queue full, dropping task", "job", t.StoreJob.Name)
		p.release2(t.StoreJob.ID)
		return false
	}
}

// Stop flags the pool closed and closes the queue exactly once. It
// waits for the Lock, so in-flight Enqueues holding RLock finish their
// send first.
func (p *Pool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.stopped = true
	close(p.queue)
}

// Drain closes the queue and waits up to d for the tail to finish.
func (p *Pool) Drain(d time.Duration) {
	p.Stop()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.log.Info("drain complete")
	case <-time.After(d):
		p.log.Warn("drain timeout, tasks still running")
	}
}

// release2 calls the release hook if one is wired.
func (p *Pool) release2(jobID int64) {
	if p.release != nil {
		p.release(jobID)
	}
}

// CancelRun aborts a job's currently executing run, if any. The
// ErrJobStopped cause is visible to runOne, so the aborted execution
// is recorded as skipped with an explicit note instead of failed.
func (p *Pool) CancelRun(jobID int64) {
	p.cancelMu.Lock()
	cancel, ok := p.cancels[jobID]
	delete(p.cancels, jobID)
	p.cancelMu.Unlock()
	if ok {
		cancel(ErrJobStopped)
	}
}

// trackCancel registers a run's cancel hook; cleanup mirrors it.
func (p *Pool) trackCancel(jobID int64, fn context.CancelCauseFunc) (untrack func()) {
	p.cancelMu.Lock()
	p.cancels[jobID] = fn
	p.cancelMu.Unlock()
	return func() {
		// The registry holds one entry per job (claim serialises
		// executions), so a plain delete cannot clobber a successor.
		p.cancelMu.Lock()
		delete(p.cancels, jobID)
		p.cancelMu.Unlock()
	}
}

// runOne executes a task, records the run, and decides retry or
// dead-lettering.
func (p *Pool) runOne(ctx context.Context, t Task) {
	j := t.StoreJob
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("task panic", "job", j.Name, "panic", r)
		}
		if p.release != nil {
			p.release(j.ID)
		}
	}()

	// The run record must survive shutdown: the scheduler context is
	// cancelled while draining, but jobs still finishing in that window
	// have to write their final status and output to the store.
	dbCtx := context.WithoutCancel(ctx)

	// Ghost-execution guard: the job row is authoritative at
	// execution time, not at enqueue time. A queued task or stale
	// retry chain must not run a command for a job that no longer
	// exists or was disabled while it waited.
	cur, err := p.db.GetJob(dbCtx, j.ID)
	switch {
	case err != nil:
		if errors.Is(err, store.ErrNotFound) {
			p.log.Info("job deleted before execution, dropping task", "job", j.Name)
		} else {
			p.log.Error("load job for execution", "job", j.Name, "err", err)
		}
		return
	case !cur.Enabled:
		// Disabled between claim and execution: record the refusal so
		// history stays honest, and stop the retry chain here.
		runID, rerr := p.db.CreateRun(dbCtx, j.ID, p.clk.Now(), t.Attempt)
		if rerr != nil {
			p.log.Error("record refused run", "job", j.Name, "err", rerr)
			return
		}
		if ferr := p.db.FinishRun(dbCtx, runID, store.StatusSkipped, p.clk.Now(), nil,
			"job disabled at execution time"); ferr != nil {
			p.log.Error("record refused run", "job", j.Name, "err", ferr)
		}
		return
	}

	start := p.clk.Now()
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Minute
	}
	// runCtx carries both the caller's cancellation/shutdown and the
	// per-job stop (deletion/disable) as a coloured cause.
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	untrack := p.trackCancel(j.ID, cancel)
	defer untrack()
	// The per-job timeout is layered on top so a stuck process dies on
	// its own budget while deletion/disable can still abort instantly.
	timeCtx, cancelTimeout := context.WithTimeout(runCtx, timeout)
	defer cancelTimeout()

	p.rep.JobStarted(j.Name)
	runID, err := p.db.CreateRun(dbCtx, j.ID, start, t.Attempt)
	if err != nil {
		p.log.Error("create run", "job", j.Name, "err", err)
		return
	}

	out, execErr := p.exec.Execute(timeCtx, j.Command, j.Payload)

	status := store.StatusSuccess
	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
		switch {
		case ctx.Err() != nil:
			// Pool shutdown cancelled the worker context.
			status = store.StatusSkipped
		case errors.Is(context.Cause(runCtx), ErrJobStopped):
			// Deletion/disable aborted this run mid-flight.
			status = store.StatusSkipped
			errMsg = "job cancelled: deleted or disabled"
		case timeoutExceeded(timeCtx):
			status = store.StatusTimeout
			// Enriched message must exist before FinishRun, or the
			// timeout note never reaches the stored run.
			errMsg = msg(errMsg, fmt.Sprintf("timeout after %s", timeout))
		default:
			status = store.StatusFailed
		}
	}

	fin := p.clk.Now()
	if err := p.db.FinishRun(dbCtx, runID, status, fin, out, errMsg); err != nil {
		p.log.Error("finish run", "job", j.Name, "err", err)
	}
	p.rep.JobFinished(j.Name, status, fin.Sub(start))

	if status == store.StatusFailed || status == store.StatusTimeout {
		p.handleRetries(dbCtx, runID, t, start)
	}
}

// timeoutExceeded reports whether runCtx hit its deadline before its
// parent was cancelled.
func timeoutExceeded(runCtx context.Context) bool {
	return runCtx.Err() == context.DeadlineExceeded
}

func msg(base, extra string) string {
	if base != "" {
		return base + "; " + extra
	}
	return extra
}

// handleRetries enqueues the next attempt after exponential backoff,
// or marks this run failed_final when attempts are exhausted.
func (p *Pool) handleRetries(dbCtx context.Context, runID int64, t Task, start time.Time) {
	j := t.StoreJob
	if j.MaxRetries < 0 {
		j.MaxRetries = 0
	}
	// attempts: initial 1 + j.MaxRetries retries; retries = attempts
	// beyond the first.
	retriesDone := t.Attempt - 1
	if retriesDone >= j.MaxRetries {
		if err := p.db.MarkRunFinal(dbCtx, runID); err != nil {
			p.log.Error("dead letter", "job", j.Name, "err", err)
		}
		return
	}
	if p.retry == nil {
		return
	}
	// A retry timer armed just before shutdown may outlive the
	// scheduler; its delivery will be refused by the stopped pool.
	p.retry(dbCtx, Task{StoreJob: j, Attempt: t.Attempt + 1}, backoffDelay(t.Attempt))
}

// backoffDelay schedules attempt n+1 after 2^(n-1) seconds capped at
// five minutes: 1s, 2s, 4s, 8s, ...
func backoffDelay(attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 12 {
		shift = 12
	}
	d := time.Duration(1<<uint(shift)) * time.Second
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d
}

// eventClock is the subset of clock.Clock the pool needs.
type eventClock interface {
	Now() time.Time
}

// ShellExecutor runs commands through the OS shell so script syntax
// ("echo hi && ls") works without embedding binaries.
type ShellExecutor struct{}

// Ensure Executor interface compliance.
var _ Executor = ShellExecutor{}

// Execute shells out; stdout+stderr are merged, keeping at most the
// last 64KiB so huge outputs cannot exhaust memory.
func (ShellExecutor) Execute(ctx context.Context, command, payload string) ([]byte, error) {
	shell, arg := "sh", "-c"
	if runtime.GOOS == "windows" {
		shell, arg = "cmd", "/C"
	}
	cmd := exec.CommandContext(ctx, shell, arg, command)
	// mouse stdin: feed payload so scripts can consume data; an empty
	// payload still gives the process a closed pipe.
	cmd.Stdin = newStringReader(payload)
	buf := newLimitedBuffer(64 * 1024)
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Run(); err != nil {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

// limitedBuffer is an io.Writer keeping only the tail of maxSize bytes,
// so unbounded run output cannot exhaust memory.
type limitedBuffer struct {
	max int
	buf []byte
}

func newLimitedBuffer(max int) *limitedBuffer {
	return &limitedBuffer{max: max}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.max {
		p = p[len(p)-b.max:]
	}
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = b.buf[len(b.buf)-b.max:]
	}
	return len(p), nil
}

// Bytes returns the retained output, never sharing the slice.
func (b *limitedBuffer) Bytes() []byte {
	out := make([]byte, len(b.buf))
	copy(out, b.buf)
	return out
}

type stringReader struct {
	s string
	i int
}

func newStringReader(s string) *stringReader { return &stringReader{s: s} }

func (r *stringReader) Read(p []byte) (int, error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n := copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

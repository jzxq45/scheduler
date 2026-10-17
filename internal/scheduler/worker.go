package scheduler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"scheduler/internal/store"
)

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
	// stop closed by Drain; workers reading Enqueue leave without
	// racing close(queue).
	stop chan struct{}

	exec    Executor
	db      *store.DB
	clk     eventClock
	log     *slog.Logger
	rep     Reporter
	release func(jobID int64)
	retry   func(Task, time.Duration)

	wg      sync.WaitGroup
	started bool
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
		stop:    make(chan struct{}),
	}
}

// SetRetry wires the retry sink (installed after RetryQueue exists).
func (p *Pool) SetRetry(fn func(Task, time.Duration)) { p.retry = fn }

// Start launches workers; each exits when ctx is done or Drain runs.
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < capFor(p); i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	p.started = true
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

// Enqueue hands a task to a worker, non-blocking. Send-on-closed can
// race with Drain; recovering here guards the shutdown race without a
// mutex on the normal path.
func (p *Pool) Enqueue(t Task) {
	defer func() {
		if recover() != nil {
			p.release(t.StoreJob.ID)
		}
	}()
	select {
	case p.queue <- t:
	case <-p.stop:
		// Draining: refuse new work.
	default:
		p.log.Error("pool queue full, dropping task", "job", t.StoreJob.Name)
		p.release(t.StoreJob.ID)
	}
}

// Drain closes the queue and waits up to d for the tail to finish.
func (p *Pool) Drain(d time.Duration) {
	close(p.queue)
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

	start := p.clk.Now()
	timeout := time.Duration(j.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	p.rep.JobStarted(j.Name)
	runID, err := p.db.CreateRun(ctx, j.ID, start, t.Attempt)
	if err != nil {
		p.log.Error("create run", "job", j.Name, "err", err)
		return
	}

	out, execErr := p.exec.Execute(runCtx, j.Command, j.Payload)

	status := store.StatusSuccess
	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
		switch {
		case ctx.Err() != nil:
			// Pool shutdown cancelled the worker context.
			status = store.StatusSkipped
		case timeoutExceeded(runCtx):
			status = store.StatusTimeout
			// Enriched message must exist before FinishRun, or the
			// timeout note never reaches the stored run.
			errMsg = msg(errMsg, fmt.Sprintf("timeout after %s", timeout))
		default:
			status = store.StatusFailed
		}
	}

	fin := p.clk.Now()
	if err := p.db.FinishRun(ctx, runID, status, fin, out, errMsg); err != nil {
		p.log.Error("finish run", "job", j.Name, "err", err)
	}
	p.rep.JobFinished(j.Name, status, fin.Sub(start))

	if status == store.StatusFailed || status == store.StatusTimeout {
		p.handleRetries(ctx, t, start)
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
func (p *Pool) handleRetries(ctx context.Context, t Task, start time.Time) {
	j := t.StoreJob
	if j.MaxRetries < 0 {
		j.MaxRetries = 0
	}
	// attempts: initial 1 + j.MaxRetries retries; retries = attempts
	// beyond the first.
	retriesDone := t.Attempt - 1
	if retriesDone >= j.MaxRetries {
		if err := p.db.MarkRunsFinal(ctx, j.ID); err != nil {
			p.log.Error("dead letter", "job", j.Name, "err", err)
		}
		return
	}
	if p.retry == nil {
		return
	}
	p.retry(Task{StoreJob: j, Attempt: t.Attempt + 1}, backoffDelay(t.Attempt))
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

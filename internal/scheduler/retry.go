package scheduler

import (
	"context"
	"sync"
	"time"

	"scheduler/internal/clock"
)

// RetryQueue holds retries between attempts. Every armed retry owns a
// goroutine that sleeps on a clock timer — count is bounded by
// max_retries per job, so this stays cheap and keeps the pool simple.
// Armed timers are tracked per job id, so deletion/disable can cancel
// the whole remaining chain instead of letting it fire for a dead job.
type RetryQueue struct {
	clk     clock.Clock
	deliver func(context.Context, Task)
	observe func(Task, int)

	// armed tracks pending retry timers by job id so Cancel can stop
	// them; entries are removed on delivery, cancellation and ctx
	// abort, so the map only ever holds genuinely pending timers.
	mu    sync.Mutex
	armed map[int64][]*clock.Timer
}

// NewRetryQueue wires the timer source, delivery sink and metrics hook.
func NewRetryQueue(clk clock.Clock, deliver func(context.Context, Task), observe func(Task, int)) *RetryQueue {
	return &RetryQueue{
		clk:     clk,
		deliver: deliver,
		observe: observe,
		armed:   make(map[int64][]*clock.Timer),
	}
}

// Arm schedules the next attempt after delay. The timer is registered
// synchronously before the observe callback returns, so callers that
// wait for observed retries know the FakeClock timer already exists.
// Cancelling ctx abandons the armed attempt (timer stopped, no
// delivery).
func (r *RetryQueue) Arm(ctx context.Context, t Task, delay time.Duration) {
	timer := r.clk.NewTimer(delay)
	r.track(t.StoreJob.ID, timer)
	if r.observe != nil {
		r.observe(t, t.Attempt)
	}
	go func() {
		select {
		case <-ctx.Done():
			timer.Stop()
			r.untrack(t.StoreJob.ID, timer)
		case <-timer.C:
			r.untrack(t.StoreJob.ID, timer)
			r.deliver(ctx, t)
		}
	}()
}

// Cancel drops every armed retry for a job: deleting or disabling a
// job must kill the whole remaining chain, not just the current run.
// Timers already delivered are the pool's problem (its execution-time
// job check refuses them).
func (r *RetryQueue) Cancel(jobID int64) {
	r.mu.Lock()
	timers := r.armed[jobID]
	delete(r.armed, jobID)
	r.mu.Unlock()
	for _, t := range timers {
		t.Stop()
	}
}

func (r *RetryQueue) track(jobID int64, t *clock.Timer) {
	r.mu.Lock()
	r.armed[jobID] = append(r.armed[jobID], t)
	r.mu.Unlock()
}

func (r *RetryQueue) untrack(jobID int64, t *clock.Timer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := r.armed[jobID]
	for i, e := range cur {
		if e == t {
			r.armed[jobID] = append(cur[:i], cur[i+1:]...)
			break
		}
	}
	if len(r.armed[jobID]) == 0 {
		delete(r.armed, jobID)
	}
}

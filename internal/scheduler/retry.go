package scheduler

import (
	"context"
	"time"

	"scheduler/internal/clock"
)

// RetryQueue holds retries between attempts. Every armed retry owns a
// goroutine that sleeps on a clock timer — count is bounded by
// max_retries per job, so this stays cheap and keeps the pool simple.
type RetryQueue struct {
	clk     clock.Clock
	deliver func(Task)
	observe func(Task, int)

	// ctx nil-safe: selecting on a nil channel never fires.
	ctx context.Context
}

// NewRetryQueue wires the timer source, delivery sink and metrics hook.
func NewRetryQueue(clk clock.Clock, deliver func(Task), observe func(Task, int)) *RetryQueue {
	return &RetryQueue{clk: clk, deliver: deliver, observe: observe, ctx: nil}
}

// SetRunCtx stops retry timers when the scheduler loop winds down.
func (r *RetryQueue) SetRunCtx(ctx context.Context) { r.ctx = ctx }

// Arm schedules the next attempt after delay. The timer is registered
// synchronously before the observe callback returns, so callers that
// wait for observed retries know the FakeClock timer already exists.
func (r *RetryQueue) Arm(t Task, delay time.Duration) {
	timer := r.clk.NewTimer(delay)
	if r.observe != nil {
		r.observe(t, t.Attempt)
	}
	go func() {
		select {
		case <-r.ctx.Done():
			timer.Stop()
		case <-timer.C:
			r.deliver(t)
		}
	}()
}

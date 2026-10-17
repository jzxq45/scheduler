package scheduler

import (
	"context"
	"time"

	"scheduler/internal/clock"
)

// RetryQueue holds retries between attempts. Every armed retry owns a
// goroutine that sleeps on a clock timer — count is bounded by
// max_retries per job, so this stays cheap and keeps the pool simple.
// The caller passes the context per arm; nothing is stored in the
// struct.
type RetryQueue struct {
	clk     clock.Clock
	deliver func(context.Context, Task)
	observe func(Task, int)
}

// NewRetryQueue wires the timer source, delivery sink and metrics hook.
func NewRetryQueue(clk clock.Clock, deliver func(context.Context, Task), observe func(Task, int)) *RetryQueue {
	return &RetryQueue{clk: clk, deliver: deliver, observe: observe}
}

// Arm schedules the next attempt after delay. The timer is registered
// synchronously before the observe callback returns, so callers that
// wait for observed retries know the FakeClock timer already exists.
// Cancelling ctx abandons the armed attempt (timer stopped, no
// delivery).
func (r *RetryQueue) Arm(ctx context.Context, t Task, delay time.Duration) {
	timer := r.clk.NewTimer(delay)
	if r.observe != nil {
		r.observe(t, t.Attempt)
	}
	go func() {
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
			r.deliver(ctx, t)
		}
	}()
}

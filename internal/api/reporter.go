package api

import (
	"time"
)

// Reporter adapts scheduler events to the SSE broker. It implements
// scheduler.Reporter and is meant to be handed to scheduler.New.
type Reporter struct {
	b *Broker
}

// NewReporter wires the broker hub.
func NewReporter(b *Broker) *Reporter { return &Reporter{b: b} }

// JobStarted publishes run_started.
func (r *Reporter) JobStarted(name string) {
	r.b.Publish(Event{Type: "run_started", Job: name, At: time.Now()})
}

// JobFinished publishes run_finished with duration and status.
func (r *Reporter) JobFinished(name, status string, d time.Duration) {
	r.b.Publish(Event{
		Type: "run_finished", Job: name, Status: status,
		DurMs: d.Milliseconds(), At: time.Now(),
	})
}

// RetryScheduled publishes run_retry.
func (r *Reporter) RetryScheduled(name string, attempt int) {
	r.b.Publish(Event{Type: "run_retry", Job: name, Attempt: attempt, At: time.Now()})
}

// NextRunSet publishes next_run (time until the next scheduled fire).
func (r *Reporter) NextRunSet(name string, d time.Duration) {
	r.b.Publish(Event{Type: "next_run", Job: name, NextInMs: d.Milliseconds(), At: time.Now()})
}

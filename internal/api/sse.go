package api

import (
	"sync"
	"time"
)

// Event is one item on the SSE stream: a small snapshot of scheduler
// activity. DurationMs is optional and filled for run_finish events.
type Event struct {
	Type       string `json:"type"` // run_started, run_finished, run_retry, run_triggered, next_run, job_created/updated/deleted
	Job        string `json:"job"`
	Status     string `json:"status,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	DurMs      int64  `json:"duration_ms,omitempty"`
	NextInMs   int64  `json:"next_in_ms,omitempty"`
	At         time.Time `json:"at"`
}

// Broker fans scheduler events out to any number of SSE subscribers.
// Publish never blocks: a slow subscriber's buffer overflows into a
// drop, so a stalled browser tab cannot stall workers.
type Broker struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// NewBroker creates an empty hub.
func NewBroker() *Broker {
	return &Broker{subs: make(map[chan Event]struct{})}
}

// Subscribe registers a new subscriber; the returned cancel func must
// be called on disconnect. Buffers hold 64 events ≈ a burst's worth.
func (b *Broker) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}
}

// Publish delivers e to every subscriber, replacing a stale pending
// event with the newest one when a buffer is full.
func (b *Broker) Publish(e Event) {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
			// Drop the oldest and keep the latest.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- e:
			default:
			}
		}
	}
}

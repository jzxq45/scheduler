package api

import (
	"errors"
	"sync"
	"time"
)

// ErrSubscribedFull is returned by Subscribe when the broker is at
// its subscriber cap: SSE connections are long-lived, so an unbounded
// set would grow without bound.
var ErrSubscribedFull = errors.New("too many event stream subscribers")

// maxSubscribers caps concurrent SSE streams; attempts beyond it get
// 503 and can retry later.
const maxSubscribers = 50

// Event is one item on the SSE stream: a small snapshot of scheduler
// activity. DurationMs is optional and filled for run_finish events.
type Event struct {
	Type     string    `json:"type"` // run_started, run_finished, run_retry, run_triggered, next_run, job_created/updated/deleted
	Job      string    `json:"job"`
	Status   string    `json:"status,omitempty"`
	Attempt  int       `json:"attempt,omitempty"`
	DurMs    int64     `json:"duration_ms,omitempty"`
	NextInMs int64     `json:"next_in_ms,omitempty"`
	At       time.Time `json:"at"`
}

// Broker fans scheduler events out to any number of capped SSE
// subscribers. Publish never blocks: a slow subscriber's buffer
// overflows into a drop, so a stalled browser tab cannot stall
// workers.
type Broker struct {
	// max may be overridden in tests via setMax.
	max  int
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

// NewBroker creates an empty hub.
func NewBroker() *Broker {
	return &Broker{max: maxSubscribers, subs: make(map[chan Event]struct{})}
}

func (b *Broker) setMax(n int) { b.mu.Lock(); b.max = n; b.mu.Unlock() }

func (b *Broker) size() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.subs) }

// Subscribe registers a new subscriber or reports ErrSubscribedFull
// when the cap is reached. The returned cancel func must be called on
// disconnect. Buffers hold 64 events ≈ a burst's worth.
func (b *Broker) Subscribe() (<-chan Event, func(), error) {
	ch := make(chan Event, 64)
	b.mu.Lock()
	if len(b.subs) >= b.max {
		b.mu.Unlock()
		return nil, nil, ErrSubscribedFull
	}
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}, nil
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

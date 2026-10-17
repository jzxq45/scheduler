package clock

import (
	"sync"
	"time"
)

// Fake is a controllable clock for tests. Timers created from it fire
// only when Advance moves time past their deadline; nothing happens
// asynchronously, so tests are fully deterministic.
type Fake struct {
	mu  sync.Mutex
	now time.Time
	// timers keeps pending timers; the number is small, so Advance
	// simply rescans the whole slice.
	timers []*fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
	fk       *Fake
}

var _ Clock = (*Fake)(nil)

// NewFake returns a Fake starting at the given time. Tests usually pass
// a fixed literal so timestamps in assertions are stable.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now returns the fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// After is shorthand for NewTimer(d).C.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.NewTimer(d).C
}

// NewTimer registers a timer that fires when Advance passes its deadline.
func (f *Fake) NewTimer(d time.Duration) *Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	ft := &fakeTimer{
		deadline: f.now.Add(d),
		ch:       make(chan time.Time, 1),
		fk:       f,
	}
	f.timers = append(f.timers, ft)
	return &Timer{
		C: ft.ch,
		stop: func() bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			if ft.fired {
				return false
			}
			ft.fired = true
			for i, t := range f.timers {
				if t == ft {
					f.timers = append(f.timers[:i], f.timers[i+1:]...)
					break
				}
			}
			return true
		},
	}
}

// Advance moves the fake forward by d and fires every timer whose
// deadline is in the past, in chronological order. Zero duration still
// fires timers whose deadline is exactly the current time.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now

	var due []*fakeTimer
	var rest []*fakeTimer
	for _, t := range f.timers {
		if t.fired {
			continue
		}
		if !t.deadline.After(now) {
			due = append(due, t)
			t.fired = true
		} else {
			rest = append(rest, t)
		}
	}
	// Fire non-blockingly: due timers are buffered (cap 1) so a slow
	// receiver cannot block Advance from firing the remaining timers.
	for _, t := range due {
		t.ch <- now
	}
	f.timers = rest
	f.mu.Unlock()
}

// Deadline returns the earliest pending timer deadline and whether any
// timer is pending; tests use it to assert on timer registration.
func (f *Fake) Deadline() (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.timers) == 0 {
		return time.Time{}, false
	}
	best := f.timers[0].deadline
	for _, t := range f.timers[1:] {
		if t.deadline.Before(best) {
			best = t.deadline
		}
	}
	return best, true
}

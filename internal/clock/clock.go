// Package clock abstracts time so all time-dependent logic can be
// tested deterministically. Every scheduler component must obtain time
// and timers through a Clock, never directly from package time.
package clock

import "time"

// Clock is the single time source for the scheduler.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTimer(d time.Duration) *Timer
}

// Timer wraps a timer whose channel signals at (or after) its deadline.
// It is a struct rather than *time.Timer so a fake clock can implement it.
type Timer struct {
	C <-chan time.Time

	// stop cancels the timer; it reports whether the timer had not
	// already fired.
	stop func() bool
}

// Stop prevents the timer from firing. It returns true if the timer
// had not yet fired.
func (t *Timer) Stop() bool {
	if t.stop == nil {
		return false
	}
	return t.stop()
}

// Real is the production clock backed by package time.
type Real struct{}

// Now returns the current wall time.
func (Real) Now() time.Time { return time.Now() }

// After returns a channel that receives the current time after d.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer returns a running timer backed by time.NewTimer.
func (Real) NewTimer(d time.Duration) *Timer {
	src := time.NewTimer(d)
	return &Timer{C: src.C, stop: func() bool { return src.Stop() }}
}

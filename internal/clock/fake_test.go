package clock

import (
	"testing"
	"time"
)

func TestFakeNowAndAdvance(t *testing.T) {
	t.Parallel()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	f := NewFake(start)
	if !f.Now().Equal(start) {
		t.Fatalf("now = %v, want %v", f.Now(), start)
	}
	f.Advance(2 * time.Hour)
	if got := f.Now(); !got.Equal(start.Add(2 * time.Hour)) {
		t.Fatalf("now after advance = %v", got)
	}
}

func TestFakeTimerFires(t *testing.T) {
	t.Parallel()
	f := NewFake(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	tm := f.NewTimer(30 * time.Minute)

	select {
	case v := <-tm.C:
		t.Fatalf("timer fired before advance: %v", v)
	default:
	}

	f.Advance(29 * time.Minute)
	select {
	case v := <-tm.C:
		t.Fatalf("timer fired too early: %v", v)
	default:
	}

	f.Advance(time.Minute)
	select {
	case v := <-tm.C:
		want := time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
		if !v.Equal(want) {
			t.Fatalf("fired %v, want %v", v, want)
		}
	default:
		t.Fatal("timer did not fire")
	}

	// Firing twice must not duplicate.
	f.Advance(time.Hour)
	select {
	case v := <-tm.C:
		t.Fatalf("unexpected second fire: %v", v)
	default:
	}
}

func TestFakeTimerStop(t *testing.T) {
	t.Parallel()
	f := NewFake(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	tm := f.NewTimer(time.Hour)
	if !tm.Stop() {
		t.Fatal("first Stop should return true")
	}
	if tm.Stop() {
		t.Fatal("second Stop should return false")
	}
	f.Advance(2 * time.Hour)
	select {
	case v := <-tm.C:
		t.Fatalf("stopped timer fired: %v", v)
	default:
	}
}

func TestFakeMultipleTimersOrder(t *testing.T) {
	t.Parallel()
	f := NewFake(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	t1 := f.NewTimer(time.Minute)
	t2 := f.NewTimer(2 * time.Minute)
	t3 := f.NewTimer(time.Minute)

	f.Advance(time.Minute)

	// Two timers share a deadline; both must have fired at the same time.
	for _, tm := range []*Timer{t1, t3} {
		select {
		case v := <-tm.C:
			if !v.Equal(start().Add(time.Minute)) {
				t.Fatalf("fired %v", v)
			}
		default:
			t.Fatal("timer with smaller deadline did not fire")
		}
	}

	f.Advance(time.Minute)
	select {
	case v := <-t2.C:
		if !v.Equal(start().Add(2 * time.Minute)) {
			t.Fatalf("t2 fired %v", v)
		}
	default:
		t.Fatal("t2 did not fire")
	}
}

func start() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }

func TestFakeDeadline(t *testing.T) {
	t.Parallel()
	f := NewFake(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	if _, ok := f.Deadline(); ok {
		t.Fatal("no timer should be pending")
	}
	_ = f.NewTimer(time.Hour)
	_ = f.NewTimer(10 * time.Minute)
	d, ok := f.Deadline()
	if !ok || !d.Equal(start().Add(10*time.Minute)) {
		t.Fatalf("deadline = %v, %v", d, ok)
	}
}

func TestRealClockSmoke(t *testing.T) {
	c := Real{}
	before := time.Now()
	tm := c.NewTimer(5 * time.Millisecond)
	select {
	case v := <-tm.C:
		if v.Before(before) {
			t.Fatalf("timer fired before creation: %v < %v", v, before)
		}
	case <-time.After(time.Second):
		t.Fatal("real timer did not fire")
	}
}

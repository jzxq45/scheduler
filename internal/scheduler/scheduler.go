// Package scheduler ties cron expressions, the store and a worker
// pool together: a 1-second ticker drains due jobs, claims them
// (last/next run bookkeeping) and hands work to workers. All time
// flows through clock.Clock so tests can Advance() a fake.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"scheduler/internal/clock"
	"scheduler/internal/config"
	"scheduler/internal/cronexpr"
	"scheduler/internal/store"
)

// Options selects scheduler behaviour.
type Options struct {
	// Interval is the tick granularity; due requests between ticks
	// wait up to one interval.
	Interval time.Duration
	// MissedPolicy decides what happens to backlogged schedules.
	MissedPolicy string
	PoolSize     int
	// DrainTimeout bounds how long shutdown waits for running tasks.
	DrainTimeout time.Duration
	Log          *slog.Logger
}

// normalize fills unset options with production-safe values.
func (o *Options) normalize() {
	if o.Interval <= 0 {
		o.Interval = time.Second
	}
	if o.PoolSize <= 0 {
		o.PoolSize = 10
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = 30 * time.Second
	}
	if o.MissedPolicy == "" {
		o.MissedPolicy = config.PolicySkip
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
}

// Reporter receives scheduler events; the metrics package implements
// it, tests use a noop.
type Reporter interface {
	JobStarted(name string)
	JobFinished(name, status string, d time.Duration)
	RetryScheduled(name string, attempt int)
	NextRunSet(name string, d time.Duration)
}

// noopReporter silences metrics until phase 9 wires real ones.
type noopReporter struct{}

func (noopReporter) JobStarted(string)                         {}
func (noopReporter) JobFinished(string, string, time.Duration) {}
func (noopReporter) RetryScheduled(string, int)                {}
func (noopReporter) NextRunSet(string, time.Duration)          {}

// Scheduler owns the tick loop and job claiming.
type Scheduler struct {
	db  *store.DB
	clk clock.Clock
	opt Options
	log *slog.Logger

	pool  *Pool
	retry *RetryQueue
	rep   Reporter

	mu       sync.Mutex
	inflight map[int64]bool

	// queueClosed guards one-shot pool shutdown.
	poolStarted sync.Once
	poolDrained sync.WaitGroup
	drainDone   chan struct{}
}

// New builds a scheduler; Run starts the loop.
func New(db *store.DB, clk clock.Clock, exec Executor, opt Options, rep Reporter) *Scheduler {
	opt.normalize()
	if rep == nil {
		rep = noopReporter{}
	}
	s := &Scheduler{
		db:        db,
		clk:       clk,
		opt:       opt,
		log:       opt.Log,
		rep:       rep,
		inflight:  make(map[int64]bool),
		drainDone: make(chan struct{}),
	}
	s.pool = NewPool(opt.PoolSize, exec, db, clk, opt.Log, rep, s.release)
	s.retry = NewRetryQueue(clk, s.pool.Enqueue, func(t Task, attempt int) {
		rep.RetryScheduled(t.StoreJob.Name, attempt)
	})
	s.pool.SetRetry(s.retry.Arm)
	return s
}

// Run runs the tick loop until ctx is cancelled, then drains the pool.
func (s *Scheduler) Run(ctx context.Context) error {
	s.retry.SetRunCtx(ctx)
	s.pool.Start(ctx) // errors inside workers are only logged

	if err := s.replayMissed(ctx); err != nil {
		s.log.Error("missed policy", "err", err)
	}

	tick := s.clk.NewTimer(s.opt.Interval)
	defer func() { tick.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return s.shutdown(ctx)
		case <-tick.C:
			now := s.clk.Now()
			jobs, err := s.db.DueJobs(ctx, now)
			if err != nil {
				s.log.Error("scan due", "err", err)
				continue
			}
			for _, j := range jobs {
				s.claim(ctx, now, j)
			}
			// A fake timer fires exactly once; arm the next tick.
			old := tick
			tick = s.clk.NewTimer(s.opt.Interval)
			old.Stop()
		}
	}
}

// replayMissed applies the missed-policy to every enabled job once at
// startup, before the first tick.
func (s *Scheduler) replayMissed(ctx context.Context) error {
	jobs, err := s.db.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	now := s.clk.Now()
	for _, j := range jobs {
		if !j.Enabled {
			continue
		}
		switch s.opt.MissedPolicy {
		case config.PolicyCatchup:
			s.catchupMissed(ctx, now, j)
		case config.PolicyLatest:
			if j.NextRunAt == nil || j.NextRunAt.After(now) {
				continue
			}
			s.claim(ctx, now, j)
		default: // skip
			if j.NextRunAt != nil && j.NextRunAt.After(now) {
				continue
			}
			next, err := s.rollForward(ctx, j, now)
			if err != nil {
				return err
			}
			s.rep.NextRunSet(j.Name, next.Sub(now))
		}
	}
	// Startup housekeeping: close runs orphaned by a previous process.
	if _, err := s.db.MarkOrphanedRuns(ctx, now); err != nil {
		s.log.Warn("orphaned runs", "err", err)
	}
	return nil
}

// catchupMissed enqueues every missed fire time (bounded, missing a
// schedule must not violate memory) and rolls next_run_at forward.
func (s *Scheduler) catchupMissed(ctx context.Context, now time.Time, j store.Job) {
	e, err := parseSchedule(j.Schedule)
	if err != nil {
		s.log.Error("schedule parse", "job", j.Name, "err", err)
		return
	}
	loc := loadLoc(j.Timezone)
	next := j.NextRunAt
	if next == nil || next.After(now) {
		// Nothing missed.
		if next == nil {
			if err := s.db.SetNextRun(ctx, j.ID, e.NextIn(now, loc)); err != nil {
				s.log.Error("set next run", "job", j.Name, "err", err)
			}
		}
		return
	}
	const maxBacklog = 200
	n := 0
	for t := *next; !t.After(now); t = e.NextIn(t.Add(time.Second), loc) {
		if n >= maxBacklog {
			s.log.Warn("catchup backlog capped", "job", j.Name, "capped", n)
			break
		}
		s.enqueueClaimed(ctx, j, t, nil)
		n++
	}
	if err := s.db.SetNextRun(ctx, j.ID, e.NextIn(now, loc)); err != nil {
		s.log.Error("set next run", "job", j.Name, "err", err)
	}
}

// claim guards one in-flight execution per job and books the claim in
// the store so a crash cannot double-fire.
func (s *Scheduler) claim(ctx context.Context, now time.Time, j store.Job) {
	s.mu.Lock()
	busy := s.inflight[j.ID]
	if !busy {
		s.inflight[j.ID] = true
	}
	s.mu.Unlock()
	if busy {
		s.log.Warn("job already running, skipping tick", "job", j.Name)
		if _, err := s.rollForward(ctx, j, now); err != nil {
			s.log.Error("roll forward", "job", j.Name, "err", err)
		}
		return
	}

	last := now
	if j.LastRunAt != nil && j.LastRunAt.After(last) {
		last = *j.LastRunAt
	}
	s.enqueueClaimed(ctx, j, now, nil)
}

// enqueueClaimed marks last_run at `at`, computes next_run strictly
// after `base` (needed for overlapping backoff windows), and starts a
// task. extraNext, when non-nil, replaces the computed next time.
func (s *Scheduler) enqueueClaimed(ctx context.Context, j store.Job, at time.Time, base *time.Time) {
	e, err := parseSchedule(j.Schedule)
	if err != nil {
		s.log.Error("schedule parse", "job", j.Name, "err", err)
		s.release(j.ID)
		return
	}
	loc := loadLoc(j.Timezone)
	baseT := at
	if base != nil {
		baseT = *base
	}
	next := e.NextIn(baseT.Add(time.Second), loc)
	if err := s.db.SetRunTimes(ctx, j.ID, &at, &next); err != nil {
		s.log.Error("claim job", "job", j.Name, "err", err)
		s.release(j.ID)
		return
	}
	s.rep.NextRunSet(j.Name, next.Sub(at))
	s.pool.Enqueue(Task{StoreJob: j, Attempt: 1})
}

// rollForward recomputes next_run_at from now; used when the current
// tick cannot start the job (skipped policy and overlaps).
func (s *Scheduler) rollForward(ctx context.Context, j store.Job, now time.Time) (time.Time, error) {
	e, err := parseSchedule(j.Schedule)
	if err != nil {
		return now, fmt.Errorf("roll forward: %w", err)
	}
	next := e.NextIn(now.Add(time.Second), loadLoc(j.Timezone))
	if err := s.db.SetNextRun(ctx, j.ID, next); err != nil {
		return now, fmt.Errorf("roll forward: %w", err)
	}
	return next, nil
}

// release marks a job no longer in flight.
func (s *Scheduler) release(jobID int64) {
	s.mu.Lock()
	delete(s.inflight, jobID)
	s.mu.Unlock()
}

// shutdown stops accepting work and waits for current tasks.
func (s *Scheduler) shutdown(ctx context.Context) error {
	s.log.Info("draining", "timeout", s.opt.DrainTimeout)
	s.pool.Drain(s.opt.DrainTimeout)
	return nil
}

// RunNow starts job id immediately (HTTP "Run now"). It respects the
// in-flight guard.
func (s *Scheduler) RunNow(ctx context.Context, jobID int64) error {
	j, err := s.db.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("run now: %w", err)
	}
	s.mu.Lock()
	busy := s.inflight[j.ID]
	if !busy {
		s.inflight[j.ID] = true
	}
	s.mu.Unlock()
	if busy {
		return fmt.Errorf("run now: job %q already running", j.Name)
	}
	s.enqueueClaimed(ctx, j, s.clk.Now(), nil)
	return nil
}

// parseSchedule is exposed for tests.
func parseSchedule(spec string) (cronexpr.Expr, error) {
	return cronexpr.Parse(spec)
}

func loadLoc(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC
	}
	return loc
}

// Package retention deletes old run history on a fixed sweep
// interval, so a long-lived scheduler cannot grow its SQLite file
// without bound. The cleaner is self-hosted: it shares the
// scheduler's clock abstraction and database.
package retention

import (
	"context"
	"log/slog"
	"time"

	"scheduler/internal/clock"
	"scheduler/internal/store"
)

// DefaultSweep is how often old runs are purged.
const DefaultSweep = time.Hour

// Cleaner deletes finished runs older than its retention window.
type Cleaner struct {
	db       *store.DB
	clk      clock.Clock
	older    time.Duration // maximum age of a run
	interval time.Duration
	log      *slog.Logger
}

// New builds a cleaner keeping `days` of run history.
func New(db *store.DB, clk clock.Clock, days int, log *slog.Logger) *Cleaner {
	if log == nil {
		log = slog.Default()
	}
	return &Cleaner{
		db:       db,
		clk:      clk,
		older:    time.Duration(days) * 24 * time.Hour,
		interval: DefaultSweep,
		log:      log,
	}
}

// Run sweeps until ctx is done; the first sweep happens immediately
// at startup, the rest on the interval timer.
func (c *Cleaner) Run(ctx context.Context) error {
	c.Sweep(ctx)
	tick := c.clk.NewTimer(c.interval)
	defer func() { tick.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			c.Sweep(ctx)
			old := tick
			tick = c.clk.NewTimer(c.interval)
			old.Stop()
		}
	}
}

// Sweep deletes runs older than the retention window once. ctx comes
// from the caller: with the scheduler shutting down the pending
// delete is abandoned instead of writing with a Background context.
// A nil ctx falls back to Background so direct callers (tests, admin
// endpoints) do not have to plumb one through.
func (c *Cleaner) Sweep(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	cutoff := c.clk.Now().Add(-c.older)
	n, err := c.db.DeleteRunsBefore(ctx, cutoff)
	if err != nil {
		c.log.Error("retention sweep", "err", err)
		return
	}
	if n > 0 {
		c.log.Info("retention sweep", "deleted", n)
	}
}

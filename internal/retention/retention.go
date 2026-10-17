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
	c.Sweep()
	tick := c.clk.NewTimer(c.interval)
	defer func() { tick.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			c.Sweep()
			old := tick
			tick = c.clk.NewTimer(c.interval)
			old.Stop()
		}
	}
}

// Sweep deletes runs older than the retention window once.
func (c *Cleaner) Sweep() {
	cutoff := c.clk.Now().Add(-c.older)
	n, err := c.db.DeleteRunsBefore(context.Background(), cutoff)
	if err != nil {
		c.log.Error("retention sweep", "err", err)
		return
	}
	if n > 0 {
		c.log.Info("retention sweep", "deleted", n)
	}
}

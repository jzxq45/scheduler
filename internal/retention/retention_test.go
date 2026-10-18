package retention

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"scheduler/internal/clock"
	"scheduler/internal/store"
)

func logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func setupDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "r.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCleanerDeletesOldRuns(t *testing.T) {
	db := setupDB(t)
	clk := clock.NewFake(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	ctx := context.Background()

	id, err := db.CreateJob(ctx, &store.Job{Name: "j", Schedule: "* * * * *", Command: "x",
		Enabled: true, Timezone: "UTC", TimeoutSeconds: 60,
		CreatedAt: clk.Now(), UpdatedAt: clk.Now()})
	if err != nil {
		t.Fatal(err)
	}
	old, err := db.CreateRun(ctx, id, clk.Now().Add(-40*24*time.Hour), 1)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := db.CreateRun(ctx, id, clk.Now(), 1)
	if err != nil {
		t.Fatal(err)
	}

	c := New(db, clk, 30, logger())
	c.Sweep(context.Background())

	if _, err := db.GetRun(ctx, old); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("old run should be deleted, err=%v", err)
	}
	if _, err := db.GetRun(ctx, fresh); err != nil {
		t.Fatalf("fresh run must survive: %v", err)
	}

	// Run exits promptly when the context is cancelled.
	rctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(rctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after cancel")
	}
}

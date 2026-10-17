// Command scheduler is a cron job scheduler with SQLite store, a
// worker pool and an HTTP API + dashboard. It runs the scheduler loop
// and the HTTP server under an errgroup and shuts down gracefully on
// SIGINT/SIGTERM: the HTTP server stops accepting, then running jobs
// drain within their timeout budget.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"scheduler/internal/api"
	"scheduler/internal/clock"
	"scheduler/internal/config"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

// drainTimeout bounds HTTP shutdown; job drain is bounded separately
// by the scheduler's DrainTimeout.
const drainTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		return err
	}
	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)
	logger.Info("starting", "config", cfg)

	db, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(context.Background()); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Build the SSE hub first, so runs stream from the first tick.
	broker := api.NewBroker()
	rep := api.NewReporter(broker)
	sched := scheduler.New(db, clock.Real{}, scheduler.ShellExecutor{},
		scheduler.Options{
			PoolSize:     cfg.WorkerPoolSize,
			MissedPolicy: cfg.MissedPolicy,
			DrainTimeout: drainTimeout,
			Log:          logger,
		}, rep)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           api.New(db, sched, logger, broker),
		ReadHeaderTimeout: 5 * time.Second,
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})

	g.Go(func() error {
		logger.Info("http listening", "addr", cfg.Listen)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Scheduler drains its pool when gctx is cancelled.
	g.Go(func() error {
		return sched.Run(gctx)
	})

	return g.Wait()
}

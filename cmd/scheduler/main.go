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
	"scheduler/internal/metrics"
	"scheduler/internal/retention"
	"scheduler/internal/scheduler"
	"scheduler/internal/store"
)

// reporterFan sends scheduler events to every subscriber: SSE hub and
// Prometheus registry both implement scheduler.Reporter.
type reporterFan []scheduler.Reporter

func (f reporterFan) JobStarted(name string) {
	for _, r := range f {
		r.JobStarted(name)
	}
}

func (f reporterFan) JobFinished(name, status string, d time.Duration) {
	for _, r := range f {
		r.JobFinished(name, status, d)
	}
}

func (f reporterFan) RetryScheduled(name string, attempt int) {
	for _, r := range f {
		r.RetryScheduled(name, attempt)
	}
}

func (f reporterFan) NextRunSet(name string, d time.Duration) {
	for _, r := range f {
		r.NextRunSet(name, d)
	}
}

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
	reg := metrics.New()
	reps := reporterFan{api.NewReporter(broker), reg}
	sched := scheduler.New(db, clock.Real{}, scheduler.ShellExecutor{},
		scheduler.Options{
			PoolSize:     cfg.WorkerPoolSize,
			MissedPolicy: cfg.MissedPolicy,
			DrainTimeout: drainTimeout,
			Log:          logger,
		}, reps)

	apiSrv := api.New(db, sched, logger, broker, cfg.APIToken)
	apiSrv.SetTrustedProxy(cfg.TrustProxy)
	apiSrv.MetricsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", metrics.ServerContent)
		_, _ = w.Write([]byte(reg.Render()))
	}))

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           apiSrv,
		ReadHeaderTimeout: 5 * time.Second,
		// Bounded reads: bodies are small JSON (capped at 1 MiB by
		// MaxBytesReader a few layers up), so 30s is generous.
		ReadTimeout: 30 * time.Second,
		// IdleTimeout must stay: keep-alive connections after one
		// request otherwise linger forever (slowloris on the pool).
		// No WriteTimeout: /events is a long-lived SSE stream and a
		// global write deadline would cut every subscriber mid-flight.
		IdleTimeout: 60 * time.Second,
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

	// Self-hosted cleanup: purge runs older than the retention window.
	g.Go(func() error {
		return retention.New(db, clock.Real{}, cfg.RetentionDays, logger).Run(gctx)
	})

	return g.Wait()
}

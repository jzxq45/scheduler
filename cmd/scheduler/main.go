// Command scheduler is a cron job scheduler with HTTP API, SQLite store
// and a worker pool. It starts an errgroup with the HTTP server and
// installs signal-triggered graceful shutdown.
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

	"scheduler/internal/config"
)

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		<-gctx.Done()
		logger.Info("shutting down")
		return nil
	})

	g.Go(func() error {
		srv := &http.Server{Addr: cfg.Listen, Handler: http.NewServeMux()}
		g.Go(func() error {
			<-gctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
			defer cancel()
			return srv.Shutdown(shutdownCtx)
		})
		logger.Info("http listening", "addr", cfg.Listen)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	return g.Wait()
}

// drainTimeout bounds HTTP shutdown and later the worker drain phase.
const drainTimeout = 10 * time.Second

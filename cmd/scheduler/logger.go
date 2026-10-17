package main

import (
	"log/slog"
	"os"
)

// newLogger returns a slog.Logger writing structured text records to
// stdout at the named level.
func newLogger(level string) *slog.Logger {
	cls := slog.LevelInfo
	switch level {
	case "debug":
		cls = slog.LevelDebug
	case "warn":
		cls = slog.LevelWarn
	case "error":
		cls = slog.LevelError
	}
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cls})
	return slog.New(h)
}

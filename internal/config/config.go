// Package config loads scheduler configuration from YAML and environment.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// Missed task policies applied at scheduler startup.
const (
	PolicySkip    = "skip"
	PolicyCatchup = "catchup"
	PolicyLatest  = "latest"
)

// Config holds all runtime settings. Fields are validated by Load.
type Config struct {
	Listen         string
	DBPath         string
	WorkerPoolSize int
	RetentionDays  int
	LogLevel       string
	MissedPolicy   string
}

func defaults() Config {
	return Config{
		Listen:         "0.0.0.0:8081",
		DBPath:         "scheduler.db",
		WorkerPoolSize: 10,
		RetentionDays:  30,
		LogLevel:       "info",
		MissedPolicy:   PolicySkip,
	}
}

// Load reads config from path (flat "key: value" YAML), then applies
// SCHEDULER_* env overrides. A missing config file is not an error:
// defaults and env overrides still apply (12-factor / container
// setups). Unknown env vars are ignored; unknown YAML keys are an
// error so typos in config.yaml fail fast.
func Load(path string) (Config, error) {
	cfg := defaults()

	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return cfg, fmt.Errorf("load config: %w", err)
	}
	if err == nil {
		if err := applyYAML(&cfg, raw); err != nil {
			return cfg, fmt.Errorf("load config: %w", err)
		}
	}
	if err := applyEnv(&cfg, os.Environ()); err != nil {
		return cfg, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return cfg, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}

func applyYAML(cfg *Config, raw []byte) error {
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, err := parseLine(line)
		if err != nil {
			return fmt.Errorf("line %d: %w", i+1, err)
		}
		var setErr error
		switch key {
		case "listen":
			cfg.Listen = val
		case "db_path":
			cfg.DBPath = val
		case "worker_pool_size":
			cfg.WorkerPoolSize, setErr = strconv.Atoi(val)
		case "retention_days":
			cfg.RetentionDays, setErr = strconv.Atoi(val)
		case "log_level":
			cfg.LogLevel = val
		case "missed_policy":
			cfg.MissedPolicy = val
		default:
			return fmt.Errorf("line %d: unknown key %q", i+1, key)
		}
		if setErr != nil {
			return fmt.Errorf("line %d: %s: %w", i+1, key, setErr)
		}
	}
	return nil
}

func parseLine(line string) (key, val string, err error) {
	key, val, ok := strings.Cut(line, ":")
	if !ok {
		return "", "", fmt.Errorf("expected \"key: value\", got %q", line)
	}
	key = strings.TrimSpace(key)
	val = strings.TrimSpace(val)
	// Strip an optional trailing comment, respecting a quoted value.
	if len(val) > 1 && (val[0] == '"' || val[0] == '\'') {
		q := val[0]
		if end := strings.IndexByte(val[1:], q); end >= 0 {
			return key, val[1 : end+1], nil
		}
		return "", "", fmt.Errorf("unterminated quote in %q", line)
	}
	if c := strings.Index(val, " #"); c >= 0 {
		val = val[:c]
	}
	val = strings.TrimSpace(val)
	if key == "" || val == "" {
		return "", "", fmt.Errorf("empty key or value in %q", line)
	}
	return key, val, nil
}

func applyEnv(cfg *Config, env []string) error {
	for _, kv := range env {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(k, "SCHEDULER_") {
			continue
		}
		key := strings.ToLower(strings.TrimPrefix(k, "SCHEDULER_"))
		var setErr error
		switch key {
		case "listen":
			cfg.Listen = v
		case "db_path":
			cfg.DBPath = v
		case "worker_pool_size":
			cfg.WorkerPoolSize, setErr = strconv.Atoi(v)
		case "retention_days":
			cfg.RetentionDays, setErr = strconv.Atoi(v)
		case "log_level":
			cfg.LogLevel = v
		case "missed_policy":
			cfg.MissedPolicy = v
		}
		if setErr != nil {
			return fmt.Errorf("%s: %w", k, setErr)
		}
	}
	return nil
}

func (c Config) validate() error {
	switch {
	case c.Listen == "":
		return fmt.Errorf("listen must not be empty")
	case c.DBPath == "":
		return fmt.Errorf("db_path must not be empty")
	case c.WorkerPoolSize < 1 || c.WorkerPoolSize > 1024:
		return fmt.Errorf("worker_pool_size must be 1..1024, got %d", c.WorkerPoolSize)
	case c.RetentionDays < 1:
		return fmt.Errorf("retention_days must be >= 1, got %d", c.RetentionDays)
	case c.MissedPolicy != PolicySkip && c.MissedPolicy != PolicyCatchup && c.MissedPolicy != PolicyLatest:
		return fmt.Errorf("missed_policy must be skip|catchup|latest, got %q", c.MissedPolicy)
	}
	return nil
}

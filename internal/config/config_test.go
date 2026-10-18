package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestLoadMissingFileUsesDefaultsAndEnv: no config.yaml is fine
// (containers rely on SCHEDULER_* env).
func TestLoadMissingFile(t *testing.T) {
	t.Parallel()
	cfg, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("missing file should not fail: %v", err)
	}
	if cfg != defaults() {
		t.Errorf("got %+v, want %+v", cfg, defaults())
	}
}

// The slog representation must never carry the token itself, only the
// boolean flag: stdout and log shippers are read by more people than
// the config file. Verified through a real logger call, which is
// exactly the production path ("logger.Info(\"starting\", \"config\", cfg)").
func TestConfigLogValueHidesToken(t *testing.T) {
	t.Parallel()
	c := Config{Listen: "127.0.0.1:8081", DBPath: "s.db", LogLevel: "info", APIToken: "s3cret-token"}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("starting", "config", c)
	out := buf.String()
	if strings.Contains(out, "s3cret-token") {
		t.Errorf("token leaked into log output: %s", out)
	}
	if !strings.Contains(out, "api_token_set=true") {
		t.Errorf("api_token_set flag missing: %s", out)
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := Load(write(t, "listen: \"127.0.0.1:8081\"\ndb_path: scheduler.db\nworker_pool_size: 10\nretention_days: 30\nlog_level: info\nmissed_policy: skip\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := defaults()
	if cfg != want {
		t.Errorf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadCommentsAndQuotes(t *testing.T) {
	t.Parallel()
	cfg, err := Load(write(t, "# comment\nlisten: \"0.0.0.0:9999\" # trailing\nmissed_policy: catchup\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != "0.0.0.0:9999" || cfg.MissedPolicy != "catchup" {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoadUnknownKey(t *testing.T) {
	t.Parallel()
	if _, err := Load(write(t, "listent: 1")); err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("SCHEDULER_LISTEN", "0.0.0.0:8082")
	t.Setenv("SCHEDULER_WORKER_POOL_SIZE", "7")
	t.Setenv("SCHEDULER_MISSED_POLICY", "latest")
	t.Setenv("SCHEDULER_API_TOKEN", "tok-123")
	t.Setenv("OTHER", "ignored")

	cfg, err := Load(write(t, "listen: \"0.0.0.0:8081\"\ndb_path: scheduler.db\nworker_pool_size: 10\nretention_days: 30\nlog_level: info\nmissed_policy: skip\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Listen != "0.0.0.0:8082" || cfg.WorkerPoolSize != 7 || cfg.MissedPolicy != "latest" || cfg.APIToken != "tok-123" {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoadEnvBadInt(t *testing.T) {
	t.Setenv("SCHEDULER_WORKER_POOL_SIZE", "0x")
	if _, err := Load(write(t, "listen: \"0.0.0.0:8081\"\ndb_path: scheduler.db\nworker_pool_size: 10\nretention_days: 30\nlog_level: info\nmissed_policy: skip\n")); err == nil {
		t.Fatal("expected error for bad int")
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()
	c := defaults()
	tests := []struct {
		name   string
		mutate func(*Config)
		wantOK bool
	}{
		{"ok", func(*Config) {}, true},
		{"bad policy", func(c *Config) { c.MissedPolicy = "nope" }, false},
		{"bad pool", func(c *Config) { c.WorkerPoolSize = 0 }, false},
		{"bad retention", func(c *Config) { c.RetentionDays = 0 }, false},
		{"bad listen", func(c *Config) { c.Listen = "" }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c2 := c
			tt.mutate(&c2)
			if err := c2.validate(); (err == nil) != tt.wantOK {
				t.Errorf("validate() err=%v wantOK=%v", err, tt.wantOK)
			}
		})
	}
}

// Package store persists jobs and run history in SQLite. It targets
// modernc.org/sqlite so the build stays CGO-free; a single connection
// serialises writes and avoids SQLITE_BUSY deadlocks at this scale.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// DB wraps *sql.DB with scheduler-specific helpers.
type DB struct {
	*sql.DB
}

// Open opens (creating if needed) the database at path and applies
// pragmas and schema.
func Open(path string) (*DB, error) {
	d, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store open: %w", err)
	}
	// One connection: modernc sqlite serialises writes per connection;
	// scheduler throughput is small and consistency beats parallelism.
	d.SetMaxOpenConns(1)

	if err := applyPragmas(d); err != nil {
		d.Close()
		return nil, fmt.Errorf("store open: %w", err)
	}
	if _, err := d.Exec(schemaSQL); err != nil {
		d.Close()
		return nil, fmt.Errorf("store migrate: %w", err)
	}
	return &DB{DB: d}, nil
}

// applyPragmas sets durability and locking bits. WAL supports
// concurrent readers during scheduler writes.
func applyPragmas(d *sql.DB) error {
	for _, p := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := d.Exec(p); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

// Ping verifies the database is reachable; used by /healthz.
func (db *DB) Ping(ctx context.Context) error {
	return db.PingContext(ctx)
}

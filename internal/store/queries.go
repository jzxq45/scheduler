package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// ErrNotFound is returned by getters when no row matches.
var ErrNotFound = errors.New("not found")

// ErrDuplicate is returned by unique-constraint violations.
var ErrDuplicate = errors.New("duplicate name")

func ms(t time.Time) int64 { return t.UTC().UnixMilli() }

func ptrMS(t *time.Time) any {
	if t == nil {
		return nil
	}
	return ms(*t)
}

// Status labels of a run.
const (
	StatusRunning     = "running"
	StatusSuccess     = "success"
	StatusFailed      = "failed"
	StatusTimeout     = "timeout"
	StatusSkipped     = "skipped"
	StatusFailedFinal = "failed_final"
)

// Job mirrors one row of the jobs table.
type Job struct {
	ID             int64
	Name           string
	Schedule       string
	Command        string
	Payload        string
	Enabled        bool
	Timezone       string
	TimeoutSeconds int
	MaxRetries     int
	CreatedAt      time.Time
	UpdatedAt      time.Time
	LastRunAt      *time.Time
	NextRunAt      *time.Time
}

// Run mirrors one row of the runs table.
type Run struct {
	ID         int64
	JobID      int64
	StartedAt  time.Time
	FinishedAt *time.Time
	Status     string
	Output     []byte
	Error      string
	Attempt    int
}

const jobCols = `id, name, schedule, command, payload, enabled, timezone,
	timeout_seconds, max_retries, created_at, updated_at, last_run_at, next_run_at`

const runCols = `id, job_id, started_at, finished_at, status, output, error, attempt`

// isUniqueViolation reports whether err is a driver-reported UNIQUE
// constraint failure. Matching the driver's typed error and result
// code, not the message text, survives wording changes across driver
// versions — string sniffing would silently break duplicate detection.
func isUniqueViolation(err error) bool {
	var serr *sqlite.Error
	if !errors.As(err, &serr) {
		return false
	}
	return serr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

// CreateJob inserts j and returns the new id. Duplicate names fail.
func (db *DB) CreateJob(ctx context.Context, j *Job) (int64, error) {
	now := time.Now()
	j.CreatedAt, j.UpdatedAt = now, now
	en := 0
	if j.Enabled {
		en = 1
	}
	res, err := db.ExecContext(ctx,
		`INSERT INTO jobs (name, schedule, command, payload, enabled, timezone,
			timeout_seconds, max_retries, created_at, updated_at, last_run_at, next_run_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.Name, j.Schedule, j.Command, j.Payload, en, j.Timezone,
		j.TimeoutSeconds, j.MaxRetries, ms(now), ms(now), ptrMS(j.LastRunAt), ptrMS(j.NextRunAt))
	if err != nil {
		if isUniqueViolation(err) {
			return 0, fmt.Errorf("create job: %w", ErrDuplicate)
		}
		return 0, fmt.Errorf("create job: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("create job: %w", err)
	}
	return id, nil
}

// GetJob loads a job by id.
func (db *DB) GetJob(ctx context.Context, id int64) (Job, error) {
	row := db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id)
	return scanJob(row)
}

// GetJobByName loads a job by name.
func (db *DB) GetJobByName(ctx context.Context, name string) (Job, error) {
	row := db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE name = ?`, name)
	return scanJob(row)
}

// DefaultListJobs caps one page of the jobs API; clients page with
// limit/offset instead of unbounded result sets.
const DefaultListJobs = 100

// ListJobs returns one page of jobs ordered by name. limit <= 0 or
// above the cap falls back to the default (100), offset must be >= 0.
func (db *DB) ListJobs(ctx context.Context, limit, offset int) ([]Job, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := db.QueryContext(ctx,
		`SELECT `+jobCols+` FROM jobs ORDER BY name LIMIT ? OFFSET ?`,
		limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ListAllJobs returns every job: the scheduler's missed-run replay
// needs the full set, pagination there would silently drop jobs past
// the first page.
func (db *DB) ListAllJobs(ctx context.Context) ([]Job, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list all jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// DueJobs returns enabled jobs whose next_run_at <= now.
func (db *DB) DueJobs(ctx context.Context, now time.Time) ([]Job, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT `+jobCols+` FROM jobs WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?`,
		ms(now))
	if err != nil {
		return nil, fmt.Errorf("due jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

type rowScanner interface{ Scan(dest ...any) error }

func scanJob(r rowScanner) (Job, error) {
	var j Job
	var en int
	var created, updated int64
	var lastRun, nextRun sql.NullInt64
	err := r.Scan(&j.ID, &j.Name, &j.Schedule, &j.Command, &j.Payload, &en, &j.Timezone,
		&j.TimeoutSeconds, &j.MaxRetries, &created, &updated, &lastRun, &nextRun)
	if err == sql.ErrNoRows {
		return j, fmt.Errorf("job: %w", ErrNotFound)
	}
	if err != nil {
		return j, fmt.Errorf("scan job: %w", err)
	}
	j.Enabled = en == 1
	j.CreatedAt = time.UnixMilli(created).UTC()
	j.UpdatedAt = time.UnixMilli(updated).UTC()
	j.LastRunAt = nullTime(lastRun)
	j.NextRunAt = nullTime(nextRun)
	return j, nil
}

func nullTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := time.UnixMilli(v.Int64).UTC()
	return &t
}

// UpdateJob rewrites the mutable fields of j. last_run_at is NOT
// here on purpose: it is owned by the claim path (SetRunTimes /
// ClaimJob), and API clients never send it — writing it would wipe
// the run history pointer on every job edit.
func (db *DB) UpdateJob(ctx context.Context, j *Job) error {
	j.UpdatedAt = time.Now()
	en := 0
	if j.Enabled {
		en = 1
	}
	res, err := db.ExecContext(ctx,
		`UPDATE jobs SET name=?, schedule=?, command=?, payload=?, enabled=?, timezone=?,
		 timeout_seconds=?, max_retries=?, updated_at=?, next_run_at=? WHERE id=?`,
		j.Name, j.Schedule, j.Command, j.Payload, en, j.Timezone,
		j.TimeoutSeconds, j.MaxRetries, ms(j.UpdatedAt), ptrMS(j.NextRunAt), j.ID)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("update job: %w", ErrDuplicate)
		}
		return fmt.Errorf("update job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("update job: %w", ErrNotFound)
	}
	return nil
}

// DeleteJob removes a job; runs cascade.
func (db *DB) DeleteJob(ctx context.Context, id int64) error {
	res, err := db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("delete job: %w", ErrNotFound)
	}
	return nil
}

// ClaimJob atomically takes one schedule slot: it sets last_run_at to
// lastRun and rolls next_run_at to nextRun, but only when the stored
// next_run_at still equals expectNext. The compare-and-set closes two
// windows the audit flagged: two replays of the same tick (or a replay
// racing RunNow) in one process would double-claim, and two scheduler
// processes sharing one SQLite file would both execute the same job.
// It returns true when the claim was won; false means someone else
// moved next_run_at first (an error is reported only on DB failure).
func (db *DB) ClaimJob(ctx context.Context, id int64, expectNext *time.Time, lastRun, nextRun time.Time) (bool, error) {
	q, args := `UPDATE jobs SET last_run_at=?, next_run_at=? WHERE id=? AND `,
		[]any{ms(lastRun), ms(nextRun), id}
	if expectNext == nil {
		q += "next_run_at IS NULL"
	} else {
		q += "next_run_at=?"
		args = append(args, ms(*expectNext))
	}
	res, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		return false, fmt.Errorf("claim job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim job: %w", err)
	}
	return n == 1, nil
}

// SetRunTimes stores last_run_at and next_run_at after scheduling.
func (db *DB) SetRunTimes(ctx context.Context, id int64, lastRun, nextRun *time.Time) error {
	_, err := db.ExecContext(ctx,
		`UPDATE jobs SET last_run_at=?, next_run_at=? WHERE id=?`,
		ptrMS(lastRun), ptrMS(nextRun), id)
	if err != nil {
		return fmt.Errorf("set run times: %w", err)
	}
	return nil
}

// SetNextRun updates only next_run_at; used by missed-policy catchup.
func (db *DB) SetNextRun(ctx context.Context, id int64, next time.Time) error {
	_, err := db.ExecContext(ctx, `UPDATE jobs SET next_run_at=? WHERE id=?`, ms(next), id)
	if err != nil {
		return fmt.Errorf("set next run: %w", err)
	}
	return nil
}

// CreateRun starts a run and returns its id.
func (db *DB) CreateRun(ctx context.Context, jobID int64, started time.Time, attempt int) (int64, error) {
	res, err := db.ExecContext(ctx,
		`INSERT INTO runs (job_id, started_at, status, attempt) VALUES (?,?,?,?)`,
		jobID, ms(started), StatusRunning, attempt)
	if err != nil {
		return 0, fmt.Errorf("create run: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("create run: %w", err)
	}
	return id, nil
}

// FinishRun completes a run with status, output (≤64KB) and app error.
func (db *DB) FinishRun(ctx context.Context, runID int64, status string, finished time.Time, output []byte, errMsg string) error {
	if len(output) > 64*1024 {
		output = output[len(output)-64*1024:]
	}
	_, err := db.ExecContext(ctx,
		`UPDATE runs SET status=?, finished_at=?, output=?, error=? WHERE id=?`,
		status, ms(finished), nullBytes(output), errMsg, runID)
	if err != nil {
		return fmt.Errorf("finish run: %w", err)
	}
	return nil
}

// GetRun loads one run by id.
func (db *DB) GetRun(ctx context.Context, id int64) (Run, error) {
	row := db.QueryRowContext(ctx, `SELECT `+runCols+` FROM runs WHERE id = ?`, id)
	return scanRun(row)
}

// RunsForJob lists a job's runs, newest first.
func (db *DB) RunsForJob(ctx context.Context, jobID int64, limit int) ([]Run, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT `+runCols+` FROM runs WHERE job_id=? ORDER BY id DESC LIMIT ?`, jobID, limit)
	if err != nil {
		return nil, fmt.Errorf("runs for job: %w", err)
	}
	return collectRuns(rows)
}

// ListRuns lists all runs, newest first.
func (db *DB) ListRuns(ctx context.Context, limit int) ([]Run, error) {
	rows, err := db.QueryContext(ctx, `SELECT `+runCols+` FROM runs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return collectRuns(rows)
}

// RunningRuns lists run ids in status running whose jobs no longer
// execute (shutdown recovery).
func (db *DB) RunningRuns(ctx context.Context) ([]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT id FROM runs WHERE status='running'`)
	if err != nil {
		return nil, fmt.Errorf("running runs: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("running runs: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// MarkRunFinal flips one run to failed_final: retries for this
// chain are exhausted, the run is the dead-letter record.
func (db *DB) MarkRunFinal(ctx context.Context, runID int64) error {
	res, err := db.ExecContext(ctx,
		`UPDATE runs SET status=? WHERE id=? AND status IN (?,?)`,
		StatusFailedFinal, runID, StatusFailed, StatusTimeout)
	if err != nil {
		return fmt.Errorf("mark run final: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark run final: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("mark run final: %w", ErrNotFound)
	}
	return nil
}

// MarkOrphanedRuns closes runs still marked running after a restart:
// the worker pool that owned them is gone.
func (db *DB) MarkOrphanedRuns(ctx context.Context, now time.Time) (int64, error) {
	res, err := db.ExecContext(ctx,
		`UPDATE runs SET status=?, finished_at=?, error=? WHERE status=?`,
		StatusFailed, ms(now), "interrupted by scheduler restart", StatusRunning)
	if err != nil {
		return 0, fmt.Errorf("mark orphaned: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mark orphaned: %w", err)
	}
	return n, nil
}

func nullBytes(b []byte) any {
	if b == nil {
		return []byte{}
	}
	return b
}

func scanRun(r rowScanner) (Run, error) {
	var ru Run
	var started int64
	var finished sql.NullInt64
	var output []byte
	err := r.Scan(&ru.ID, &ru.JobID, &started, &finished, &ru.Status, &output, &ru.Error, &ru.Attempt)
	if err == sql.ErrNoRows {
		return ru, fmt.Errorf("run: %w", ErrNotFound)
	}
	if err != nil {
		return ru, fmt.Errorf("scan run: %w", err)
	}
	ru.StartedAt = time.UnixMilli(started).UTC()
	ru.FinishedAt = nullTime(finished)
	ru.Output = output
	return ru, nil
}

func collectRuns(rows *sql.Rows) ([]Run, error) {
	defer rows.Close()
	var out []Run
	for rows.Next() {
		ru, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ru)
	}
	return out, rows.Err()
}

// deleteBatch is how many rows one retention statement removes before
// looping. Unbounded single-statement deletes hold the one SQLite
// connection (and therefore the tick loop) for seconds on a large
// runs table; batches interleave with normal traffic.
const deleteBatch = 500

// DeleteRunsBefore removes finished runs older than the cutoff in
// batches and returns how many rows were affected.
func (db *DB) DeleteRunsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	cutMS := ms(cutoff)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("retention: %w", err)
		}
		res, err := db.ExecContext(ctx,
			`DELETE FROM runs WHERE id IN
			 (SELECT id FROM runs WHERE started_at < ? LIMIT ?)`, cutMS, deleteBatch)
		if err != nil {
			return total, fmt.Errorf("retention: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("retention: %w", err)
		}
		total += n
		if n < deleteBatch {
			return total, nil
		}
	}
}

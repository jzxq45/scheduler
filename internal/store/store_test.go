package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mkJob(name string) Job {
	return Job{
		Name:           name,
		Schedule:       "0 * * * *",
		Command:        "echo hi",
		Payload:        "{}",
		Enabled:        true,
		Timezone:       "UTC",
		TimeoutSeconds: 300,
		MaxRetries:     3,
	}
}

func TestJobCRUD(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)

	id, err := db.CreateJob(ctx, func() *Job { j := mkJob("a"); return &j }())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id <= 0 {
		t.Fatalf("id = %d", id)
	}

	got, err := db.GetJob(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "a" || !got.Enabled || got.MaxRetries != 3 || got.CreatedAt.IsZero() {
		t.Errorf("got %+v", got)
	}

	byName, err := db.GetJobByName(ctx, "a")
	if err != nil || byName.ID != id {
		t.Errorf("by name: %v %+v", err, byName)
	}

	list, err := db.ListJobs(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %d", err, len(list))
	}

	// Update
	got.Enabled = false
	got.MaxRetries = 5
	lr := time.UnixMilli(12345).UTC()
	nr := time.UnixMilli(54321).UTC()
	got.LastRunAt, got.NextRunAt = &lr, &nr
	if err := db.UpdateJob(ctx, &got); err != nil {
		t.Fatalf("update: %v", err)
	}
	u, err := db.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if u.Enabled || u.MaxRetries != 5 || u.LastRunAt == nil || u.NextRunAt == nil ||
		u.LastRunAt.UnixMilli() != 12345 {
		t.Errorf("updated: %+v", u)
	}

	// Delete runs nuke job and runs cascade.
	_, err = db.CreateRun(ctx, id, time.Now(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteJob(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.GetJob(ctx, id); err == nil {
		t.Error("job not deleted")
	}
	runs, err := db.RunsForJob(ctx, id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Errorf("cascade failed: %d runs", len(runs))
	}
}

func TestDuplicateName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)
	if _, err := db.CreateJob(ctx, func() *Job { j := mkJob("dup"); return &j }()); err != nil {
		t.Fatal(err)
	}
	_, err := db.CreateJob(ctx, func() *Job { j := mkJob("dup"); return &j }())
	if err == nil {
		t.Fatal("want duplicate error")
	}
}

func TestNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)
	if _, err := db.GetJob(ctx, 999); err == nil {
		t.Error("want not found")
	}
	if err := db.DeleteJob(ctx, 999); err == nil {
		t.Error("want not found")
	}
	j := mkJob("x")
	j.ID = 999
	if err := db.UpdateJob(ctx, &j); err == nil {
		t.Error("want not found")
	}
}

func TestDueJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)

	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	due := func() *Job {
		j := mkJob("due")
		en := time.UnixMilli(1)
		nr := time.UnixMilli(now.UnixMilli() - 1)
		j.LastRunAt = &en
		j.NextRunAt = &nr
		return &j
	}()
	if _, err := db.CreateJob(ctx, due); err != nil {
		t.Fatal(err)
	}
	later := func() *Job {
		j := mkJob("later")
		nr := time.UnixMilli(now.UnixMilli() + 100_000)
		j.NextRunAt = &nr
		return &j
	}()
	if _, err := db.CreateJob(ctx, later); err != nil {
		t.Fatal(err)
	}
	disabled := func() *Job {
		j := mkJob("disabled")
		nr := time.UnixMilli(now.UnixMilli() - 10)
		j.NextRunAt = &nr
		j.Enabled = false
		return &j
	}()
	if _, err := db.CreateJob(ctx, disabled); err != nil {
		t.Fatal(err)
	}
	noNext := mkJob("nonext")
	if _, err := db.CreateJob(ctx, &noNext); err != nil {
		t.Fatal(err)
	}

	jobs, err := db.DueJobs(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Name != "due" {
		t.Errorf("due = %+v", jobs)
	}
}

func TestRunsCRUDAndRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)
	id, err := db.CreateJob(ctx, func() *Job { j := mkJob("r"); return &j }())
	if err != nil {
		t.Fatal(err)
	}

	start := time.UnixMilli(1000).UTC()
	runID, err := db.CreateRun(ctx, id, start, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishRun(ctx, runID, StatusSuccess, time.UnixMilli(5000).UTC(), []byte("out"), ""); err != nil {
		t.Fatal(err)
	}
	ru, err := db.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if ru.Status != StatusSuccess || string(ru.Output) != "out" || ru.FinishedAt == nil {
		t.Errorf("run = %+v", ru)
	}

	run2, _ := db.CreateRun(ctx, id, time.UnixMilli(999_000_000).UTC(), 2)
	if err := db.FinishRun(ctx, run2, StatusFailed, time.UnixMilli(999_100_000).UTC(), nil, "boom"); err != nil {
		t.Fatal(err)
	}

	runs, err := db.RunsForJob(ctx, id, 5)
	if err != nil || len(runs) != 2 || runs[0].ID != run2 {
		t.Errorf("runs = %+v err=%v", runs, err)
	}
	all, err := db.ListRuns(ctx, 1)
	if err != nil || len(all) != 1 {
		t.Errorf("limit=1: %d %v", len(all), err)
	}

	// Big output is clamped to 64KB.
	big := make([]byte, 100*1024)
	if err := db.FinishRun(ctx, run2, StatusFailed, time.Now(), big, ""); err != nil {
		t.Fatal(err)
	}
	ru2, _ := db.GetRun(ctx, run2)
	if len(ru2.Output) > 64*1024 {
		t.Errorf("output not clamped: %d", len(ru2.Output))
	}

	// Retention
	old := time.UnixMilli(10).UTC()
	n, err := db.DeleteRunsBefore(ctx, time.UnixMilli(2000).UTC())
	if err != nil || n != 1 {
		t.Fatalf("retention n=%d err=%v", n, err)
	}
	_ = old
	remaining, _ := db.ListRuns(ctx, 10)
	if len(remaining) != 1 || remaining[0].ID != run2 {
		t.Errorf("remaining = %+v", remaining)
	}
}

func TestSetRunTimesAndDue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)
	id, err := db.CreateJob(ctx, func() *Job { j := mkJob("times"); return &j }())
	if err != nil {
		t.Fatal(err)
	}
	lr := time.UnixMilli(1).UTC()
	nr := time.UnixMilli(2).UTC()
	if err := db.SetRunTimes(ctx, id, &lr, &nr); err != nil {
		t.Fatal(err)
	}
	if err := db.SetNextRun(ctx, id, time.UnixMilli(999_999).UTC()); err != nil {
		t.Fatal(err)
	}
	j, err := db.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.NextRunAt.UnixMilli() != 999_999 || j.LastRunAt.UnixMilli() != 1 {
		t.Errorf("times: %+v", j)
	}
	if _, err := db.DueJobs(ctx, time.UnixMilli(1).UTC()); err != nil {
		t.Errorf("due with old time: %v", err)
	}
}

func TestMarkOrphanedRuns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTest(t)
	id, _ := db.CreateJob(ctx, func() *Job { j := mkJob("orphan"); return &j }())
	runID, _ := db.CreateRun(ctx, id, time.Now(), 1)
	now := time.Now()
	n, err := db.MarkOrphanedRuns(ctx, now)
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	ru, _ := db.GetRun(ctx, runID)
	if ru.Status != StatusFailed || ru.FinishedAt == nil || ru.Error == "" {
		t.Errorf("orphan run: %+v", ru)
	}
}

func TestPragmasAndPersistence(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "persist.db")
	ctx := context.Background()
	db1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db1.CreateJob(ctx, func() *Job { j := mkJob("persist"); return &j }()); err != nil {
		t.Fatal(err)
	}
	db1.Close()

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.GetJobByName(ctx, "persist"); err != nil {
		t.Errorf("reopen: %v", err)
	}
	var mode string
	if err := db2.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Error(err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q", mode)
	}
}

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"scheduler/internal/store"
)

// starter records RunNow calls and fails unknown jobs.
type starter struct {
	calls int64
	err   error
}

func (st *starter) RunNow(_ context.Context, id int64) error {
	st.calls = id
	if id == 404 {
		return fmt.Errorf("run now: job: %w", store.ErrNotFound)
	}
	return st.err
}

func testServer(t *testing.T) (*Server, *store.DB, *starter) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	st := &starter{}
	s := New(db, st, nil, nil)
	return s, db, st
}

func do(t *testing.T, s *Server, method, path string, body any) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var rd *bytes.Reader
	switch v := body.(type) {
	case nil:
		rd = bytes.NewReader(nil)
	case []byte:
		rd = bytes.NewReader(v)
	case string:
		rd = bytes.NewReader([]byte(v))
	default:
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rd)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

func jobBody(name, schedule string, enabled bool) JobDTO {
	en := enabled
	return JobDTO{
		Name: name, Schedule: schedule, Command: "true " + name,
		Enabled: &en, Timezone: "UTC", TimeoutSeconds: 60, MaxRetries: 1,
	}
}

func TestJobCRUD(t *testing.T) {
	s, db, _ := testServer(t)

	// create
	rec, body := do(t, s, "POST", "/api/jobs", jobBody("alpha", "* * * * *", true))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, body)
	}
	var d JobDTO
	if err := json.Unmarshal(body, &d); err != nil {
		t.Fatal(err)
	}
	if d.ID == 0 || d.Enabled == nil || !*d.Enabled || d.NextRunAt == "" {
		t.Fatalf("created job = %+v", d)
	}

	// duplicate name → 409
	rec, _ = do(t, s, "POST", "/api/jobs", jobBody("alpha", "* * * * *", true))
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d", rec.Code)
	}

	// list
	rec, body = do(t, s, "GET", "/api/jobs", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d", rec.Code)
	}
	var list []JobDTO
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("list len = %d", len(list))
	}

	// get
	rec, body = do(t, s, "GET", fmt.Sprintf("/api/jobs/%d", d.ID), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d", rec.Code)
	}

	// update → disabled
	disabled := jobBody("alpha", "0 12 * * *", false)
	rec, body = do(t, s, "PUT", fmt.Sprintf("/api/jobs/%d", d.ID), disabled)
	if rec.Code != http.StatusOK {
		t.Fatalf("update: %d %s", rec.Code, body)
	}
	j, err := db.GetJob(context.Background(), d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.Enabled || j.Schedule != "0 12 * * *" {
		t.Fatalf("after update: %+v", j)
	}

	// delete
	rec, _ = do(t, s, "DELETE", fmt.Sprintf("/api/jobs/%d", d.ID), nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	rec, _ = do(t, s, "GET", fmt.Sprintf("/api/jobs/%d", d.ID), nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get after delete: %d", rec.Code)
	}
}

func TestValidation(t *testing.T) {
	s, _, _ := testServer(t)
	cases := []struct {
		name string
		dto  JobDTO
	}{
		{"no name", jobBody("", "* * * * *", true)},
		{"no command", JobDTO{Name: "x", Schedule: "* * * * *", Enabled: boolPtr(true)}},
		{"bad schedule", jobBody("x", "not-a-cron", true)},
		{"bad timezone", jobBody("x", "* * * * *", true)},
	}
	for _, c := range cases {
		if c.name == "bad timezone" {
			c.dto.Timezone = "Mars/Olympus"
		}
		rec, _ := do(t, s, "POST", "/api/jobs", c.dto)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: want 400, got %d", c.name, rec.Code)
		}
	}
}

func boolPtr(b bool) *bool { return &b }

func TestRunNowEndpoint(t *testing.T) {
	s, db, st := testServer(t)
	id, err := db.CreateJob(context.Background(), &store.Job{
		Name: "manual", Schedule: "0 0 1 1 *", Command: "x", Enabled: true,
		Timezone: "UTC", TimeoutSeconds: 60,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := do(t, s, "POST", "/api/jobs/404/run", nil)
	if rec.Code != http.StatusConflict { // starter present, but job missing → RunNow error
		t.Fatalf("unknown job: %d", rec.Code)
	}
	rec, _ = do(t, s, "POST", fmt.Sprintf("/api/jobs/%d/run", id), nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("run now: %d", rec.Code)
	}
	if st.calls != id {
		t.Fatalf("starter got %d, want %d", st.calls, id)
	}
}

func TestRunHistory(t *testing.T) {
	s, db, _ := testServer(t)
	ctx := context.Background()
	id, err := db.CreateJob(ctx, &store.Job{
		Name: "hist", Schedule: "* * * * *", Command: "x", Enabled: true,
		Timezone: "UTC", TimeoutSeconds: 60,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := 0; i < 3; i++ {
		rid, err := db.CreateRun(ctx, id, now, 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.FinishRun(ctx, rid, store.StatusSuccess, now, []byte("hi"), ""); err != nil {
			t.Fatal(err)
		}
	}
	rec, body := do(t, s, "GET", fmt.Sprintf("/api/jobs/%d/runs", id), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("runs: %d", rec.Code)
	}
	var runs []RunDTO
	if err := json.Unmarshal(body, &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 || runs[0].Status != store.StatusSuccess || runs[0].Output != "hi" {
		t.Fatalf("runs = %+v", runs)
	}
}

func TestSSEStream(t *testing.T) {
	s, _, _ := testServer(t)
	b := s.Broker()

	srv := httptest.NewServer(s)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Pull one event after publishing.
	go b.Publish(Event{Type: "run_finished", Job: "x", Status: "success"})

	buf := make([]byte, 512)
	// Read until the data line arrives.
	deadline := time.Now().Add(2 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		n, err := resp.Body.Read(buf)
		got += string(buf[:n])
		if err != nil || bytes.Contains([]byte(got), []byte("run_finished")) {
			break
		}
	}
	if !bytes.Contains([]byte(got), []byte("run_finished")) {
		t.Fatalf("no SSE event, read %q", got)
	}

	// Late subscriber notices unregistration by not being published
	// after cancel (implicit — no deadlock).
}

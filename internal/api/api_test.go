package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"scheduler/internal/store"
)

// starter records RunNow/StopJob calls and fails unknown jobs.
type starter struct {
	mu    sync.Mutex
	calls int64
	err   error
	stops []int64
}

func (st *starter) RunNow(_ context.Context, id int64) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.calls = id
	if id == 404 {
		return fmt.Errorf("run now: job: %w", store.ErrNotFound)
	}
	return st.err
}

func (st *starter) StopJob(id int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.stops = append(st.stops, id)
}

func (st *starter) count() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.calls
}

func (st *starter) stopList() []int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make([]int64, len(st.stops))
	copy(out, st.stops)
	return out
}

func testServer(t *testing.T) (*Server, *store.DB, *starter) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	st := &starter{}
	s := New(db, st, nil, nil, "")
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

func TestBearerAuth(t *testing.T) {
	s, _, _ := testServer(t)
	_ = s // token server created below via NewWithToken helper

	dbPath := filepath.Join(t.TempDir(), "auth.db")
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	st := &starter{}
	ts := New(db, st, nil, nil, "s3cret")

	// Missing token → 401 everywhere.
	rec, body := do(t, ts, "GET", "/api/jobs", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d %s", rec.Code, body)
	}

	// Wrong token → 401.
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(`{"name":"x","schedule":"* * * * *","command":"c"}`))
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	ts.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", rec.Code)
	}

	// Correct header token → accepted.
	req = httptest.NewRequest("POST", "/api/jobs", strings.NewReader(`{"name":"x","schedule":"* * * * *","command":"c","timeout_seconds":60}`))
	req.Header.Set("Authorization", "Bearer s3cret")
	rec = httptest.NewRecorder()
	ts.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("bearer header: %d %s", rec.Code, rec.Body.String())
	}

	// Query parameter token accepted (EventSource cannot set headers).
	rec, _ = do(t, ts, "GET", "/api/jobs?token=s3cret", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("query token: %d", rec.Code)
	}

	// Empty token (auth disabled) → open access.
	rec, _ = do(t, s, "GET", "/api/jobs", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("auth disabled: %d", rec.Code)
	}
}

// TestUIRenderSmoke guards the dashboard template: it must render and
// ship the quote-escaping esc() so job names cannot break HTML
// attributes (stored-XSS fix).
func TestUIRenderSmoke(t *testing.T) {
	s, _, _ := testServer(t)
	rec, body := do(t, s, "GET", "/", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("ui: %d", rec.Code)
	}
	html := string(body)
	if !strings.Contains(html, `&quot;`) {
		t.Errorf("esc() does not escape double quotes")
	}
	if !strings.Contains(html, `&#39;`) {
		t.Errorf("esc() does not escape single quotes")
	}
	if !strings.Contains(html, `data-name="${esc(j.name)}"`) {
		t.Errorf("attribute injection guard missing")
	}
}

// A panicking handler must become one 500 response, not a crashed
// process.
func TestHTTPRecovery(t *testing.T) {
	s, _, _ := testServer(t)
	s.mux.HandleFunc("GET /api/boom", func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	rec, body := do(t, s, "GET", "/api/boom", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("panic handler: %d %s", rec.Code, body)
	}
}

// The /run endpoint is throttled per client IP: 5 manual triggers per
// minute, then 429 with Retry-After.
func TestRunRateLimited(t *testing.T) {
	s, _, _ := testServer(t)
	rec, body := do(t, s, "POST", "/api/jobs", jobBody("rl", "* * * * *", true))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, body)
	}
	var d JobDTO
	if err := json.Unmarshal(body, &d); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < defaultRunRatePerMin; i++ {
		rec, _ := do(t, s, "POST", fmt.Sprintf("/api/jobs/%d/run", d.ID), nil)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("run %d: %d", i+1, rec.Code)
		}
	}
	rec, _ = do(t, s, "POST", fmt.Sprintf("/api/jobs/%d/run", d.ID), nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over-limit run: %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Errorf("missing Retry-After on 429")
	}
	// The limit is per IP: another address still gets through.
	req := httptest.NewRequest("POST", fmt.Sprintf("/api/jobs/%d/run", d.ID), nil)
	req.RemoteAddr = "10.9.8.7:4321"
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("other IP: %d", rec.Code)
	}
}

// Security headers must be present on every response.
func TestSecurityHeaders(t *testing.T) {
	s, _, _ := testServer(t)
	rec, _ := do(t, s, "GET", "/api/jobs", nil)
	for hdr, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "strict-origin-when-cross-origin",
	} {
		if got := rec.Header().Get(hdr); got != want {
			t.Errorf("%s = %q, want %q", hdr, got, want)
		}
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
	rec, _ = do(t, s, "GET", fmt.Sprintf("/api/jobs/%d", d.ID), nil)
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

// TestOversizedBodyRejected keeps blob uploads from OOM-ing the
// process: JSON bodies are capped at 1 MiB.
func TestOversizedBodyRejected(t *testing.T) {
	s, _, _ := testServer(t)
	big := strings.Repeat("x", 2<<20) // 2 MiB, over the cap
	rec, _ := do(t, s, "POST", "/api/jobs", big)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversize body: %d", rec.Code)
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

	// Range validation.
	ranges := []struct {
		name string
		dto  JobDTO
	}{
		{"timeout zero", jobBody("x", "* * * * *", true)},
		{"huge retries", jobBody("x", "* * * * *", true)},
	}
	ranges[0].dto.TimeoutSeconds = 0
	ranges[1].dto.MaxRetries = 33
	for _, c := range ranges {
		rec, _ := do(t, s, "POST", "/api/jobs", c.dto)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: want 400, got %d", c.name, rec.Code)
		}
	}
}

func boolPtr(b bool) *bool { return &b }

func TestHealthz(t *testing.T) {
	s, db, _ := testServer(t)
	rec, _ := do(t, s, "GET", "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	// A store that cannot answer must fail health.
	_ = db.Close()
	rec, _ = do(t, s, "GET", "/healthz", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed store healthz: %d", rec.Code)
	}
}

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
	if got, want := st.count(), id; got != want {
		t.Fatalf("starter got %d, want %d", got, want)
	}
}

// TestDeleteStopsExecutions: DELETE must cancel the running run and
// armed retries (no ghost executions of a dead job's commands). An
// edit that disables the job must stop in-flight work the same way.
func TestDeleteStopsExecutions(t *testing.T) {
	s, db, st := testServer(t)
	ctx := context.Background()
	id, err := db.CreateJob(ctx, &store.Job{
		Name: "ghost", Schedule: "* * * * *", Command: "x", Enabled: true,
		Timezone: "UTC", TimeoutSeconds: 60,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := do(t, s, "DELETE", fmt.Sprintf("/api/jobs/%d", id), nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body.String())
	}
	if stops := st.stopList(); len(stops) != 1 || stops[0] != id {
		t.Fatalf("stops = %v, want [%d]", stops, id)
	}

	// Disabling runs the same stop path.
	id2, err := db.CreateJob(ctx, &store.Job{
		Name: "drowse", Schedule: "* * * * *", Command: "x", Enabled: true,
		Timezone: "UTC", TimeoutSeconds: 60,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	body := jobBody("drowse", "* * * * *", false)
	rec, _ = do(t, s, "PUT", fmt.Sprintf("/api/jobs/%d", id2), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("disable: %d %s", rec.Code, rec.Body.String())
	}
	stops := st.stopList()
	if len(stops) != 2 || stops[1] != id2 {
		t.Fatalf("stops = %v, want second %d", stops, id2)
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

// TestSSESubscriberCap: the broker refuses subscribers past its cap
// so long-lived streams cannot exhaust resources; freed slots are
// reusable.
func TestSSESubscriberCap(t *testing.T) {
	b := NewBroker()
	b.setMax(2)

	ch1, cancel1, err := b.Subscribe()
	if err != nil || ch1 == nil {
		t.Fatalf("first: %v", err)
	}
	if _, cancel2, err := b.Subscribe(); err != nil {
		t.Fatalf("second: %v", err)
	} else {
		defer cancel2()
	}
	if _, _, err := b.Subscribe(); err == nil {
		t.Fatal("third subscriber must fail")
	}

	// Cancelling frees the slot.
	cancel1()
	if _, cancel3, err := b.Subscribe(); err != nil {
		t.Fatalf("after cancel: %v", err)
	} else {
		cancel3()
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

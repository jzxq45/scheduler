// Package api exposes the scheduler over HTTP: a JSON CRUD API for
// jobs, a "run now" trigger, run history and an SSE stream of live
// run events. A single-page UI (ui.go) sits on top of the same API.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"scheduler/internal/cronexpr"
	"scheduler/internal/store"
)

// RunStarter is the subset of the scheduler the API needs.
type RunStarter interface {
	RunNow(ctx context.Context, jobID int64) error
	// StopJob aborts a running execution and armed retries for the
	// job. DELETE and disabling must call it: nothing should keep
	// executing shell commands for a job the operator just killed.
	StopJob(jobID int64)
}

// Server serves the API and the web UI.
type Server struct {
	db     *store.DB
	sched  RunStarter
	log    *slog.Logger
	broker *Broker
	mux    *http.ServeMux
	// token, when non-empty, is required via
	// "Authorization: Bearer <token>" (or "?token=" for EventSource,
	// which cannot set custom headers).
	token string
	// runLimit throttles manual "run now" triggers per client IP.
	runLimit *rateLimiter
	// now is the handler time source, so the project rule "all time
	// flows through an injectable clock" covers the API too.
	now func() time.Time
	// trustProxy, when set, takes the rate-limit key from
	// X-Forwarded-For instead of the socket address: behind a reverse
	// proxy every client otherwise shares the single loopback bucket.
	trustProxy bool
	// onJobDelete drops job-scoped registry state (metrics series) for
	// deleted jobs so label churn cannot grow those maps forever.
	onJobDelete func(name string)
}

// statusRecorder captures the response code for the access log while
// forwarding everything else (including Flush for SSE) untouched.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// New builds the server; RunStarter may be nil for API-only setups,
// in which case "run now" answers 503. A pre-made broker lets main
// share one hub between the scheduler Reporter and the server. An
// empty apiToken disables authentication.
func New(db *store.DB, sched RunStarter, log *slog.Logger, b *Broker, apiToken string) *Server {
	if b == nil {
		b = NewBroker()
	}
	s := &Server{
		db:       db,
		sched:    sched,
		log:      log,
		broker:   b,
		token:    apiToken,
		runLimit: newRateLimiter(defaultRunRatePerMin, time.Minute),
		now:      time.Now,
	}
	if log == nil {
		s.log = slog.Default()
	}
	s.routes()
	return s
}

// ServeHTTP authenticates (when a token is configured), sets baseline
// security headers, and recovers panics before dispatching.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Baseline hardening for every response, UI and API alike: no
	// MIME sniffing, no framing, no referrer leakage.
	h := w.Header()
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "strict-origin-when-cross-origin")
	// A panic in a handler must cost one 500, not the whole process:
	// this API runs arbitrary shell commands and is exposed to
	// operators' scripts; the goroutine crash would be fatal to the
	// scheduler process itself.
	defer func() {
		if rec := recover(); rec != nil {
			s.log.Error("http handler panic",
				"method", r.Method, "path", r.URL.Path,
				"panic", rec, "stack", string(debug.Stack()))
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()
	if err := s.authorize(r); err != nil {
		httpError(w, http.StatusUnauthorized, err.Error())
		return
	}
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	s.mux.ServeHTTP(rec, r)
	// One line per request: without it a stray 500 is undebuggable.
	// SSE connections log once, at disconnect, with their full lifetime.
	s.log.Info("http",
		"method", r.Method, "path", r.URL.Path, "status", rec.status,
		"dur", time.Since(start).Round(time.Millisecond), "ip", s.clientIP(r))
}

// healthz reports process liveness plus a live store check. A pod that
// lost its database must fail readiness, or probes would keep stale
// instances in rotation.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if err := s.db.PingContext(r.Context()); err != nil {
		s.log.Error("healthz", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// authorize verifies the bearer token, if one is configured, in
// constant time: a plain == leaks length/prefix timing to a local
// attacker looping requests.
func (s *Server) authorize(r *http.Request) error {
	if s.token == "" {
		return nil
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.token)) == 1 {
		return nil
	}
	// EventSource cannot attach headers; keep the query-param channel.
	if subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("token")), []byte(s.token)) == 1 {
		return nil
	}
	return fmt.Errorf("unauthorized: missing or invalid API token")
}

// Broker exposes the SSE hub so the scheduler Reporter can publish.
func (s *Server) Broker() *Broker { return s.broker }

// SetTrustedProxy toggles trusting X-Forwarded-For for rate-limit
// keys. Must be called before serving; false (default) never trusts
// client-supplied headers.
func (s *Server) SetTrustedProxy(v bool) { s.trustProxy = v }

// SetJobCleanup installs a hook invoked with the deleted job's name,
// so external registries (Prometheus exposition) can drop their
// per-job series. Must be called before serving.
func (s *Server) SetJobCleanup(fn func(name string)) { s.onJobDelete = fn }

// MetricsHandler registers an extra GET route (used for /metrics).
// Must be called before the server starts serving.
func (s *Server) MetricsHandler(h http.Handler) {
	s.mux.Handle("GET /metrics", h)
}

func (s *Server) routes() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.ui)
	// Liveness/readiness for load balancers and container probes:
	// DB must answer, not just the process.
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /api/jobs", s.createJob)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("PUT /api/jobs/{id}", s.updateJob)
	mux.HandleFunc("DELETE /api/jobs/{id}", s.deleteJob)
	mux.HandleFunc("POST /api/jobs/{id}/run", s.runJob)
	mux.HandleFunc("GET /api/jobs/{id}/runs", s.listRuns)
	mux.HandleFunc("GET /events", s.events)
	s.mux = mux
}

// ---- JSON DTOs ----

// JobDTO is the wire form of a job; times are RFC3339 UTC strings.
// Enabled is a pointer so "absent" (start enabled) differs from
// explicit false.
type JobDTO struct {
	ID             int64  `json:"id"`
	Name           string `json:"name"`
	Schedule       string `json:"schedule"`
	Command        string `json:"command"`
	Payload        string `json:"payload,omitempty"`
	Enabled        *bool  `json:"enabled,omitempty"`
	Timezone       string `json:"timezone"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	MaxRetries     int    `json:"max_retries"`
	LastRunAt      string `json:"last_run_at,omitempty"`
	NextRunAt      string `json:"next_run_at,omitempty"`
}

func toDTO(j store.Job) JobDTO {
	d := JobDTO{
		ID: j.ID, Name: j.Name, Schedule: j.Schedule,
		Command: j.Command, Payload: j.Payload,
		Enabled: &j.Enabled, Timezone: j.Timezone,
		TimeoutSeconds: j.TimeoutSeconds, MaxRetries: j.MaxRetries,
	}
	if j.LastRunAt != nil {
		d.LastRunAt = j.LastRunAt.UTC().Format(time.RFC3339)
	}
	if j.NextRunAt != nil {
		d.NextRunAt = j.NextRunAt.UTC().Format(time.RFC3339)
	}
	return d
}

// RunDTO is the wire form of a run; output is trimmed.
type RunDTO struct {
	ID         int64  `json:"id"`
	JobID      int64  `json:"job_id"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
	Status     string `json:"status"`
	Output     string `json:"output,omitempty"`
	Error      string `json:"error,omitempty"`
	Attempt    int    `json:"attempt"`
}

// maxListOutput caps the output included in run listings. A run row
// can hold up to 64 KiB; a page of 500 untruncated outputs answers
// with 32 MB per request and holds the single SQLite connection while
// serialising it — starving the tick loop. Full output still lives in
// the database; the listing only shows the head with a truncation
// marker.
const maxListOutput = 4096

func truncateOutput(b []byte) string {
	if len(b) <= maxListOutput {
		return string(b)
	}
	return fmt.Sprintf("%s… (%d bytes total, truncated)",
		string(b[:maxListOutput]), len(b))
}

func runDTO(r store.Run) RunDTO {
	d := RunDTO{
		ID: r.ID, JobID: r.JobID, Status: r.Status,
		StartedAt: r.StartedAt.UTC().Format(time.RFC3339),
		Error:     r.Error, Attempt: r.Attempt,
		Output: truncateOutput(r.Output),
	}
	if r.FinishedAt != nil {
		d.FinishedAt = r.FinishedAt.UTC().Format(time.RFC3339)
	}
	return d
}

// ---- handlers ----

// maxJobBody bounds the JSON payload accepted from API clients
// (unfinished payloads aside, jobs are tiny).
const maxJobBody = 1 << 20

// Range caps for per-job fields; reject nonsense outright instead of
// letting the worker pool guess.
const (
	maxTimeoutSeconds = 24 * 60 * 60
	maxRetriesAllowed = 32
)

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxJobBody)
	var d JobDTO
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		httpError(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	j, err := s.buildJob(d)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := s.db.CreateJob(r.Context(), &j)
	if err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			httpError(w, http.StatusConflict, "job name already exists")
		} else {
			httpError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	s.broker.Publish(Event{Type: "job_created", Job: j.Name, At: s.now()})

	created, err := s.db.GetJob(r.Context(), id)
	if err != nil {
		// Failure reading back what we just wrote is a 500, not a
		// silent zero-value DTO with a meaningless id.
		s.log.Error("reload created job", "job", j.Name, "err", err)
		httpError(w, http.StatusInternalServerError, "job created but reload failed")
		return
	}
	writeJSON(w, http.StatusCreated, toDTO(created))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	// Pagination: ?limit=&offset=; the default caps one page at 100
	// jobs so a huge fleet cannot return unbounded payloads.
	limit, offset := store.DefaultListJobs, 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(n, 500)
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	jobs, err := s.db.ListJobs(r.Context(), limit, offset)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]JobDTO, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toDTO(j))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := s.db.GetJob(r.Context(), id)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toDTO(j))
}

func (s *Server) updateJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxJobBody)
	var d JobDTO
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		httpError(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	j, err := s.buildJob(d)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	j.ID = id
	if err := s.db.UpdateJob(r.Context(), &j); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			httpError(w, http.StatusConflict, "job name already exists")
		} else if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, err.Error())
		} else {
			httpError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	s.broker.Publish(Event{Type: "job_updated", Job: j.Name, At: s.now()})

	// Disabling must not leave a running execution or armed retries.
	if !j.Enabled && s.sched != nil {
		s.sched.StopJob(id)
	}
	writeJSON(w, http.StatusOK, toDTO(j))
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := s.db.GetJob(r.Context(), id)
	if err != nil {
		httpError(w, http.StatusNotFound, err.Error())
		return
	}
	if err := s.db.DeleteJob(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, err.Error())
		} else {
			s.log.Error("delete job", "job", j.Name, "err", err)
			httpError(w, http.StatusInternalServerError, "delete failed")
		}
		return
	}
	// No ghost executions: a running run and armed retries stop here.
	if s.sched != nil {
		s.sched.StopJob(id)
	}
	if s.onJobDelete != nil {
		s.onJobDelete(j.Name)
	}
	s.broker.Publish(Event{Type: "job_deleted", Job: j.Name, At: s.now()})

	w.WriteHeader(http.StatusNoContent)
}

// defaultRunRatePerMin caps manual triggers per client IP per minute:
// "run now" bypasses the schedule and lands on the worker pool; an
// unbounded endpoint lets one noisy consumer starve everyone else.
const defaultRunRatePerMin = 5

// rateLimiter is a minimal in-memory sliding-window limiter. It is
// deliberately per-process state: a restart resets counts, which is
// acceptable for throttling abuse, not for quota accounting.
type rateLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	hits   map[string][]time.Time
	lastGC time.Time
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, hits: map[string][]time.Time{}, lastGC: time.Now()}
}

// allow reports whether key gets a slot in the current window.
func (r *rateLimiter) allow(key string) bool {
	now := time.Now()
	cut := now.Add(-r.window)
	r.mu.Lock()
	defer r.mu.Unlock()
	// Prune long-idle keys opportunistically so the map cannot grow
	// with unique attacker addresses forever.
	if now.Sub(r.lastGC) >= r.window {
		for k, hh := range r.hits {
			if len(hh) == 0 || hh[len(hh)-1].Before(cut) {
				delete(r.hits, k)
			}
		}
		r.lastGC = now
	}
	kept := r.hits[key][:0]
	for _, h := range r.hits[key] {
		if h.After(cut) {
			kept = append(kept, h)
		}
	}
	if len(kept) >= r.limit {
		r.hits[key] = kept
		return false
	}
	r.hits[key] = append(kept, now)
	return true
}

// clientIP reduces the request to a rate-limit key. With trustProxy
// set, the first X-Forwarded-For entry wins (a reverse proxy is in
// front); without it the socket address is used — XFF is trivially
// spoofable by direct clients, so it must not be trusted by default.
func (s *Server) clientIP(r *http.Request) string {
	if s.trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			ip, _, _ := strings.Cut(xff, ",")
			ip = strings.TrimSpace(ip)
			if ip != "" {
				return ip
			}
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	if r.RemoteAddr == "" {
		return "local"
	}
	return r.RemoteAddr
}

func (s *Server) runJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if !s.runLimit.allow(s.clientIP(r)) {
		w.Header().Set("Retry-After", "30")
		httpError(w, http.StatusTooManyRequests, "too many run requests; slow down")
		return
	}
	if s.sched == nil {
		httpError(w, http.StatusServiceUnavailable, "scheduler not attached")
		return
	}
	if err := s.sched.RunNow(r.Context(), id); err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	s.broker.Publish(Event{Type: "run_triggered", Job: strconv.FormatInt(id, 10), At: s.now()})

	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	runs, err := s.db.RunsForJob(r.Context(), id, limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]RunDTO, 0, len(runs))
	for _, ru := range runs {
		out = append(out, runDTO(ru))
	}
	writeJSON(w, http.StatusOK, out)
}

// events streams live run activity as Server-Sent Events. It caps the
// number of concurrent subscribers and exits as soon as the client
// stops reading (write error or context done).
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	events, cancel, err := s.broker.Subscribe()
	if err != nil {
		// 503 with Retry-After: the stream is a convenience, not a
		// contract, and reconnecting browsers will retry anyway.
		w.Header().Set("Retry-After", "10")
		httpError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	// Heartbeats hold proxies open; direct flushes per event.
	beat := time.NewTicker(20 * time.Second)
	defer beat.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-events:
			data, err := json.Marshal(e)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				// Client is gone: stop streaming, free the slot.
				return
			}
			fl.Flush()
		case <-beat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

// ---- helpers ----

// buildJob validates the DTO into a store.Job and computes the first
// next_run_at from the server clock (the Schedule regexp parser runs
// here exactly once — handlers previously parsed it two or three
// times through buildJob + nextRun).
func (s *Server) buildJob(d JobDTO) (store.Job, error) {
	j := store.Job{
		Name: d.Name, Schedule: d.Schedule, Command: d.Command,
		Payload: d.Payload, Timezone: d.Timezone,
		TimeoutSeconds: d.TimeoutSeconds, MaxRetries: d.MaxRetries,
	}
	j.Enabled = d.Enabled == nil || *d.Enabled
	if j.Name == "" {
		return j, fmt.Errorf("name is required")
	}
	if j.Command == "" {
		return j, fmt.Errorf("command is required")
	}
	if j.TimeoutSeconds < 1 || j.TimeoutSeconds > maxTimeoutSeconds {
		return j, fmt.Errorf("timeout_seconds must be 1..%d, got %d", maxTimeoutSeconds, j.TimeoutSeconds)
	}
	if j.MaxRetries < 0 || j.MaxRetries > maxRetriesAllowed {
		return j, fmt.Errorf("max_retries must be 0..%d, got %d", maxRetriesAllowed, j.MaxRetries)
	}
	e, err := cronexpr.Parse(j.Schedule)
	if err != nil {
		return j, fmt.Errorf("schedule: %w", err)
	}
	if j.Timezone == "" {
		j.Timezone = "UTC"
	} else if _, err := time.LoadLocation(j.Timezone); err != nil {
		return j, fmt.Errorf("timezone: %w", err)
	}
	next := e.NextIn(s.now(), loadLocOr(j.Timezone))
	j.NextRunAt = &next
	return j, nil
}

// loadLocOr resolves a timezone name, falling back to UTC: callers
// validate the name before this helper; the fallback only covers
// tests that skip validation.
func loadLocOr(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC
	}
	return loc
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		httpError(w, http.StatusBadRequest, "bad id")
		return 0, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// httpError writes a JSON error body. 4xx carry the reason (client
// can act on it); 5xx carry a generic message — internal details such
// as raw SQLite errors must not leak to the client. Callers that need
// the specifics log them separately.
func httpError(w http.ResponseWriter, code int, msg string) {
	if code >= 500 {
		writeJSON(w, code, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, code, map[string]string{"error": msg})
}

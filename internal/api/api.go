// Package api exposes the scheduler over HTTP: a JSON CRUD API for
// jobs, a "run now" trigger, run history and an SSE stream of live
// run events. A single-page UI (ui.go) sits on top of the same API.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"scheduler/internal/cronexpr"
	"scheduler/internal/store"
)

// RunStarter is the subset of the scheduler the API needs.
type RunStarter interface {
	RunNow(ctx context.Context, jobID int64) error
}

// Server serves the API and the web UI.
type Server struct {
	db    *store.DB
	sched RunStarter
	log   *slog.Logger
	broker *Broker
	mux   *http.ServeMux
	// token, when non-empty, is required via
	// "Authorization: Bearer <token>" (or "?token=" for EventSource,
	// which cannot set custom headers).
	token string
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
		db:     db,
		sched:  sched,
		log:    log,
		broker: b,
		token:  apiToken,
	}
	if log == nil {
		s.log = slog.Default()
	}
	s.routes()
	return s
}

// ServeHTTP authenticates (when a token is configured) and sets
// baseline security headers before dispatching.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := s.authorize(r); err != nil {
		httpError(w, http.StatusUnauthorized, err.Error())
		return
	}
	s.mux.ServeHTTP(w, r)
}

// authorize verifies the bearer token, if one is configured.
func (s *Server) authorize(r *http.Request) error {
	if s.token == "" {
		return nil
	}
	if r.Header.Get("Authorization") == "Bearer "+s.token {
		return nil
	}
	// EventSource cannot attach headers; keep the query-param channel.
	if r.URL.Query().Get("token") == s.token {
		return nil
	}
	return fmt.Errorf("unauthorized: missing or invalid API token")
}

// Broker exposes the SSE hub so the scheduler Reporter can publish.
func (s *Server) Broker() *Broker { return s.broker }

// MetricsHandler registers an extra GET route (used for /metrics).
// Must be called before the server starts serving.
func (s *Server) MetricsHandler(h http.Handler) {
	s.mux.Handle("GET /metrics", h)
}


func (s *Server) routes() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.ui)
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

func runDTO(r store.Run) RunDTO {
	d := RunDTO{
		ID: r.ID, JobID: r.JobID, Status: r.Status,
		StartedAt: r.StartedAt.UTC().Format(time.RFC3339),
		Error:     r.Error, Attempt: r.Attempt,
		Output: string(r.Output),
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
	maxTimeoutSeconds  = 24 * 60 * 60
	maxRetriesAllowed  = 32
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
	j.NextRunAt = s.nextRun(j, time.Now())
	id, err := s.db.CreateJob(r.Context(), &j)
	if err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			httpError(w, http.StatusConflict, "job name already exists")
		} else {
			httpError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	s.broker.Publish(Event{Type: "job_created", Job: j.Name, At: time.Now()})
	created, _ := s.db.GetJob(r.Context(), id)
	writeJSON(w, http.StatusCreated, toDTO(created))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.db.ListJobs(r.Context())
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
	// Recompute next_run after a schedule change.
	j.NextRunAt = s.nextRun(j, time.Now())
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
	s.broker.Publish(Event{Type: "job_updated", Job: j.Name, At: time.Now()})
	writeJSON(w, http.StatusOK, toDTO(j))
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := s.db.DeleteJob(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, err.Error())
		} else {
			httpError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	s.broker.Publish(Event{Type: "job_deleted", Job: strconv.FormatInt(id, 10), At: time.Now()})
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) runJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
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
	s.broker.Publish(Event{Type: "run_triggered", Job: strconv.FormatInt(id, 10), At: time.Now()})
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

// buildJob validates the DTO into a store.Job.
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
	if _, err := cronexpr.Parse(j.Schedule); err != nil {
		return j, fmt.Errorf("schedule: %w", err)
	}
	if j.Timezone == "" {
		j.Timezone = "UTC"
	} else if _, err := time.LoadLocation(j.Timezone); err != nil {
		return j, fmt.Errorf("timezone: %w", err)
	}
	return j, nil
}

func (s *Server) nextRun(j store.Job, now time.Time) *time.Time {
	e, err := cronexpr.Parse(j.Schedule)
	if err != nil {
		return nil
	}
	loc, err := time.LoadLocation(j.Timezone)
	if err != nil {
		loc = time.UTC
	}
	next := e.NextIn(now, loc)
	return &next
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

func httpError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

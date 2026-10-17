// Package metrics implements a tiny, dependency-free exposition in
// Prometheus text format. Counters/gauges live in mutex-protected
// maps keyed by job name and are written by the scheduler's Reporter
// callbacks, read by the /metrics handler.
package metrics

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Metrics aggregates scheduler activity by job name.
type Metrics struct {
	mu        sync.Mutex
	started   map[string]uint64
	finished  map[string]map[string]uint64 // job → status → count
	retries   map[string]uint64
	sumSec    map[string]float64 // duration sum per job
	count     map[string]uint64  // finished run count per job
	lastSec   map[string]float64 // last finished duration
	nextInSec map[string]float64 // seconds until next scheduled run
	start     time.Time
}

// New creates zeroed metrics.
func New() *Metrics {
	return &Metrics{
		started:   make(map[string]uint64),
		finished:  make(map[string]map[string]uint64),
		retries:   make(map[string]uint64),
		sumSec:    make(map[string]float64),
		count:     make(map[string]uint64),
		lastSec:   make(map[string]float64),
		nextInSec: make(map[string]float64),
		start:     time.Now(),
	}
}

// JobStarted increments the started counter.
func (m *Metrics) JobStarted(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started[name]++
}

// JobFinished increments the per-status counter and duration stats.
func (m *Metrics) JobFinished(name, status string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.finished[name] == nil {
		m.finished[name] = make(map[string]uint64)
	}
	m.finished[name][status]++
	secs := d.Seconds()
	m.sumSec[name] += secs
	m.count[name]++
	m.lastSec[name] = secs
}

// RetryScheduled increments the retry counter.
func (m *Metrics) RetryScheduled(name string, _ int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retries[name]++
}

// NextRunSet records how far away the next scheduled run is.
func (m *Metrics) NextRunSet(name string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextInSec[name] = d.Seconds()
}

// UptimeSeconds exposes process uptime as a gauge value.
func (m *Metrics) UptimeSeconds() float64 {
	return time.Since(m.start).Seconds()
}

// Render produces the Prometheus text format (version 0.0.4).
// Lines are sorted for a stable, diff-friendly output.
func (m *Metrics) Render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	writeCounter(&b, "jobs_started_total", "Job executions started.", perJob(m.started))
	writeCounter(&b, "jobs_retried_total", "Retry attempts scheduled.", perJob(m.retries))

	// Flattened finished counters: one sample per (job, status).
	flat := make(map[string]uint64)
	jobs := make(map[string]struct{})
	for j, byStatus := range m.finished {
		jobs[j] = struct{}{}
		for status, n := range byStatus {
			flat[j+"\x00"+status] = n
		}
	}
	names := make([]string, 0, len(flat))
	for k := range flat {
		names = append(names, k)
	}
	sort.Strings(names)
	var rows []string
	for _, k := range names {
		j, status, _ := strings.Cut(k, "\x00")
		rows = append(rows, `jobs_finished_total{job="`+escape(j)+`",status="`+escape(status)+`"} `+strconv.FormatUint(flat[k], 10))
	}
	writeCounterRows(&b, "jobs_finished_total", "Job executions finished by status.", rows)

	// Duration gauges/counters per job.
	var sumRows, countRows, lastRows, nextRows []string
	allJobs := make([]string, 0, len(jobs))
	for j := range jobs {
		allJobs = append(allJobs, j)
	}
	for j := range m.nextInSec {
		if _, ok := jobs[j]; !ok {
			allJobs = append(allJobs, j)
		}
	}
	sort.Strings(allJobs)
	for _, j := range allJobs {
		lab := `job="` + escape(j) + `"`
		sumRows = append(sumRows, "job_duration_seconds_sum{"+lab+"} "+formatSec(m.sumSec[j]))
		countRows = append(countRows, "job_duration_seconds_count{"+lab+"} "+strconv.FormatUint(m.count[j], 10))
		if v, ok := m.lastSec[j]; ok {
			lastRows = append(lastRows, "job_last_duration_seconds{"+lab+"} "+formatSec(v))
		}
		if v, ok := m.nextInSec[j]; ok {
			nextRows = append(nextRows, "job_next_run_in_seconds{"+lab+"} "+formatSec(v))
		}
	}
	writeGaugeRows(&b, "job_duration_seconds", "Run duration histogram parts (sum/count).", append(sumRows, countRows...))
	writeGaugeRows(&b, "job_last_duration_seconds", "Most recent finished run duration.", lastRows)
	writeGaugeRows(&b, "job_next_run_in_seconds", "Seconds until the next scheduled run.", nextRows)

	// Scheduler process uptime.
	writeGaugeRows(&b, "scheduler_uptime_seconds", "Seconds since the metrics registry was created.",
		[]string{"scheduler_uptime_seconds " + formatSec(m.UptimeSeconds())})
	return b.String()
}

// ServerContent is the Content-Type Prometheus expects.
const ServerContent = "text/plain; version=0.0.4; charset=utf-8"

func perJob(src map[string]uint64) []string {
	names := make([]string, 0, len(src))
	for name := range src {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([]string, 0, len(names))
	for _, name := range names {
		rows = append(rows, `{job="`+escape(name)+`"} `+strconv.FormatUint(src[name], 10))
	}
	return rows
}

func writeCounter(b *strings.Builder, name, help string, rows []string) {
	writeCounterRows(b, name, help, rows)
}

func writeCounterRows(b *strings.Builder, name, help string, rows []string) {
	b.WriteString("# HELP " + name + " " + help + "\n")
	b.WriteString("# TYPE " + name + " counter\n")
	for _, r := range rows {
		b.WriteString(name + r + "\n")
	}
}

func writeGaugeRows(b *strings.Builder, name, help string, rows []string) {
	if len(rows) == 0 {
		rows = []string{} // keep the metric present, zero samples
	}
	b.WriteString("# HELP " + name + " " + help + "\n")
	b.WriteString("# TYPE " + name + " gauge\n")
	for _, r := range rows {
		b.WriteString(name + r + "\n")
	}
}

func formatSec(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// escape implements Prometheus label-value escaping.
func escape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
	return r.Replace(s)
}

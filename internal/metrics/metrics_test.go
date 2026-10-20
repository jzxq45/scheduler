package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestJobDeletedDropsSeries(t *testing.T) {
	m := New()
	m.JobStarted("gone")
	m.JobFinished("gone", "failed", 2*time.Second)
	m.NextRunSet("gone", 5*time.Second)

	m.ResetJob("gone")

	if out := m.Render(); strings.Contains(out, `job="gone"`) {
		t.Fatalf("deleted job's series survived:\n%s", out)
	}
	// The registry cap freed one slot: a new name is tracked again.
	m.JobStarted("fresh")
	m.NextRunSet("fresh", 2*time.Second)
	if out := m.Render(); !strings.Contains(out, `jobs_started_total{job="fresh"}`) {
		t.Fatalf("fresh series missing after reset:\n%s", out)
	}
}

func TestNameCapBoundsTracking(t *testing.T) {
	m := New()
	// Fill the registry to the cap and overflow beyond it.
	for i := 0; i < maxTrackedJobs; i++ {
		m.JobStarted(fmt.Sprintf("job-%d", i))
	}
	m.JobStarted("overflowing-name")
	m.NextRunSet("overflowing-name", time.Second)

	out := m.Render()
	if strings.Contains(out, `job="overflowing-name"`) {
		t.Fatalf("capped registry tracked the unknown name:\n%s", out)
	}
	if !strings.Contains(out, `jobs_started_total{job="job-0"} 1`) {
		t.Fatal("existing tracked job lost its series")
	}
}

func TestRenderCountersAndDurations(t *testing.T) {
	m := New()
	m.JobStarted("alpha")
	m.JobStarted("alpha")
	m.JobFinished("alpha", "success", 1500*time.Millisecond)
	m.JobStarted("beta")
	m.JobFinished("beta", "failed", 250*time.Millisecond)
	m.RetryScheduled("beta", 2)
	m.NextRunSet("beta", 30*time.Second)

	out := m.Render()

	want := []string{
		`# TYPE jobs_started_total counter`,
		`jobs_started_total{job="alpha"} 2`,
		`jobs_started_total{job="beta"} 1`,
		`jobs_finished_total{job="alpha",status="success"} 1`,
		`jobs_finished_total{job="beta",status="failed"} 1`,
		`jobs_retried_total{job="beta"} 1`,
		`job_duration_seconds_sum{job="alpha"} 1.5`,
		`job_duration_seconds_count{job="alpha"} 1`,
		`job_duration_seconds_count{job="beta"} 1`,
		`job_last_duration_seconds{job="alpha"} 1.5`,
		`job_next_run_in_seconds{job="beta"} 30`,
		`scheduler_uptime_seconds`,
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("render missing %q\n%s", w, out)
		}
	}

	// Deterministic ordering: lines are grouped by metric name.
	if strings.Index(out, "jobs_started_total{job=") > strings.Index(out, "jobs_started_total{job=\"beta\"}") {
		// alpha must precede beta (sorted).
		if i, j := strings.Index(out, `jobs_started_total{job="alpha"}`), strings.Index(out, `jobs_started_total{job="beta"}`); i > j {
			t.Error("job rows not sorted")
		}
	}
}

func TestEscapeLabelValue(t *testing.T) {
	m := New()
	m.JobStarted(`weird"job\name`)
	out := m.Render()
	if !strings.Contains(out, `job="weird\"job\\name"`) {
		t.Errorf("escape failed in:\n%s", out)
	}
}

func TestEmptyRenderHasSamples(t *testing.T) {
	out := New().Render()
	for _, name := range []string{"jobs_started_total", "jobs_finished_total", "job_duration_seconds"} {
		if !strings.Contains(out, "# TYPE "+name+" ") {
			t.Errorf("metric %s missing from empty render\n%s", name, out)
		}
	}
}

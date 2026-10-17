package cronexpr

import (
	_ "time/tzdata" // deterministic DST tests without host tz database

	"testing"
	"time"
)

// mustParse parses a literal like "2024-03-01 10:00:00 UTC".
func mustParse(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse("2006-01-02 15:04:05 MST", s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return v
}

func TestNextTable(t *testing.T) {
	t.Parallel()
	// Base "from" is 2024-03-01 10:00:00 UTC (Friday) unless overridden.
	from := mustParse(t, "2024-03-01 10:00:00 UTC")
	_ = from

	tests := []struct {
		spec string
		from string
		want string // "" means zero time
	}{
		{"0 3 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 03:00:00 UTC"},
		{"*/15 * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:15:00 UTC"},
		{"0 0 * * MON", "2024-03-01 10:00:00 UTC", "2024-03-04 00:00:00 UTC"},
		{"0 0 1 * *", "2024-03-01 10:00:00 UTC", "2024-04-01 00:00:00 UTC"},
		{"0 12 * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 12:00:00 UTC"},
		{"* * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:01:00 UTC"},
		{"* * * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:00:01 UTC"},
		{"0,30 12 * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 12:00:00 UTC"},
		{"30 0-5 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 00:30:00 UTC"},
		{"0 0 * * SUN", "2024-03-01 10:00:00 UTC", "2024-03-03 00:00:00 UTC"},
		{"0 0 * * SAT,SUN", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"0 0 1,15 * *", "2024-03-01 10:00:00 UTC", "2024-03-15 00:00:00 UTC"},
		{"0 0 15-20 * *", "2024-03-01 10:00:00 UTC", "2024-03-15 00:00:00 UTC"},
		{"0 */6 * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 12:00:00 UTC"},
		{"0 9-17 * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 11:00:00 UTC"},
		{"5 5 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 05:05:00 UTC"},
		{"0 3 1-7 * *", "2024-03-01 10:00:00 UTC", "2024-03-02 03:00:00 UTC"},
		{"*/10 0 0 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"0 0 0 29 2 *", "2024-03-01 10:00:00 UTC", "2028-02-29 00:00:00 UTC"},
		{"0 0 31 * *", "2024-03-01 10:00:00 UTC", "2024-03-31 00:00:00 UTC"},
		{"0 0 31 2 *", "2024-03-01 10:00:00 UTC", ""},
		{"15,45 2 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 02:15:00 UTC"},
		{"*/2 * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:02:00 UTC"},
		{"0 0 1 1 *", "2024-03-01 10:00:00 UTC", "2025-01-01 00:00:00 UTC"},
		{"0 0 * JAN *", "2024-03-01 10:00:00 UTC", "2025-01-01 00:00:00 UTC"},
		{"0 0 * * MON-FRI", "2024-03-01 10:00:00 UTC", "2024-03-04 00:00:00 UTC"},
		{"0 0 * * SAT", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"0 0 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"59 23 31 12 *", "2024-03-01 10:00:00 UTC", "2024-12-31 23:59:00 UTC"},
		{"0 0 2,3,4 * *", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"0 */2 * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 12:00:00 UTC"},
		{"30 6 1 1 *", "2024-03-01 10:00:00 UTC", "2025-01-01 06:30:00 UTC"},
		{"30 * * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:00:30 UTC"},
		{"0 0 */2 * *", "2024-03-01 10:00:00 UTC", "2024-03-03 00:00:00 UTC"},
		{"0 0 * * 7", "2024-03-01 10:00:00 UTC", "2024-03-03 00:00:00 UTC"},
		{"0 22 * * 1-5", "2024-03-01 10:00:00 UTC", "2024-03-01 22:00:00 UTC"},
		{"0 9 * * 1-5", "2024-03-01 10:00:00 UTC", "2024-03-04 09:00:00 UTC"},
		{"0 0 5 3 *", "2024-03-01 10:00:00 UTC", "2024-03-05 00:00:00 UTC"},
		{"*/30 0 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"0 0 1 JUL-AUG *", "2024-03-01 10:00:00 UTC", "2024-07-01 00:00:00 UTC"},
		{"0 0 * FEB 2", "2024-03-01 10:00:00 UTC", "2025-02-04 00:00:00 UTC"},
		{"15 14 1 10 *", "2024-03-01 10:00:00 UTC", "2024-10-01 14:15:00 UTC"},
		{"*/5 * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:05:00 UTC"},
		{"0 0 * * 0-3", "2024-03-01 10:00:00 UTC", "2024-03-03 00:00:00 UTC"},
		{"1/2 * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:01:00 UTC"},
		{"20/10 * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:20:00 UTC"},
		{"* 0 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 00:00:00 UTC"},
		{"0 0 ? * MON", "2024-03-01 10:00:00 UTC", "2024-03-04 00:00:00 UTC"},
		{"1 0 * * 7", "2024-03-01 10:00:00 UTC", "2024-03-03 00:01:00 UTC"},
		{"0 12 1-15/2 1 *", "2024-03-01 10:00:00 UTC", "2025-01-01 12:00:00 UTC"},
		{"5 0 * 8 *", "2024-03-01 10:00:00 UTC", "2024-08-01 00:05:00 UTC"},
		{"0/15 * * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:00:15 UTC"},
		{"00 03 * * *", "2024-03-01 10:00:00 UTC", "2024-03-02 03:00:00 UTC"},
		{"0 0 1-31/7 * *", "2024-03-01 10:00:00 UTC", "2024-03-08 00:00:00 UTC"},
		{"0 0 29 2 *", "2024-03-01 10:00:00 UTC", "2028-02-29 00:00:00 UTC"},
		{"0 3 * * *", "2024-03-01 03:00:00 UTC", "2024-03-02 03:00:00 UTC"},    // exact hit excluded
		{"0 23 31 12 *", "2023-12-31 23:23:00 UTC", "2024-12-31 23:00:00 UTC"}, // next Dec 31
		{"* * 1 1 *", "2023-12-31 23:23:00 UTC", "2024-01-01 00:00:00 UTC"},
		{"*/2 * * * * *", "2024-03-01 10:00:00 UTC", "2024-03-01 10:00:02 UTC"},
		{"0 0 1 1 0", "2024-03-01 10:00:00 UTC", "2025-01-01 00:00:00 UTC"}, // dow 0
		{"0 0 1 1 7", "2024-03-01 10:00:00 UTC", "2025-01-01 00:00:00 UTC"}, // 7 == Sunday
		{"0 0 1 1 1", "2024-03-01 10:00:00 UTC", "2025-01-01 00:00:00 UTC"}, // dom+mon restricted → dom decides (OR semantics)
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.spec+"/"+tt.from, func(t *testing.T) {
			t.Parallel()
			e, err := Parse(tt.spec)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.spec, err)
			}
			got := e.Next(mustParse(t, tt.from))
			if tt.want == "" {
				if !got.IsZero() {
					t.Fatalf("Next = %v, want zero", got)
				}
				return
			}
			want := mustParse(t, tt.want)
			if !got.Equal(want) {
				t.Fatalf("Next(%s) = %v, want %v", tt.from, got, want)
			}
		})
	}
}

func TestNextTableFixup(t *testing.T) {
	t.Parallel()
	// "0 23 31 12 *" from 23:20 on the day itself: today's 23:00 already passed.
	e, err := Parse("0 23 31 12 *")
	if err != nil {
		t.Fatal(err)
	}
	got := e.Next(mustParse(t, "2023-12-31 23:20:00 UTC"))
	want := mustParse(t, "2024-12-31 23:00:00 UTC")
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestParseErrors(t *testing.T) {
	t.Parallel()
	bad := []string{
		"",
		"* * * *",
		"* * * * * * *",
		"61 * * * * *",
		"5-2 * * * *",
		"1-60 * * * *",
		"*/0 * * * *",
		"FRI * * * * *",
		"0 0 L * *",
		"?0 * * * *",
		"** * * * *",
		"0 0 1 1 0 SUN",
		"0, * * * *",
		"0- * * * *",
		"*/x * * * *",
		"5,5,,5 * * * *",
		"+5 * * * *",
		"5,-1 * * * *",
		"? * * * * *",
		"0 0 ? * * *",
	}
	for _, spec := range bad {
		if _, err := Parse(spec); err == nil {
			t.Errorf("Parse(%q) succeeded, want error", spec)
		}
	}
}

func TestDaySemantics(t *testing.T) {
	t.Parallel()
	from := mustParse(t, "2024-03-01 10:00:00 UTC")

	// dom=13 restricted, dow unrestricted: only Nth day matters.
	e, _ := Parse("0 0 13 * *")
	if got, want := e.Next(from), mustParse(t, "2024-03-13 00:00:00 UTC"); !got.Equal(want) {
		t.Errorf("dom restricted: %v want %v", got, want)
	}
	// Both restricted: OR semantics (vixie-cron).
	e, _ = Parse("0 0 2 * SUN")
	if got, want := e.Next(from), mustParse(t, "2024-03-02 00:00:00 UTC"); !got.Equal(want) {
		t.Errorf("OR day semantics: %v want %v", got, want)
	}
}

func TestNextInTimezone(t *testing.T) {
	t.Parallel()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no tzdata: %v", err)
	}
	e, _ := Parse("0 3 * * *")
	got := e.NextIn(mustParse(t, "2024-03-01 10:00:00 UTC"), ny)
	// 3:00 NY == 8:00 UTC.
	want := mustParse(t, "2024-03-02 08:00:00 UTC")
	if !got.Equal(want) {
		t.Fatalf("NY 3:00: got %v want %v", got, want)
	}
}

func TestDSTSpringSkip(t *testing.T) {
	t.Parallel()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no tzdata: %v", err)
	}
	// 2024-03-10: clocks jump 01:59:59 EST -> 03:00:00 EDT; 02:00 never exists.
	e, _ := Parse("0 2 * * *")
	got := e.NextIn(mustParse(t, "2024-03-10 07:30:00 UTC"), ny) // = 03:30 EDT
	want := mustParse(t, "2024-03-11 06:00:00 UTC")              // Mar 11 02:00 EDT == 06:00Z
	if !got.Equal(want) {
		t.Fatalf("spring-forward skip: got %v want %v", got, want)
	}
}

func TestDSTFallFireOnce(t *testing.T) {
	t.Parallel()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no tzdata: %v", err)
	}
	e, _ := Parse("0 1 * * *")

	// First occurrence (01:00 EDT, 05:00Z) fires.
	got := e.NextIn(mustParse(t, "2024-11-02 12:00:00 UTC"), ny)
	want := mustParse(t, "2024-11-03 05:00:00 UTC")
	if !got.Equal(want) {
		t.Fatalf("first occurrence: got %v want %v", got, want)
	}
	// The repeated wall reading (01:00 EST = 06:00Z) is skipped; the
	// absolute instant already fired.
	got = e.NextIn(got, ny)
	want = mustParse(t, "2024-11-04 06:00:00 UTC") // Nov 4 01:00 EST == 06:00Z
	if !got.Equal(want) {
		t.Fatalf("repeated hour: got %v want %v", got, want)
	}
	// From inside the repeated hour, the already-absolute-passed wall
	// time is not refired either.
	got = e.NextIn(mustParse(t, "2024-11-03 06:45:00 UTC"), ny)
	if !got.Equal(want) {
		t.Fatalf("mid-repeated-hour from: got %v want %v", got, want)
	}
}

func TestDSTBerlinSpring(t *testing.T) {
	t.Parallel()
	bln, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skipf("no tzdata: %v", err)
	}
	// 2024-03-31: 02:00->03:00, so 02:00 does not exist.
	e, _ := Parse("0 2 * * *")
	got := e.NextIn(mustParse(t, "2024-03-30 12:00:00 UTC"), bln)
	want := mustParse(t, "2024-04-01 00:00:00 UTC") // Apr 1 02:00 CEST == 00:00Z
	if !got.Equal(want) {
		t.Fatalf("Berlin spring: got %v want %v", got, want)
	}
}

func TestMatches(t *testing.T) {
	t.Parallel()
	e, _ := Parse("*/15 * * * *")
	if !e.Matches(mustParse(t, "2024-03-01 10:15:00 UTC")) {
		t.Error("10:15 should match */15")
	}
	if e.Matches(mustParse(t, "2024-03-01 10:16:00 UTC")) {
		t.Error("10:16 should not match */15")
	}
}

package cronexpr

import "time"

// zeroTime is returned by Next when no firing time exists within the
// five-year projection window (covers expressions like Feb 31). Zero is
// unambiguous because no real cron time normalises to the zero Time.
var zeroTime time.Time

// NextIn returns the next firing instant strictly after from, computed
// in wall-clock time of loc.
//
// The loop walks wall-clock time rather than adding fixed offsets: DST
// transitions mean "add one minute" is not the same as "next minute".
// Nonexistent wall readings (spring forward) are skipped, and repeated
// readings (fall back) fire only once, because Next rejects any
// candidate whose absolute instant is not newer than from.
func (e Expr) NextIn(from time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	t := from.In(loc)
	t = t.Truncate(time.Second).Add(time.Second)
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		switch {
		case !e.at(e.mon, int(t.Month())):
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc)
		case !e.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc)
		case !e.at(e.hour, t.Hour()):
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, loc)
		case !e.at(e.min, t.Minute()):
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, loc)
		case !e.at(e.sec, t.Second()):
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second()+1, 0, loc)
		default:
			cand := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc)
			// time.Date normalises nonexistent wall readings
			// (spring forward): the reconstructed reading differs
			// from the requested one.
			if cand.Year() != t.Year() || cand.Month() != t.Month() ||
				cand.Day() != t.Day() || cand.Hour() != t.Hour() ||
				cand.Minute() != t.Minute() || cand.Second() != t.Second() {
				t = t.Add(time.Second)
				continue
			}
			// Wall readings that repeat after fall-back map back to
			// an instant already passed: fire once.
			if !cand.After(from) {
				t = t.Add(time.Second)
				continue
			}
			return cand
		}
	}
	return zeroTime
}

// Next returns the next firing strictly after from, in the timezone of
// from. Use NextIn with an explicit location for per-job timezones.
func (e Expr) Next(from time.Time) time.Time {
	return e.NextIn(from, nil)
}

// Matches reports whether t matches the expression exactly (second
// precision). Used by tests and the missed-policy "latest" path.
func (e Expr) Matches(t time.Time) bool {
	return e.at(e.sec, t.Second()) && e.at(e.min, t.Minute()) &&
		e.at(e.hour, t.Hour()) && e.dayMatches(t) && e.at(e.mon, int(t.Month()))
}

// at tests membership in a bitset mask.
func (e Expr) at(mask uint64, v int) bool {
	if v < 0 || v > 60 {
		return false
	}
	return mask&(1<<uint(v)) != 0
}

// dayMatches implements vixie-cron day selection:
//   - both dom and dow unrestricted: every day;
//   - only dom restricted: dom decides;
//   - only dow restricted: dow decides;
//   - both restricted: union (OR) — this surprises people, but it is
//     what stock crontab(5) specifies.
func (e Expr) dayMatches(t time.Time) bool {
	if e.domAny && e.dowAny {
		return true
	}
	if e.domAny {
		return e.at(e.dow, int(t.Weekday()))
	}
	if e.dowAny {
		return e.at(e.dom, t.Day())
	}
	return e.at(e.dom, t.Day()) || e.at(e.dow, int(t.Weekday()))
}

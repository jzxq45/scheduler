// Package cronexpr implements a cron expression parser and next-fire
// calculator. It supports 5-field (min hour dom mon dow) and 6-field
// (sec min hour dom mon dow) specs with *, ?, lists, ranges, steps
// and month/weekday names. Written from scratch so the scheduler
// owns exact projection semantics: see next.go for why.
package cronexpr

import (
	"fmt"
	"strings"
)

var dowNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

var monNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
	"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

// fieldDef describes one cron field for the parser.
type fieldDef struct {
	name  string
	min   int
	max   int
	names map[string]int
	// isDay marks dom/dow, the only fields that accept "?" and that
	// participate in vixie-cron OR-joining.
	isDay bool
}

var fieldDefs = []fieldDef{
	{"second", 0, 59, nil, false},
	{"minute", 0, 59, nil, false},
	{"hour", 0, 23, nil, false},
	{"day-of-month", 1, 31, nil, true},
	{"month", 1, 12, monNames, false},
	{"day-of-week", 0, 6, dowNames, true},
}

// Expr is a parsed cron expression created via Parse.
type Expr struct {
	sec, min, hour, dom, mon, dow uint64

	// domAny/dowAny record unrestricted (* or ?) day fields, which
	// vixie-cron uses to decide day matching: both restricted means OR.
	domAny, dowAny bool
}

// Parse parses a 5-field (min hour dom mon dow; second implied 0) or
// 6-field (sec min hour dom mon dow) expression.
func Parse(spec string) (Expr, error) {
	fields := strings.Fields(spec)
	switch len(fields) {
	case 5:
		fields = append([]string{"0"}, fields...)
	case 6:
	default:
		return Expr{}, fmt.Errorf("cron: want 5 or 6 fields, got %d in %q", len(fields), spec)
	}

	var e Expr
	for i, def := range fieldDefs {
		m, any, err := parseField(fields[i], def)
		if err != nil {
			return Expr{}, fmt.Errorf("cron %q: %s: %w", spec, def.name, err)
		}
		switch i {
		case 0:
			e.sec = m
		case 1:
			e.min = m
		case 2:
			e.hour = m
		case 3:
			e.dom, e.domAny = m, any
		case 4:
			e.mon = m
		case 5:
			e.dow, e.dowAny = m, any
		}
	}
	return e, nil
}

// parseField parses one field into a bitset mask. The bool result
// reports an unrestricted day field ("*" or "?").
func parseField(field string, def fieldDef) (uint64, bool, error) {
	var mask uint64
	any := false
	for _, item := range strings.Split(field, ",") {
		if item == "" {
			return 0, false, fmt.Errorf("empty item in %q", field)
		}
		body, stepStr, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			n, err := parseNum(stepStr, def)
			if err != nil {
				return 0, false, err
			}
			if n < 1 {
				return 0, false, fmt.Errorf("step must be >=1, got %d in %q", n, item)
			}
			step = n
		}

		lo, hi := def.min, def.max
		if body != "*" && body != "?" {
			loStr, hiStr, isRange := strings.Cut(body, "-")
			v, err := parseNum(loStr, def)
			if err != nil {
				return 0, false, err
			}
			lo = v
			if isRange {
				hi, err = parseNum(hiStr, def)
				if err != nil {
					return 0, false, err
				}
			} else if !hasStep {
				// Bare value: exact. With a step ("5/10") vixie-cron
				// means v..max, so hi keeps the field maximum.
				hi = v
			}
		} else if body == "?" {
			// "?" is a wildcard only the day fields accept; it behaves
			// like "*" apart from the dom/dow matching bookkeeping.
			if !def.isDay {
				return 0, false, fmt.Errorf("? not allowed in %s field", def.name)
			}
			any = true
		} else if def.isDay && !hasStep {
			any = true
		}
		if lo > hi {
			return 0, false, fmt.Errorf("range %q inverted (%d>%d) and %s max is %d", body, lo, hi, def.name, def.max)
		}
		if lo < def.min || hi > def.max {
			return 0, false, fmt.Errorf("range %q outside %d..%d for %s", body, def.min, def.max, def.name)
		}
		for i := lo; i <= hi; i += step {
			mask |= 1 << uint(i)
		}
	}
	if mask == 0 {
		return 0, false, fmt.Errorf("field %q matches nothing", field)
	}
	return mask, any, nil
}

// parseNum resolves a field token: decimal, weekday/month alias, or
// the dow "7" alias for Sunday.
func parseNum(tok string, def fieldDef) (int, error) {
	if tok == "" {
		return 0, fmt.Errorf("empty number in %s field", def.name)
	}
	if def.names != nil {
		if v, ok := def.names[strings.ToUpper(tok)]; ok {
			return v, nil
		}
	}
	n, ok := atoi(tok)
	if !ok {
		return 0, fmt.Errorf("bad number or name %q in %s field", tok, def.name)
	}
	if def.isDay && def.name == "day-of-week" && n == 7 {
		n = 0
	}
	if n < def.min || n > def.max {
		return 0, fmt.Errorf("%q outside %d..%d for %s", tok, def.min, def.max, def.name)
	}
	return n, nil
}

// atoi parses a non-negative decimal integer.
func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n > 1<<20 {
			return 0, false
		}
	}
	return n, true
}

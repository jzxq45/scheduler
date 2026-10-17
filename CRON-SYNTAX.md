# Cron expressions

The scheduler uses its own parser (`internal/cronexpr`), following
vixie-cron semantics:

- **5 fields** — `minute hour dom month dow`
  (a leading `0` second is implied).
- **6 fields** — `second minute hour dom month dow`.

## Fields

| Position | Field        | Range | Names      |
|----------|--------------|-------|------------|
| 1        | second       | 0–59  |            |
| 2        | minute       | 0–59  |            |
| 3        | hour         | 0–23  |            |
| 4        | day-of-month | 1–31  |            |
| 5        | month        | 1–12  | JAN…DEC    |
| 6        | day-of-week  | 0–7   | SUN…SAT (7≡0≡Sun) |

Case-insensitive names are accepted (`mon`, `Feb`); numeric weekday
`0` and `7` both mean Sunday (`0`–`7` valid, vixie-cron compatible).

## Syntax

| Symbol    | Meaning | Example |
|-----------|---------|---------|
| `*`       | every value | `* * * * *` |
| `,`       | list | `0,15,30,45 * * * *` |
| `-`       | inclusive range | `0 9-17 * * *` |
| `/`       | step over the field range (or after a range) | `*/10 * * * *`, `0 0-23/2 * * *` |
| `?`       | same as `*` (allowed only in dom/dow, marks the field "unrestricted") | `0 0 1 * ?` |

Whitespace separates fields; extra whitespace is tolerated. Comments
are not part of an expression.

## Day-of-month / day-of-week OR rule

When **both** dom and dow are restricted (neither is `*`/`?`), a date
matches if *either* matches (vixie-cron rule):

- `0 0 1 * 1` → midnight on the 1st *or* Mondays;
- `0 0 1 * ?` → midnight on the 1st only.

## Next-fire semantics

`Expr.Next(t)` returns the first fire time strictly after `t`
(`NextIn(t, loc)` in the given location). Stepping uses second
granularity in six-field mode and never double-fires on a downward
adjustment; DST spring-forward gaps and fall-back overlaps are handled
by projecting in the job's own timezone — a schedule like `0 2 * * *`
fires at the local wall-clock 02:00 whenever it exists, and at the
adjacent real time once when the hour is skipped.

Verification: the parser's test suite mirrors expectations checked with
[crontab.guru](https://crontab.guru).

## Examples

| Expression | Meaning |
|------------|---------|
| `* * * * *` | every minute |
| `*/5 * * * *` | every 5 minutes |
| `0 9-17 * * *` | hourly from 09:00 to 17:00 |
| `30 21 * * 1FRI` | 21:30 on Fridays |
| `0 0 1,15 * *` | midnight on the 1st and 15th |
| `0 12 * ? 2-6/2` | noon Feb, Apr, Jun … |
| `*/10 * * * * *` | every 10 seconds (six-field) |

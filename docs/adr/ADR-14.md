# ADR-14: Event-study horizons never cross a session boundary

**Status:** IMPLEMENTED as the pilot default — **needs stakeholder confirmation** (see "Open question").

## Context

v1 indexed the 5-minute trading grid over the whole calendar, so the "30m after" of an event at 11:20
was measured at 13:25 (across the lunch break) and the "30m after" of an event at 14:40 landed on the
next morning, both with `is_complete = true` while `news_count_after` was NULL (P8). The before bar was
not checked for completeness (P9).

## Decision

- Intraday horizons are counted in 5-minute windows **inside the session of the event**:
  `30m` = 6 windows, `1h` = 12 windows. The window index restarts at every session
  (`reference.trading_grid` adds `window_idx`, `session_windows`).
- If `T + n` is after the last window of the session, or `T - n` is before its first window, the
  horizon is incomplete: `is_complete = false`, `missing_reason = HORIZON_CROSSES_SESSION`, and the
  corresponding return is NULL.
- A horizon is complete only if the event bar, every bar of the before path `[T-n, T]` and every bar
  of the after path `(T, T+n]` are complete (`completeness = 1.0`), and both news-density rows exist.
- `1tradingday` = close of the last bar of the **next trading date** of the exchange calendar (not +24h).
- `missing_reason` values: `EVENT_NOT_ON_TRADING_GRID`, `INCOMPLETE_EVENT_BAR`,
  `MISSING_NEXT_TRADING_DAY`, `MISSING_NEXT_DAY_CLOSE`, `HORIZON_CROSSES_SESSION`,
  `MISSING_FUTURE_BARS`, `MISSING_HISTORY_BARS`, `MISSING_NEWS_DENSITY`. `is_complete` is never NULL
  (quality gate `event_analysis.is_complete_not_null`).
- `return_after` may be present while the before path is incomplete (and vice versa); `is_complete`
  requires both.

## Open question for the team

Alternative: count trading minutes across the lunch break (11:25 + 30m = 13:25). It would raise the
share of complete horizons in the last 30/60 minutes of each session, but mixes the overnight/lunch
information flow into an "intraday" return. Changing the rule requires a new `analysis_version`.

## Evidence

`tests/integration/test_batch_v3.py::test_p8_intraday_horizon_does_not_cross_session`,
`::test_p9_before_bar_must_be_complete_and_flag_never_null`, `tests/unit/test_v3_pure.py`
(`horizon_target`).

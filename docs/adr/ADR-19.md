# ADR-19: Late or corrected history — downstream staleness warning, explicit re-run

**Status:** IMPLEMENTED (warning only). Automatic cascading re-runs are deliberately not done.

## Context

B5 baselines of day D use the N previous trading days; B6/B7 of day D use the baseline of D and (for
the 1-trading-day horizon) the bars of the next trading day. When B3 features or B4 density of an
earlier day change (late bars, corrected `published_at`, new alias dictionary), baselines/events
already published for later days silently become stale.

## Decision

- Every committed partition carries a business fingerprint (`stats` in the catalog).
- After a run, for each B3/B4 partition written in this run whose fingerprint differs from the
  previous generation (or had none), the pipeline lists the `stock_baseline`, `historical_events` and
  `event_analysis` partitions of the following N trading days (N = `baseline.train_trading_days`)
  plus the `event_analysis` partition of the previous trading day that exist in the catalog and were
  not rewritten in the same run.
- The list is stored in the run manifest (`metadata.stale_downstream`), appended to `warnings`,
  printed, and returned by `stock-batch run`.
- The operator re-runs them explicitly, e.g. `run --from-job B5 --start <first stale> --end <last>`.

## Why not automatic

A single late bar on day D can invalidate up to 60 later trading days of baselines; silently
recomputing them would change published `baseline_version`s that the Speed Layer may have pinned.
The operator decides, and the content-addressed versions make the change visible.

## Evidence

`jobs/pipeline.py::stale_downstream`;
`tests/integration/test_batch_v3.py::test_adr19_changed_history_flags_stale_downstream` (identical
rerun ⇒ no warning; a late corrected bar on 2026-10-02 ⇒ baselines of 10-05/10-06/10-07 listed).

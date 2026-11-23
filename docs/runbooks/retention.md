# Retention and garbage collection

`stock-batch gc` deletes only immutable partition directories (and leftover staging) that **no protected
snapshot references**. It is a dry-run unless `--apply` is given.

## What is protected

1. Every partition referenced by the newest `storage.gc_keep_generations` catalog generations (default dev 10,
   test 5, prod 30) and by the current generation.
2. Every partition referenced by a SUCCESS run manifest — i.e. every rollback target — unless
   `--keep-success-runs N` / `storage.gc_keep_success_runs` limits this to the newest N SUCCESS runs.
3. Gold partitions behind the published baseline pointer and every entry of `_system/baselines/history/`.
4. Anything younger than `storage.gc_min_age_hours` (default dev 24 h, prod 72 h) — protects in-flight writers.

Never deleted by gc: `_system/catalog/log`, run manifests, quality reports, `_system/baselines/versions`
(the Speed Layer may still read them), exports.

## What is collected

Partitions of FAILED/conflicting runs, superseded `write=<n>` attempts inside a run, partitions that only belonged
to SUCCESS runs outside the `--keep-success-runs` window, and `_system/ingest_staging` / `_system/baselines/_staging`
left behind by failed runs.

## Procedure

1. `storage-report` → look at `unreferenced_bytes` per dataset.
2. `gc` → review `candidates`, `bytes`, `paths`, `policy`, `protected_by`.
3. Take an HDFS snapshot of `/data` if the cluster allows it.
4. `gc --apply`, then `verify` (current generation) — must be `OK`.
5. A rollback to a run whose partitions were collected fails with a clear error (`partition paths no longer exist`).

Raw Bronze and raw news content follow the provider licence (ADR-11); setting `--keep-success-runs` effectively
expires old raw snapshots, so agree it with the data owner first.

# ADR-13: Snapshot atomicity — append-only commit log with compare-and-swap

**Status:** ACCEPTED for the pilot (implemented in v1.1, verified on local FS and on HDFS 3.2.1 in the isolated smoke stack). Not a replacement for Iceberg/Delta in production.

## Context

v1 published a mutable `_system/catalog/current.json` with delete + rename on HDFS. Confirmed defects:
two concurrent commits silently lost one writer's datasets (S1); a missing pointer made the lake look
empty and the next `run --job B1` published empty data (S2); the SUCCESS manifest was written before the
pointer and no RUNNING manifest existed (S10).

## Decision

1. **Source of truth = commit log.** Every commit writes one immutable file
   `_system/catalog/log/<generation, 20 digits>.json` with an atomic *create-exclusive*:
   - HDFS: write a temp file, then `FileContext.rename(tmp, dst, Options.Rename.NONE)`; the NameNode
     refuses atomically when `dst` exists (same mechanism as Delta Lake's HDFS log store);
   - local FS: `os.link(tmp, dst)` (fails atomically with `FileExistsError`).
2. **Compare-and-swap.** A transaction remembers `base_generation` and commits `base + 1`. If that file
   already exists, `CommitConflictError` is raised; nothing of the losing run is published and its
   manifest is FAILED. No automatic retry: its outputs were computed from an older snapshot.
3. **`current.json` stays** (read by legacy readers / other modules) and is refreshed with an atomic
   replace (`FileContext.rename(..., OVERWRITE)` / `os.replace`). It may lag the log after a crash;
   `LakeReader`/`Transaction` always resolve the newest log entry, `verify` reports `pointer_in_sync`,
   and the next commit refreshes it.
4. **Legacy adoption.** A `current.json` without a log is read as generation 0.
5. **Corruption is loud.** No log and no pointer while SUCCESS runs exist → `CatalogCorruptError`
   with the `rollback --run-id` recovery instruction (never "empty lake").
6. **Manifest lifecycle.** `RUNNING` (at `begin` or first write) → per-job checkpoints → `COMMITTING`
   (records the target generation) → `SUCCESS` | `FAILED`. `get_run` reconciles a `COMMITTING`
   manifest against the log (entry with this run id ⇒ SUCCESS, otherwise FAILED). An error after the
   log entry exists never reports FAILED.
7. **Rollback** publishes a NEW generation (CAS as well) equal to the target run's datasets, stats and
   Kafka offsets, after checking every path still exists.

## Consequences

- Single-writer is no longer required for safety: concurrent writers get a conflict, not a lost update.
- Throughput of concurrent writers is limited (optimistic concurrency, one winner per generation).
- Multi-table atomicity holds because one log entry pins every dataset.
- Not provided: distributed locking, multi-cluster replication, HDFS HA failover tests, time travel by
  timestamp (only by generation/run id).

## Evidence

`tests/unit/test_v3_pure.py` (create_exclusive contention, legacy adoption, corrupt catalog),
`tests/integration/test_storage_v3.py` (S1, S2, S10), `tests/integration/test_e2e_v3.py`
(AC-15 incl. crash between log and pointer, pipeline-level conflict), `scripts/smoke/smoke_hdfs.py`
(create-exclusive and atomic replace through py4j on a real NameNode).

# Operator runbook (Windows PowerShell & Linux)

Full Vietnamese hand-over: [../HANDOVER.md](../HANDOVER.md). One-command verification:
`powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1` (isolated stack, never touches the
dev HDFS volumes).

## Dev stack (keeps your HDFS data)

From the repo root: `docker compose up -d namenode datanode`; `docker compose exec namenode hdfs dfs -mkdir -p /data`;
`docker compose exec namenode hdfs dfs -chmod -R 777 /data` (**dev only**). Build: `docker compose build spark-job`
(the image bundles the Spark 3.5.9 Kafka connector). Never type a trailing bash `\` in PowerShell; keep commands on one line.

Every command below is prefixed with `docker compose --profile tools run --rm spark-job --env dev`.

## Daily cycle

1. `ingest-kafka --brokers kafka:9092` — reads `[committed, latest)` per partition; offsets are stored in the
   catalog commit. Optional `--max-records-per-partition N` bounds a catch-up run.
2. `run --job all --start D --end D` — B1..B7 for day D; prints `generation`, `warnings`, `stale_downstream`.
3. `verify --run-id <run>` — paths, rows, fingerprints, gates; exit code 1 on problems.
4. `publish-baseline --version bl_D_<cfg>_<content>` (find it with `show-current` or the Gold table) — policy:
   usable cohorts (READY/ZERO_STD) ≥ `baseline.publish_min_usable_ratio`; `--allow-older` to go back in time.
5. `export --dataset historical_events --start D --end D --format jsonl` for Serving.

## Re-run, backfill, resume, rollback

- Backfill a range: `run --job all --start D1 --end D2` replaces exactly those partitions (B2 always rebuilds the
  whole news history). Rejects are filed by trading date, so a backfill keeps its quarantine.
- Re-run from a job: `run --from-job B5 --start D1 --end D2`.
- `stale_downstream` in the run output (ADR-19) lists later baseline/event partitions computed from changed
  B3/B4 inputs; re-run them with `--from-job B5` for that range.
- Failed run: `list-runs`, read `_system/runs/run_id=<run>/manifest.json` (failed job + error) and
  `_system/quality_reports/run_id=<run>/report.json`; fix; `resume --run-id <run>` (skips completed jobs when the
  business config and catalog generation are unchanged, otherwise re-runs everything).
- `CommitConflictError`: another writer published first; nothing of your run was published — run it again.
- Bad data published: `rollback --run-id <earlier SUCCESS run>` — publishes a NEW generation equal to that
  snapshot, including the Kafka offsets.
- `CatalogCorruptError` (log and pointer missing while SUCCESS runs exist): `list-runs`, then `rollback --run-id`
  of the newest SUCCESS run.

## Maintenance

- `storage-report` — files, bytes, small files, max files per partition, unreferenced bytes.
- `gc` (dry-run) then `gc --apply` — see [retention.md](retention.md).
- `compact --dataset stock_features --start D1 --end D2 --files N` — keeps provenance, commits only if the
  business fingerprints are unchanged.
- `register-hive --database stock_batch` — see [hive.md](hive.md).

## Local Python

Python 3.10/3.11 with **PySpark 3.5.9** and Java 17; `pip install -e '.[test]'`;
`DATA_ROOT=file:///absolute/path`, `SPARK_LOCAL_IP=127.0.0.1`. Pure tests: `pytest tests/unit tests/contract`.

## Known limits

Synthetic demo references (5 HOSE dates, 3 symbols); single NameNode; no Kerberos; reference CSVs read from the
driver's local disk. Do not run the full test suite in parallel with the smoke stack on an 8 GB Docker VM.

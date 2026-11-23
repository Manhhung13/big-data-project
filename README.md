# Stock Batch Layer — HDFS Data Lake × Apache Spark B1–B7

Version `1.1.0` — Python 3.10/3.11 + **Apache Spark 3.5.9** + Java 17 + HDFS + Parquet/Snappy. A runnable,
tested **pilot** on synthetic data — not a certified production deployment.

- Hand-over (Vietnamese): [docs/HANDOVER.md](docs/HANDOVER.md)
- Integration contracts for Speed Layer / Serving / Kafka producers (Vietnamese): [docs/INTEGRATION.md](docs/INTEGRATION.md)
- Architecture: [docs/architecture.md](docs/architecture.md) · Acceptance: [docs/acceptance.md](docs/acceptance.md) ·
  Verification record: [docs/BUILD_AUDIT.md](docs/BUILD_AUDIT.md) · FR matrix: [docs/FR_TRACEABILITY.md](docs/FR_TRACEABILITY.md)
- Specification: `docs/specification/Business_Requirements_HDFS_BatchLayer_v1.docx`

## Goal

For Vietnam stock symbols, ingest 1-minute OHLCV and news Kafka envelopes; preserve raw provenance; standardise and
deduplicate; resample 5-minute features; compute 5m/30m/1h news density; build as-of baselines; flag historical
price/volume/news anomalies; report before/after outcomes without inventing data. **Not trading advice or
forecasting**; news/price association is temporal, not causal.

## One-command verification (Windows PowerShell, Docker Desktop)

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1
```

Builds the image (Kafka connector included), runs all pytest suites inside Spark 3.5.9, then HDFS and Kafka smoke
tests on an **isolated** compose project `stockbatch-verify` (own volumes, no published ports) that is removed
afterwards. It never touches the dev stack or its HDFS volumes.

## Quick start (dev stack)

```powershell
docker compose up -d namenode datanode
docker compose exec namenode hdfs dfs -mkdir -p /data
docker compose exec namenode hdfs dfs -chmod -R 777 /data
docker compose build spark-job
docker compose --profile tools run --rm spark-job --env dev seed-demo
docker compose --profile tools run --rm spark-job --env dev run --job all --start 2026-10-01 --end 2026-10-07
docker compose --profile tools run --rm spark-job --env dev verify
docker compose --profile tools run --rm spark-job --env dev show-current
```

Each command is one line (never end a PowerShell line with a bash `\`). With the dev policy (`min_samples: 20`)
five synthetic days leave most cohorts SPARSE — correct behaviour; the test policy (`min_samples: 2`) exercises the
anomaly logic.

## Commands

`validate-config`, `seed-demo`, `ingest-kafka`, `run --job|--from-job --start --end`, `resume`, `verify`,
`show-current`, `list-runs`, `rollback`, `publish-baseline [--allow-older]`, `show-baseline`, `export`,
`storage-report`, `gc [--apply]`, `compact`, `register-hive [--embedded-metastore]`. Details:
[docs/runbooks/operations.md](docs/runbooks/operations.md).

## Repository layout

```text
src/stock_batch/
  cli.py                 command line
  config.py              config defaults/validation, business config_hash
  rules.py               pure rules (clean_text, canonical_url, signal_state, ...)
  schemas.py             explicit Spark schemas (source of contracts/schemas/*.json)
  reference.py           symbol master (effective ranges) / trading calendar / 5m grid
  quality.py             pre-commit quality gates + reports
  lake.py                LakeReader: read-only, generation-pinned access for other modules
  serving.py             publish-baseline (Speed Layer) and export (Serving/ES)
  ingest/kafka.py        incremental Kafka -> Bronze, offsets in the catalog commit
  storage/fs.py          local/HDFS primitives incl. atomic create-exclusive and replace
  storage/catalog.py     commit log + CAS, manifests, Transaction, rollback
  storage/maintenance.py verify, storage report, gc, compact
  jobs/pipeline.py       B1->B7 DAG, checkpoints/resume, quarantine helper, stale-downstream
  transforms/*.py        B1..B7
contracts/{kafka,schemas,gold}/   configs/{dev,test,prod}/   reference/*.csv (synthetic)
scripts/verify_all.ps1  scripts/smoke/*.py  scripts/benchmark.py  scripts/export_contracts.py
docker/images/Dockerfile  docker/verify/docker-compose.verify.yml  docker-compose.yml
tests/{unit,contract,integration}/
docs/{HANDOVER,INTEGRATION,architecture,acceptance,BUILD_AUDIT,FR_TRACEABILITY,decisions_to_survey}.md, docs/adr/, docs/runbooks/
```

`contracts/schemas/*.json` is generated from `schemas.py` with `python scripts/export_contracts.py`
(a contract test fails if they drift).

## Local Python

Use Python 3.10/3.11, **PySpark 3.5.9** and JDK 17. `pip install -e '.[test]'`; pure tests:
`pytest -q tests/unit tests/contract`; Spark tests: `SPARK_LOCAL_IP=127.0.0.1 pytest -q tests/integration`.
With a different PySpark (e.g. 4.x) run the Spark tests in the Docker image instead.

## Validation status

Last recorded run (2026-11-23, image `stock-batch-layer-spark-job:verify-20261122`, Spark 3.5.9, Java 17) — exact
commands, outputs and log files: [docs/BUILD_AUDIT.md](docs/BUILD_AUDIT.md); per-AC status:
[docs/acceptance.md](docs/acceptance.md).

- pytest inside the image: all suites pass except `test_hive_p1` (SKIPPED: needs an external Hive Metastore;
  the embedded-Derby Hive test passes).
- Real HDFS smoke 12/12 and real Kafka smoke 8/8 on the isolated `stockbatch-verify` stack, twice in a row
  (the second run cleans up a deliberately left-over stack first).

Everything was executed on **synthetic** data. Not verified: real licensed news, an official HOSE calendar and
symbol list, HDFS HA/Kerberos, a shared Hive Metastore, performance at 30–100 symbols × 1–3 years
(`scripts/benchmark.py` exists, no result recorded). **Not production-ready.** Open decisions:
[docs/decisions_to_survey.md](docs/decisions_to_survey.md).

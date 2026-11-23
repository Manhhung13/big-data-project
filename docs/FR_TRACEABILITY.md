# Traceability: FR -> code -> tests -> output (v1.1)

Status = result of the runs recorded in [BUILD_AUDIT.md](BUILD_AUDIT.md) (image `stock-batch-layer-spark-job:verify-20261122`, Spark 3.5.9, Java 17, synthetic data only). Per-AC detail: [acceptance.md](acceptance.md). Not production-ready: no real licensed dataset has been processed.

| ID | Main implementation | Test evidence | Output | Status |
|---|---|---|---|---|
| FR01 Kafka→Bronze | `ingest/kafka.py` (offset range resolved once, Parquet staging, anti-join, headers map, offsets in catalog), `storage/catalog.py` | `integration/test_kafka_v3.py`, `scripts/smoke/smoke_kafka.py` (real Kafka 3.9.0 broker, 8/8 on a clean stack) | bronze_stock, bronze_news, bronze_bad_records, `kafka_offsets` | PASS |
| FR02 Clean stock | `transforms/stock.py` | `test_b1_b4.py::test_ac01*`, `test_batch_v3.py::test_s4*, test_p1*, test_p2*, test_p10*` | stock_price, quarantine | PASS |
| FR03 Clean news | `transforms/news.py`, `rules.clean_text` | `test_b1_b4.py::test_ac02*, test_ac08*`, `test_batch_v3.py::test_p1*, test_p4*, test_s5*`, `unit/test_v3_pure.py` | news, quarantine | PASS |
| FR04 Many-to-many mapping | `transforms/news.py`, `reference.py` | `test_ac02_ac03*`, `test_batch_v3.py::test_p3*` | news_stock_mapping | PASS |
| FR05 5m resample | `transforms/features.py` | `test_ac04_ac05*`, `test_ac06*`, `test_batch_v3.py::test_ac06_suspended_symbol_day_is_not_interpolated` | stock_features | PASS (demo calendar; intraday halts/ATO/ATC not covered) |
| FR06 News density | `transforms/density.py` | `test_ac07*`, `test_ac08*`, `test_batch_v3.py::test_p5*`, gate `zero_filled_grid_all_windows` | news_density | PASS |
| FR07 Baseline | `transforms/baseline.py` | `test_b5_b7.py::test_ac09_ac10*`, `test_batch_v3.py::test_s8*, test_float_noise*` | stock_baseline | PASS |
| FR08 Anomaly + events | `transforms/events.py` | `test_ac11*`, `test_ac12*`, `test_batch_v3.py::test_p6*, test_p7*, test_events_on_different_days*` | historical_events | PASS |
| FR09 Event study | `transforms/study.py` | `test_ac13*`, `test_batch_v3.py::test_p8*, test_p9*` | event_analysis | PASS |
| FR10 Gold provenance | `storage/catalog.py::Transaction.write`, `schemas.py` | `test_e2e_v3.py::test_brd23*`, `test_cli_v3.py::test_s6*` | all Gold + manifests | PASS |
| FR11 Baseline publish | `serving.py::publish_baseline`, `lake.py` | `test_e2e_v3.py::test_e2e_verify_publish_export`, `test_batch_v3.py::test_p12*`, `test_speed_baseline_contract.py` (export vs row contract), `contract/test_static.py::test_every_configured_news_window_is_in_the_speed_row_contract`, smoke HDFS `publish_baseline` | `_system/baselines/*` | PASS |
| FR12 Backfill / resume / scoped replacement | `jobs/pipeline.py`, `storage/catalog.py`, `cli.py` | `test_e2e_v3.py` (AC-14, AC-15, resume, conflict), `test_storage_v3.py`, `test_batch_v3.py::test_s3*, test_adr19*` | catalog generations | PASS |
| FR13 Hive external | `cli.py::register_hive` | `integration/test_hive_embedded.py` (embedded Derby); `test_hive_p1.py` SKIPPED (no external metastore) | `<dataset>_g<g>` + `current_*` | PASS (embedded only) |
| FR14 Small files, retention, audit | `storage/catalog.py` (1 file/partition), `storage/maintenance.py` (report, gc, compact, verify), `quality.py` | `test_storage_v3.py::test_s9*, test_gc*, test_s12*`, `test_cli_v3.py`, smoke HDFS `one_file_per_partition`/`gc_dry_run_deletes_nothing` | reports | PASS for layout/gc/compact/verify; `scripts/benchmark.py` exists but **no benchmark result is recorded** |

Integration points for other modules: [INTEGRATION.md](INTEGRATION.md).

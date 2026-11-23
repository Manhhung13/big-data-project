# HDFS Data Lake × Spark Batch Layer — architecture (v1.1)

Vietnamese operator/hand-over guide: [HANDOVER.md](HANDOVER.md). Integration contracts:
[INTEGRATION.md](INTEGRATION.md).

## Boundary

Kafka `stock-price-topic`, `news-topic` → immutable **Bronze envelopes** → Spark **B1/B2 Silver** →
**B3/B4/B5/B6/B7 Gold** → published baseline (Speed Layer) and pinned exports (Serving). Crawler,
Speed Layer, Elasticsearch, dashboard, trading model and Airflow are outside v1 scope.

## Jobs

| Job | Reads (pruned partitions) | Writes | Must enforce |
|---|---|---|---|
| B1 | Bronze stock (all; filtered by `record_date` in scope before ranking), symbol master by effective range, calendar | Silver `stock_price` (scoped), stock rows of `quarantine` | INVALID_JSON detection, source priority, valid minute in session, raw lineage, raw = accepted + rejected |
| B2 | Bronze news (all) + alias dictionary | `news`, `news_stock_mapping` (**replace_all**), news rows of `quarantine` | HTML cleanup + NFC, canonical URL, `duplicate_group`, Unicode case-insensitive alias match, latest version wins |
| B3 | `stock_price` of the requested dates | `stock_features` | ordered first/last, 5m, session-safe returns, completeness |
| B4 | `news`/mapping of the dates and the previous day | `news_density` | distinct stories (`duplicate_group`) and articles, half-open windows, zero-fill |
| B5 | features/density of the N previous **trading** days | `stock_baseline` (trading days only) | `train_end < valid_for_trading_date`, ddof=1, SPARSE/ZERO_STD (float-noise tolerant), content-addressed version |
| B6 | features/density/baseline of the dates, news ±1 day | `historical_events` | ANOMALY / NORMAL / UNKNOWN states, signed max-|z|, grouping per symbol and trading day |
| B7 | events of the dates, features of the dates + next trading days | `event_analysis` | horizons inside the session (ADR-14), before/after paths complete, `is_complete` never NULL |

Clock convention: a bar `[10:30,10:35)` produces `window_end = 10:35` (event time, UTC stored),
baseline `time_bucket = 10:30` (Asia/Ho_Chi_Minh), news density joined at `window_end = 10:35`.
`spark.sql.session.timeZone` must be `UTC` (the pipeline refuses to run otherwise).

## Storage layout and catalog (ADR-13)

```text
<root>/<area>/_versions/run_id=<run>/write=<n>/<partition_col>=<value>/part-*.parquet   immutable
<root>/_system/catalog/log/<generation:020d>.json     one immutable file per commit (source of truth)
<root>/_system/catalog/current.json                   copy of the newest generation (legacy readers)
<root>/_system/runs/run_id=<run>/manifest.json        RUNNING -> COMMITTING -> SUCCESS | FAILED
<root>/_system/quality_reports/run_id=<run>/report.json
<root>/_system/baselines/{current.json, history/, versions/<baseline_version>/}
<root>/_system/exports/<dataset>/generation=<g>/...
```

- A transaction commits generation `base + 1` with an atomic create-exclusive (HDFS
  `FileContext.rename(..., NONE)`, local `os.link`); a concurrent winner makes it fail with
  `CommitConflictError` (no lost update). `current.json` is replaced atomically
  (`Options.Rename.OVERWRITE` / `os.replace`).
- Each catalog generation pins **all** datasets (multi-table atomicity) and stores per-partition
  `stats` (rows + order-independent business fingerprint) and the Kafka offsets of Bronze.
- Readers (`LakeReader`, `Transaction`) pin one generation. Never scan `_versions` recursively.
- Writes: `repartition(partition key)` + `sortWithinPartitions(business key)` → one file per partition
  (bounded by `maxRecordsPerFile`); modes: partitions present / `only_partitions` (exact scope,
  empty partitions removed) / `replace_all`.
- Every job checkpoints its catalog delta in the manifest; `resume` re-applies completed deltas when the
  business config and base generation are unchanged.
- Shared `quarantine` is only modified through `replace_quarantine(dataset, rows, scope)`.

## Quality gates (before commit)

One aggregation per written dataset (only written partitions): business key not-null + unique,
OHLC validity, completeness range, return ⇔ reason, HTML removed from news, density zero-fill for all
three window sizes (row count = trading grid), baseline leakage/status consistency, event type ⇔ flags
⇔ states, `is_complete` semantics, and raw = accepted + rejected for B1/B2. Report written to
`_system/quality_reports`; any failure ⇒ run FAILED, catalog unchanged.

## Kafka ingest

`[committed offset, latest)` per partition is resolved once with a KafkaConsumer (driver, py4j),
read with explicit JSON offsets, staged once to Parquet under the run id, anti-joined on
`(topic, partition, offset)`, and only touched `ingest_date` partitions are rewritten. Offsets are
saved in the same catalog commit; rollback restores them. Headers → `map<string,string>`.

## Determinism and provenance

- `config_hash` = business configuration + reference-file digests (no paths/environment/Spark knobs).
- `baseline_version = <prefix>_<date>_<config_hash[:8]>_<content hash[:8]>`.
- `event_id = sha256(symbol | event_time | rule_version)`.
- Gold rows carry `run_id`, `code_version`, `config_hash`, `processing_timestamp`, `source_snapshot`
  (`<inputs>|base_generation=<g>;run_id=<run>`); these are excluded from fingerprints so identical
  reruns have identical fingerprints (AC-14). `compact` preserves them.
- BRD 2.3 lineage: event → `baseline_version` + `baseline_sample_count` → `stock_features` at
  `window_end = event_time` → `stock_price.source_record_key = topic:partition:offset` → Bronze record.

## Known limits

Optimistic concurrency on a single log, not an ACID table format; single NameNode demo; reference
files read from the driver's local disk; batch (not streaming) Kafka ingest; synthetic data only.
See [HANDOVER.md §7](HANDOVER.md).

# Decisions still needing evidence from the group (do not silently assume production correctness)

| ADR | Question / source evidence required | Pilot assumption | Impact |
|---|---|---|---|
| 01 | Provider unit and adjusted/unadjusted prices | VND; strict input unit | False price spikes if wrong |
| 02 | Is incoming 1m timestamp bar start or bar end? | bar_start `[t,t+1m)` | Bucket shifts by 1m |
| 03 | Exchange-level calendar, special sessions, suspensions, ticker changes | Sample HOSE dates only | Return/coverage corruption |
| 04 | Missing published_at fallback | Never infer from crawl | News count sparsity |
| 05 | Near-duplicates/syndication and alias false positives | Canonical URL exact; `duplicate_group` = hash of normalised title+body (`news.duplicate_group_policy: content_hash`, alternative `article`); Unicode case-insensitive whole-word alias match | Double counting / wrong mapping |
| 06 | Lookback and min sample calibration | 60 trading days, 20 observations | Sparse baselines |
| 07 | Daily effective date and historical snapshot protocol | Baseline for D from prior dates | Leakage and Speed mismatch |
| 08 | Which anomaly rule and min count? | z 3 or P99, observed>mean | Zero baseline false signals |
| 09 | Duration for merging consecutive anomalies | <=10m end-time gap | Too many / too few events |
| 10 | Required persistent Hive Metastore and concurrency? | Session temp views only until validated | FR-13/P1 incomplete |
| 11 | Which providers permit storing raw news HTML/text? | Sample data only, source policies pending | Legal/regulatory issue |
| 12 | Actual HDFS/Spark/Kafka image and Windows Docker setup? | Pinned demo image combination | Integration could fail |
| 13 | Atomic multi-table catalog pointer, writer concurrency? | **Implemented**: append-only commit log + create-exclusive CAS ([ADR-13](adr/ADR-13.md)) | Optimistic concurrency only, not Iceberg/Delta |
| 14 | Is 30m/1h event study clock time or trading minutes? | **Implemented default**: windows inside the event's session, crossing => `HORIZON_CROSSES_SESSION` ([ADR-14](adr/ADR-14.md)) — **to confirm** | Fewer complete horizons near lunch/close |
| 15 | Adjacent event grouping across lunch/day boundary? | Same symbol and <=10m elapsed | Event semantics |
| 16 | Rights, exchange, official corporate mapping and alias validity? | Synthetic reference | Mapping accuracy |
| 17 | Does upstream news contain raw HTML or only cleaned text? | Store exactly what arrived | Cannot reconstruct omitted HTML |
| 18 | Kafka retention, offsets and exactly-once required? | **Implemented**: offsets in the catalog commit, incremental ranges, anti-join; retention gaps reported | Batch ingest, not streaming |
| 19 | How to align historic late data with B5/B6/B7 when dates change? | **Implemented**: fingerprint-based `stale_downstream` warning, explicit `--from-job` re-run ([ADR-19](adr/ADR-19.md)) | Operator must act |
| 20 | Required expected volume/price exceptions (auction/halts/ATO/ATC)? | Only continuous sessions in demo | Missed or wrongly flagged bars |

## Production readiness gates

1. Obtain 2–4 approved news providers, actual source payload samples, legal rights, and official market calendar.
2. Verify bar semantics, units, adjustments and corp-action policy with provider per symbol.
3. Execute AC01–AC16 with Spark 3.5.9 + Java 17, then Docker HDFS/Kafka and Hive smoke (cannot certify from static review).
4. Load representative 30–100 symbols and 1–3 years; measure Spark shuffle, small files, offset replay, Kafka lag.
5. Build certified alias mapping review, near-duplicate policy and event ground-truth evaluation.
6. Upgrade catalog concurrency and durable atomic publish mechanism before unsupervised production execution.

## Policy values to confirm (v1.1 defaults)

| Decision | Default now | Where |
|---|---|---|
| Baseline publish policy | usable (READY + ZERO_STD) cohorts ≥ 80 % (dev/prod), 50 % (test); no publishing an older date without `--allow-older` | `baseline.publish_min_usable_ratio` |
| Duplicate group policy | `content_hash` (syndicated copies count once in `news_count`; `eligible_article_count` counts articles) | `news.duplicate_group_policy` |
| Retention / gc | keep newest 10 (dev) / 30 (prod) generations, every SUCCESS run, min age 24 h / 72 h | `storage.gc_*` |
| ZERO_STD tolerance | std ≤ 1e-9 × max(|mean|, 1) is treated as zero (float noise) | `rules.ZERO_STD_RELATIVE_EPS` |

# Bàn giao — Module lưu trữ (HDFS Data Lake) và Batch Processing (Spark B1–B7)

Phiên bản code: `1.1.0` (prototype/pilot, **chưa phải production**). Tài liệu này dành cho thành viên
khác của nhóm: dựng môi trường, chạy, vận hành, xử lý sự cố mà không cần hỏi tác giả code.
Hợp đồng tích hợp cho Speed Layer / Serving / Kafka producer nằm ở [INTEGRATION.md](INTEGRATION.md).

## 1. Chạy kiểm chứng bằng một lệnh

```powershell
# từ thư mục gốc repo, Windows PowerShell + Docker Desktop (Linux containers)
powershell -ExecutionPolicy Bypass -File .\scripts\verify_all.ps1
```

Lệnh này: build image Spark 3.5.9/Java 17 (có sẵn jar Kafka) → chạy toàn bộ pytest trong image
(repo mount **read-only**, test chỉ ghi vào `/tmp` của container) → dựng stack **cách ly**
`stockbatch-verify` (HDFS + Kafka, volume riêng, không publish port) → smoke HDFS và smoke Kafka →
`down -v` **chỉ** project đó. Log: `reports/runtime/verify_all_<timestamp>.log`.
Tuỳ chọn: `-SkipBuild`, `-SkipTests`, `-SkipSmoke`, `-KeepStack`, `-Image <tag>`.

Chạy riêng test (image đã build):

```powershell
docker run --rm --user 185 -e HOME=/tmp -e SPARK_LOCAL_IP=127.0.0.1 -e PYTHONDONTWRITEBYTECODE=1 -e PYTHONPATH=/opt/project/src:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip -v "E:/btl-bigdata/stock-batch-layer:/opt/project:ro" -w /opt/project --entrypoint python3 stock-batch-layer-spark-job:latest -m pytest -q -p no:cacheprovider tests --tb=short
```

> Không chạy test Spark bằng Python trên máy host nếu PySpark khác 3.5.9 (máy hiện có PySpark 4.1.1).
> Test thuần Python (`tests/unit`, `tests/contract`) chạy được trên host.

## 2. Kiến trúc lưu trữ

```text
Kafka (stock-price-topic, news-topic)
   │  ingest-kafka: [committed offset, latest) giải một lần, staging Parquet theo run_id
   ▼
Bronze  bronze/stock, bronze/news, bronze/bad_records        (partition ingest_date)
   │  B1 clean stock (precedence + quarantine)   B2 clean news (HTML, NFC, dedup, alias mapping)
   ▼
Silver  silver/stock_price (trade_date), silver/news + news_stock_mapping (publish_date)
   │  B3 features 5m   B4 news density 5m/30m/1h (zero-fill)
   ▼
Gold    gold/stock_features, gold/news_density (event_date)
   │  B5 baseline as-of (N ngày giao dịch trước)  B6 events  B7 event study
   ▼
Gold    gold/stock_baseline (baseline_date), gold/historical_events, gold/event_analysis (event_date)
        _system/baselines/{versions/<version>/, current.json, history/}   ← Speed Layer đọc
        _system/exports/<dataset>/generation=<g>/                      ← Serving/ES đọc
```

### 2.1 Layout HDFS (dưới `data_root`, mặc định `hdfs://namenode:8020/data`)

| Đường dẫn | Nội dung | Thay đổi? |
|---|---|---|
| `<area>/_versions/run_id=<run>/write=<n>/<partition_col>=<value>/part-*.parquet` | dữ liệu mọi dataset | **bất biến** |
| `_system/catalog/log/<generation 20 chữ số>.json` | 1 file / commit — **nguồn sự thật** | bất biến, tạo bằng create-exclusive |
| `_system/catalog/current.json` | bản sao generation mới nhất (cho reader cũ) | thay thế nguyên tử |
| `_system/runs/run_id=<run>/manifest.json` | RUNNING → COMMITTING → SUCCESS/FAILED, job, quality, delta | ghi đè nguyên tử |
| `_system/quality_reports/run_id=<run>/report.json` | kết quả từng quality gate | |
| `_system/quarantine/...` | dataset `quarantine` (partition `record_date`) | như dữ liệu |
| `_system/baselines/versions/<version>/` | Parquet baseline đã publish | bất biến |
| `_system/baselines/current.json`, `history/*.json` | con trỏ Speed Layer + lịch sử | thay thế nguyên tử / append |
| `_system/exports/<dataset>/generation=<g>/<start>_<end>/` | export cho Serving | |
| `_system/ingest_staging/run_id=<run>/` | staging Kafka (xoá sau commit, giữ khi lỗi) | gc dọn |
| `_system/hive_external/<table>/` | LOCATION gốc của bảng Hive external | |

Partition: bronze theo `ingest_date`; `stock_price`, `stock_features` theo `trade_date`; `news`,
`news_stock_mapping` theo `publish_date` (bài thiếu giờ vào `__NULL__`); `news_density`,
`historical_events`, `event_analysis` theo `event_date`; `stock_baseline` theo `baseline_date`
(= `valid_for_trading_date`); `quarantine` theo `record_date` (stock: `coalesce(trade_date, ingest_date)`,
news: `ingest_date`).

**1 file Parquet / partition**, sắp xếp theo khóa nghiệp vụ (`repartition` theo khóa partition +
`sortWithinPartitions`), giới hạn bởi `storage.max_records_per_file`. Không bao giờ quét đệ quy
`_versions` như một bảng: thư mục đó chứa cả dữ liệu chưa publish hoặc đã bị thay.

### 2.2 Catalog và vòng đời commit (ADR-13)

1. `Transaction(spark, cfg)` đọc generation mới nhất trong log (`base_generation`).
2. `begin()` hoặc lần `write()` đầu tiên ghi manifest **RUNNING** (crash vẫn để lại dấu vết).
3. Mỗi `write()` ghi partition vào đường dẫn bất biến mới, đọc lại Parquet để tính **số dòng +
   fingerprint nghiệp vụ** từng partition (xxhash64 không phụ thuộc thứ tự, bỏ cột provenance,
   làm tròn double) → `catalog.stats`.
4. Mỗi job B1..B7 chạy trong `tx.job(...)`: lưu delta catalog của job vào manifest (checkpoint).
5. Quality gates chạy trên các partition vừa ghi. FAIL → run FAILED, catalog giữ nguyên.
6. `commit()`: manifest **COMMITTING** → tạo `log/<base+1>.json` bằng create-exclusive (CAS). Nếu
   đã có (writer khác thắng) → `CommitConflictError`, không publish gì. Thành công → thay
   `current.json` nguyên tử → manifest **SUCCESS**.

Reader (`LakeReader`, `Transaction`) **ghim một generation** cho cả truy vấn nên không bao giờ thấy
snapshot lẫn lộn. Mất `current.json` không làm lake "rỗng" (đọc từ log). Mất cả log lẫn pointer mà có
run SUCCESS → `CatalogCorruptError` kèm hướng dẫn `rollback`.

## 3. Lệnh vận hành (CLI)

Trong Docker (stack dev): `docker compose --profile tools run --rm spark-job --env dev <lệnh>`;
local: `python -m stock_batch.cli --env test --data-root file:///abs/path <lệnh>`.

| Lệnh | Mục đích |
|---|---|
| `validate-config` | kiểm tra cấu hình (sau khi áp `DATA_ROOT`/`--data-root`) |
| `seed-demo` | ghi Bronze **giả lập** (2 mã × 5 ngày, có biến động + 1 bản tin đăng lại + 1 bài HTML) |
| `ingest-kafka --brokers kafka:9092 [--max-records-per-partition N]` | Kafka → Bronze tăng dần, offsets lưu trong commit |
| `run --job all\|B1..B7 --start D --end D` | chạy DAG/job theo khoảng ngày (thay đúng partition) |
| `run --from-job B5 --start D --end D` | chạy lại từ một job trở đi |
| `resume --run-id <FAILED run>` | chạy lại run lỗi, bỏ qua job đã xong nếu an toàn |
| `verify [--run-id R \| --generation G] [--no-gates]` | path tồn tại + rows + fingerprint + gates; exit 1 nếu FAILED |
| `show-current`, `list-runs [--limit N]` | xem catalog hiện tại / các run gần nhất |
| `rollback --run-id <SUCCESS run>` | publish generation MỚI bằng snapshot của run đó (kèm Kafka offsets) |
| `publish-baseline --version V [--allow-older]` / `show-baseline` | công bố baseline cho Speed Layer |
| `export --dataset X [--start --end] --format jsonl\|parquet [--out URI] [--include-content]` | export ghim generation cho Serving/ES |
| `storage-report [--generation G]` | số file, bytes, small files, bytes không còn tham chiếu |
| `gc [--keep-generations K] [--min-age-hours H] [--keep-success-runs N] [--apply]` | dọn dữ liệu không tham chiếu — **mặc định dry-run** |
| `compact --dataset X --start --end --files N` | gộp file, giữ provenance, chỉ commit khi fingerprint trước = sau |
| `register-hive --database DB [--embedded-metastore DIR]` | bảng Hive EXTERNAL theo generation + view `current_*` |

Quy trình hằng ngày gợi ý: `ingest-kafka` → `run --job all --start D --end D` → `verify` →
`publish-baseline --version <bl_D_...>` → `export` cho Serving. Xem `docs/runbooks/operations.md`.

## 4. Kịch bản lỗi và cách xử lý

| Tình huống | Hệ thống làm gì | Người vận hành làm gì |
|---|---|---|
| Job B1..B7 lỗi / gate FAIL | manifest FAILED (job lỗi ghi rõ), catalog giữ generation cũ | đọc `list-runs`, `_system/quality_reports/...`; sửa nguyên nhân; `resume --run-id` |
| Crash trước khi tạo log entry | manifest kẹt COMMITTING → `get_run` đối soát thành FAILED | `resume` hoặc chạy lại |
| Crash sau log entry, trước pointer | generation mới ĐÃ publish (log là nguồn sự thật); `current.json` cũ | không cần làm gì; `verify` báo `pointer_in_sync=false`, commit sau tự sửa |
| Hai run commit đồng thời | một bên thắng; bên kia `CommitConflictError`, manifest FAILED | chạy lại run thua trên snapshot mới |
| Mất `current.json` | reader dùng log, không ảnh hưởng | không cần làm gì |
| Mất cả log và pointer | `CatalogCorruptError` (không publish dữ liệu rỗng) | `list-runs` tìm run SUCCESS gần nhất → `rollback --run-id` |
| Dữ liệu sai sau một run | | `rollback --run-id <run tốt trước đó>` (tạo generation mới, khôi phục cả offsets Kafka) |
| File Parquet bị mất/sửa | `verify` báo `missing_paths` / `mismatches` / `read_error` | rollback về run có dữ liệu nguyên vẹn, rồi chạy lại phạm vi bị ảnh hưởng |
| Dữ liệu lịch sử được sửa (bar trễ, giờ đăng sửa) | run báo `stale_downstream` (ADR-19) | `run --from-job B5 --start <ngày stale đầu> --end <cuối>` |

`resume` chỉ tái dùng job đã xong nếu `config_hash` nghiệp vụ không đổi **và** catalog vẫn ở
`base_generation` của run lỗi; ngược lại nó chạy lại toàn bộ (an toàn với CAS).

## 5. Cấu hình

`configs/{dev,test,prod}/config.yml`; khóa mới có mặc định trong `config.DEFAULTS` và được validate:
`baseline.publish_min_usable_ratio`, `news.duplicate_group_policy` (`content_hash`|`article`),
`storage.{max_records_per_file, small_file_bytes, gc_keep_generations, gc_min_age_hours,
gc_keep_success_runs}`, `kafka.{stock_topic, news_topic}`. Khóa cũ `baseline.publish_require_ready: true`
(không có tỉ lệ) được hiểu là tỉ lệ 1.0.

`config_hash` chỉ tính trên cấu hình **nghiệp vụ** + nội dung file reference; bỏ `data_root`,
`environment`, `spark`, `storage`, `kafka`, các khóa vận hành. Vì vậy cùng dữ liệu ở dev/test/prod cho
cùng `baseline_version`.

## 6. Kiểm thử hiện có (xem số liệu thật ở `docs/BUILD_AUDIT.md`)

- `tests/unit` (thuần Python): `clean_text`, FS local (create-exclusive tranh chấp 16 luồng, replace),
  resolve catalog, config hash, rule B6/B7.
- `tests/integration`: B1–B7, storage v3, Kafka (không cần broker), CLI, E2E (AC-14, AC-15 tiêm lỗi
  B1..B7/gate/giữa log và pointer, xung đột, resume, truy vết BRD 2.3), Hive embedded (AC-16).
- `scripts/smoke/smoke_hdfs.py`, `scripts/smoke/smoke_kafka.py`: chỉ chạy trong stack cách ly.
- `scripts/benchmark.py`: dữ liệu tổng hợp do Spark sinh, đo thời gian từng job.

## 7. Giới hạn còn lại (trung thực)

- Toàn bộ dữ liệu kiểm thử là **giả lập**; chưa chạy với dữ liệu thật có bản quyền, lịch HOSE chính
  thức (lịch demo chỉ có 5 ngày tháng 10/2026), danh mục alias chính thức.
- HDFS demo là 1 NameNode + 1 DataNode (bde2020, Hadoop 3.2.1); chưa thử HA, Kerberos, phân quyền.
- Catalog dùng optimistic concurrency trên một log; không phải ACID table format (Iceberg/Delta).
  Không có time-travel theo thời gian, chỉ theo generation/run.
- File reference (lịch, symbol) đọc từ đĩa local của driver (Spark local mode). Chạy cluster mode cần
  đưa reference lên HDFS.
- Ingest Kafka là batch (không phải Structured Streaming); exactly-once ở mức catalog nhờ offsets lưu
  trong commit + anti-join, không dùng Kafka transactions.
- Spark local mode **bắt buộc** `spark.locality.wait=0s`: `LocalSchedulerBackend` không revive định kỳ,
  nên stage trộn partition ưu tiên driver (shuffle/cache/checkpoint) với partition HDFS (ưu tiên host
  DataNode) treo vĩnh viễn ở `(n + 0) / N` (đã tái hiện: ghi lại `bronze_bad_records` trong
  `ingest-kafka`, Spark UI cho thấy task không bao giờ được launch). `check_local_scheduling` chặn chạy
  khi thiếu cấu hình này; mọi session builder trong repo đã đặt sẵn. Ingest Kafka vẫn staging Parquet
  theo run_id (BRD 8.1) để đọc Kafka đúng một lần.
- Ngưỡng z/P99, `min_samples`, cửa sổ 60 ngày chưa được hiệu chỉnh trên dữ liệu thật (ADR-06/08).
- Docker Desktop trên máy phát triển có 8 GB RAM: đừng chạy test suite song song với stack smoke.

## 8. Đề xuất dọn dẹp (KHÔNG tự xoá)

Các file sau là bản vá/backup cũ, đã lỗi thời so với `src/` hiện tại: `src/stock_batch/**/*.bak_*`,
`_b1fix/`, `_b2v5/`, `_b2v5_1/`, `apply_*.py`, `*.zip` ở gốc repo, `HOTFIX_README.md`. Nên
`git init`, commit trạng thái hiện tại, rồi xoá chúng sau khi nhóm xác nhận.

# Build & validation audit — 2026-11-22 / 2026-11-23

Tài liệu này chỉ ghi những gì **đã chạy thật**, kèm lệnh, kết quả và file log. Dữ liệu dùng trong mọi
lần chạy là **giả lập** (`seed-demo`: 2 mã × 5 ngày giao dịch; payload mẫu `examples/KAFKA_PAYLOADS.jsonl`).
Bản audit cũ ngày 2026-11-10 (chưa chạy được Spark/Docker) đã được thay thế.

## 1. Môi trường

| Thành phần | Giá trị |
|---|---|
| Host | Windows 11, PowerShell 5.1, Docker Desktop, Docker Engine 29.1.2, VM ~8 GB RAM (`MemTotal=8161234944`), 20 CPU |
| Image test/smoke | `stock-batch-layer-spark-job:verify-20261122` (sha256:3ea08d31c19c…, build 2026-11-22) |
| Trong image | Spark 3.5.9 (`spark-core_2.12-3.5.9.jar`), OpenJDK 17.0.19, Python 3.10.12, pytest, PyYAML; jar Kafka `spark-sql-kafka-0-10_2.12-3.5.9`, `kafka-clients-3.4.1` |
| Stack smoke (cách ly) | project `stockbatch-verify`: `bde2020/hadoop-namenode|datanode:2.0.0-hadoop3.2.1-java8`, `apache/kafka:3.9.0`; volume riêng, không publish port |
| Python host | 3.11.7 với PySpark 4.1.1, **chỉ** dùng cho test thuần Python (`tests/unit`, `tests/contract`) |

Repo được mount read-only vào container (`-v "E:/btl-bigdata/stock-batch-layer:/opt/project:ro"`), test chỉ
ghi vào `/tmp` của container. Tag `:latest` cũ, thiếu jar Kafka, **không** được dùng.

## 2. Lệnh đã chạy và kết quả

### 2.1 Mốc ngày 2026-11-22 (trước khi sửa F1/F2)

| Lệnh | Kết quả | Log |
|---|---|---|
| `verify_all.ps1` (build + pytest + smoke) | pytest **129 passed, 1 skipped** (392,29 s); smoke_hdfs **12/12**; smoke_kafka **4/8 FAIL** | `reports/runtime/verify_all_20261122_121303.log` |
| `verify_all.ps1 -SkipBuild -SkipTests` (stack sạch) | smoke_hdfs **12/12**; smoke_kafka **8/8** | `reports/runtime/verify_all_20261122_122400.log` |

**Sự cố leftover stack (dẫn tới F2).** Lần 12:13 khởi động trên container `stockbatch-verify-*` còn sót từ
một lần chạy ngày 19/11 bị ngắt (khối `finally` không chạy `down -v`). Topic Kafka vẫn còn message cũ, nên
mọi số đếm đều gấp đôi. Các check FAIL: `first_ingest_counts {'stock': 4, 'news': 4, 'bad': 4}`,
`offsets_in_catalog`, `second_ingest_reads_nothing_new`, `replay_after_rollback_no_duplicate_offsets
{'stock_rows': 5}`. Code của module **không** sai; script kiểm chứng không idempotent.

### 2.2 Sửa F1 — contract dòng baseline cho Speed Layer

Lỗi: export baseline có `news_count` ở `5m/30m/1h` (102/506 dòng là `1h`), nhưng
`contracts/gold/speed_baseline_rows.schema.json` chỉ cho `window_size ∈ {5m, 30m}`.

| Lệnh (trong image, trừ dòng đầu) | Trước khi sửa | Sau khi sửa |
|---|---|---|
| `PYTHONPATH=src python -m pytest ... tests/contract -k news_window` (host) | `1 failed` — `AssertionError: ('dev', ['1h'])` | (trong `tests/unit tests/contract`) `57 passed` |
| `pytest tests/integration/test_speed_baseline_contract.py` | `1 failed` — `window_size values not in the contract enum: ['1h']` | `1 passed in 35.18s` |
| `pytest tests` (toàn bộ) | — | **131 passed, 1 skipped** in 438.85s |

Hành vi export và công thức B5 **không đổi** (chỉ sửa contract theo dữ liệu thật đã publish).

### 2.3 Sửa F2 — `verify_all.ps1` idempotent

Trước `up -d`, script gọi `down -v --remove-orphans` cho đúng project `stockbatch-verify` (giữ chốt chặn
tên project) và ghi `removed leftover isolated stack: ...` vào log nếu thật sự có thứ để dọn.

| Lệnh | Kết quả | Log |
|---|---|---|
| `verify_all.ps1 -SkipBuild -SkipTests -KeepStack -Image ...:verify-20261122` | smoke_hdfs **12/12**, smoke_kafka **8/8**; stack được cố ý **giữ lại** (3 container đang chạy) | `reports/runtime/verify_all_20261123_182408.log` |
| chạy lại ngay: `verify_all.ps1 -SkipBuild -SkipTests -Image ...:verify-20261122` | log in `removed leftover isolated stack: containers=[stockbatch-verify-datanode-1, stockbatch-verify-namenode-1, stockbatch-verify-kafka-1] volumes=[stockbatch-verify_verify_datanode, stockbatch-verify_verify_namenode]`; smoke_hdfs **12/12**, smoke_kafka **8/8**; teardown cuối thành công | `reports/runtime/verify_all_20261123_182653.log` |
| `docker ps -a / volume ls / network ls --filter label=com.docker.compose.project=stockbatch-verify` | rỗng; volume dev `stock-batch-layer_*` còn nguyên | — |

### 2.4 Lần chạy cuối (2026-11-23, sau F1–F4)

| Lệnh | Kết quả |
|---|---|
| `PYTHONPATH=src python -m pytest -q -p no:cacheprovider --basetemp <tmp> tests/unit tests/contract` (host) | **57 passed** in 4.42s |
| `docker run ... stock-batch-layer-spark-job:verify-20261122 -m pytest -q -p no:cacheprovider tests --tb=short -rs` | **132 passed, 1 skipped** in 426.86s — `SKIPPED [1] tests/integration/test_hive_p1.py:5: external Hive Metastore not configured` |
| `verify_all.ps1 -SkipBuild -Image stock-batch-layer-spark-job:verify-20261122` | `pytest_all_suites PASS 132 passed, 1 skipped in 477.50s`; `verify_stack_up PASS`; `verify_stack_ready PASS hdfs=True kafka=True`; `smoke_hdfs PASS checks passed=12 failed=0`; `smoke_kafka PASS checks passed=8 failed=0`; sau đó không còn container/volume `stockbatch-verify`. Log: `reports/runtime/verify_all_20261123_183712.log` |

Số test tăng từ 129 lên 132 gồm: `test_speed_baseline_contract.py` (F1), contract tĩnh
`test_every_configured_news_window_is_in_the_speed_row_contract` (F1), và
`test_batch_v3.py::test_ac06_suspended_symbol_day_is_not_interpolated` (đánh giá lại AC-06).

## 3. Nội dung các smoke (đều PASS ở lần chạy cuối)

- **smoke_hdfs (12 check)**: create-exclusive và atomic replace qua py4j trên NameNode thật (1 bên thắng
  trong 8 luồng tranh chấp); seed → B1..B7 với gates PASS; `verify` OK; 1 file/partition (45/45);
  `publish_baseline` `bl_2026-10-07_40526ac7_9507a2aa` (506 dòng: READY 219, ZERO_STD 287, version giống
  hệt ở mọi lần chạy, mọi root); export JSONL; **crash giữa commit log và pointer** (`log [1,2,3]`,
  `pointer 2`, generation 3 vẫn được publish); rollback tạo generation mới; gc dry-run không xoá gì.
- **smoke_kafka (8 check)**: produce payload mẫu + 2 payload lỗi (có header) lên broker thật; ingest lần 1
  (2 stock, 2 news, 2 bad record); offsets nằm trong catalog; header giữ thành map; ingest lần 2 không đọc
  gì mới; ingest tăng dần chỉ lấy bản ghi mới; rollback khôi phục offsets; replay sau rollback không
  nhân bản offset; Bronze giải mã qua B1/B2 (quarantine `INVALID_JSON`, `EMPTY_TITLE`).

## 4. Regression test cho các lỗi v1 (S1–S12, P1–P12)

| Lỗi | Test |
|---|---|
| S1 lost update | `test_storage_v3.py::test_s1_concurrent_commit_conflict_keeps_first_writer`, `test_e2e_v3.py::test_concurrent_pipeline_commit_conflict` |
| S2 mất pointer | `test_storage_v3.py::test_s2_missing_pointer_does_not_empty_the_lake`, `::test_s2_missing_pointer_and_log_with_success_runs_is_corrupt` |
| S3 B1 xoá quarantine news | `test_batch_v3.py::test_s3_b1_rerun_keeps_news_quarantine_of_same_day` |
| S4 reject backfill mất | `test_batch_v3.py::test_s4_backfill_reject_is_kept_under_its_trade_date` |
| S5 partition stale | `test_batch_v3.py::test_s5_b2_moves_article_without_stale_partition` |
| S6 compact ghi đè provenance | `test_cli_v3.py::test_s6_compact_preserves_provenance_and_fingerprints` |
| S7 baseline_version không tái lập | `test_e2e_v3.py::test_ac14_identical_rerun_and_other_root_are_deterministic`, `unit/test_v3_pure.py::test_config_hash_ignores_operational_keys` |
| S8 baseline ngày nghỉ | `test_batch_v3.py::test_s8_baseline_only_for_trading_days_and_removes_legacy_weekend` |
| S9 small files | `test_storage_v3.py::test_s9_one_sorted_file_per_partition` |
| S10 manifest | `test_storage_v3.py::test_s10_running_manifest_before_commit_and_reconciliation`, `::test_s10_crash_before_log_entry_reconciles_failed` |
| S11 Kafka | `test_kafka_v3.py::test_s11_*` (2 test) + smoke_kafka |
| S12 verify | `test_storage_v3.py::test_s12_verify_detects_missing_and_modified_files` |
| P1–P10 | `test_batch_v3.py::test_p1_*` … `::test_p10_*` |
| P11 upstream / fail_on_empty | `test_batch_v3.py::test_p11_upstream_missing_and_fail_on_empty` |
| P12 publish policy | `test_batch_v3.py::test_p12_publish_accepts_zero_std_with_legacy_require_ready` |

## 5. Chưa chạy / chưa kiểm chứng

1. Dữ liệu thật có bản quyền (giá, tin), lịch HOSE chính thức, danh mục 30–100 mã — chưa có nguồn.
2. Benchmark quy mô (`scripts/benchmark.py`, 30 mã × 60 ngày tổng hợp) — script có sẵn, **chưa ghi nhận
   kết quả** trong audit này.
3. Hive Metastore dùng chung (`test_hive_p1` SKIPPED); chỉ kiểm chứng metastore Derby embedded.
4. HDFS HA, Kerberos/phân quyền, nhiều DataNode; crash thật (kill -9 JVM, mất NameNode giữa lúc ghi). Crash
   hiện được mô phỏng bằng exception trong tiến trình.
5. Spark cluster mode (file reference đang đọc từ đĩa local của driver).

## 6. Ghi chú vận hành

- Không chạy pytest song song với stack smoke trên VM 8 GB. Ngày 19/11 Docker Desktop từng tự dừng giữa
  một lần chạy.
- Log của `verify_all.ps1` do PowerShell 5.1 ghi ở dạng **UTF-16LE**; đọc bằng PowerShell, hoặc
  `iconv -f UTF-16LE -t UTF-8`.

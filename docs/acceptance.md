# Ma trận nghiệm thu AC-01 … AC-16

Trạng thái dựa trên lần chạy thật ghi trong [BUILD_AUDIT.md](BUILD_AUDIT.md) (image
`stock-batch-layer-spark-job:verify-20261122`, Spark 3.5.9, Java 17). Toàn bộ dữ liệu là **giả lập**.
`PASS` = test tương ứng chạy và đạt trong lần chạy đó; `PARTIAL` = đạt phần đã kiểm thử, còn phần chưa
kiểm chứng (ghi lý do); `SKIPPED` = không chạy được trong môi trường này (ghi lý do).

| AC | Kịch bản (BRD 12.2) | Test / lệnh kiểm chứng | Trạng thái |
|---|---|---|---|
| AC-01 | B1 trùng 2 nguồn + high/low sai | `tests/integration/test_b1_b4.py::test_ac01_stock_dedup_invalid_and_source_priority`; `test_batch_v3.py::test_p1_malformed_json_is_invalid_json`, `::test_p2_symbol_with_two_effective_rows_is_not_duplicated` | PASS |
| AC-02 | B2 hai URL khác tracking param cùng bài | `test_b1_b4.py::test_ac02_ac03_canonical_url_two_symbols`; `test_batch_v3.py::test_p5_syndicated_copies_count_once` | PASS |
| AC-03 | B2 bài nhắc 2 mã, alias trùng | `test_b1_b4.py::test_ac02_ac03_canonical_url_two_symbols`; `test_batch_v3.py::test_p3_vietnamese_alias_unicode_case_and_nfd` | PASS |
| AC-04 | B3 5 bar 1m liên tục | `test_b1_b4.py::test_ac04_ac05_features_lunch_break` | PASS |
| AC-05 | B3 cuối sáng / đầu chiều | `test_b1_b4.py::test_ac04_ac05_features_lunch_break` | PASS |
| AC-06 | B3 mất bar, ngày nghỉ, mã tạm ngừng | `test_b1_b4.py::test_ac06_incomplete_bucket_is_not_filled` (mất bar); `test_batch_v3.py::test_ac06_suspended_symbol_day_is_not_interpolated` (mã không có bar cả một ngày giao dịch: không sinh feature, không nội suy giá, return đầu phiên sau là NULL, density vẫn zero-fill); `test_batch_v3.py::test_s8_baseline_only_for_trading_days_and_removes_legacy_weekend` (ngày nghỉ) | PASS — trên lịch demo; tạm ngừng *giữa phiên* và phiên đặc biệt (ATO/ATC) chưa có fixture (mục 20 trong [decisions_to_survey.md](decisions_to_survey.md)) |
| AC-07 | B4 không có bài ở 5m/30m/1h | `test_b1_b4.py::test_ac07_news_zero_materialized`; gate `news_density.zero_filled_grid_all_windows` | PASS |
| AC-08 | B4 tin đến trễ và tin thiếu `published_at` | `test_b1_b4.py::test_ac08_missing_publish_time_never_counts` (thiếu giờ); `test_batch_v3.py::test_s5_b2_moves_article_without_stale_partition` (bản sửa đến sau, đổi ngày đăng, rebuild sạch); `::test_adr19_changed_history_flags_stale_downstream` (bar đến trễ → cảnh báo baseline/event ngày sau); `test_kafka_v3.py::test_s11_incremental_exactly_once_with_headers_and_offsets` (replay không nhân bản) | PASS — lineage qua `source_record_key`/offset; downstream của dữ liệu trễ được **cảnh báo**, không tự chạy lại (ADR-19) |
| AC-09 | B5 không dùng ngày hiện tại | `test_b5_b7.py::test_ac09_ac10_baseline_asof`; gate `stock_baseline.no_future_leakage` | PASS |
| AC-10 | B5 std=0 / P99=0 / ít mẫu | `test_b5_b7.py::test_ac09_ac10_baseline_asof`; `test_batch_v3.py::test_float_noise_std_is_zero_std_not_ready`; `tests/unit/test_v3_pure.py::test_signal_state_distinguishes_unknown_from_normal` | PASS |
| AC-11 | B6 composite FPT | `test_b5_b7.py::test_ac11_composite_event`; `test_e2e_v3.py::test_e2e_pipeline_produces_composite_event_and_passes_gates` | PASS |
| AC-12 | B6 gộp 2 bucket kề nhau | `test_b5_b7.py::test_ac12_merge_two_adjacent_event_buckets`; `test_batch_v3.py::test_events_on_different_days_are_never_merged` | PASS |
| AC-13 | B7 thiếu sau 1h / cuối dữ liệu | `test_b5_b7.py::test_ac13_study_unknown_future`; `test_batch_v3.py::test_p8_intraday_horizon_does_not_cross_session`, `::test_p9_before_bar_must_be_complete_and_flag_never_null` | PASS |
| AC-14 | Chạy lại B1–B7 cùng input + config | `test_e2e_v3.py::test_ac14_identical_rerun_and_other_root_are_deterministic` (fingerprint, `baseline_version`, `event_id` giống nhau, kể cả khác `data_root`); smoke HDFS: cùng `bl_2026-10-07_40526ac7_9507a2aa` ở mọi lần chạy | PASS |
| AC-15 | Lỗi giữa chừng → current vẫn là bản cũ | `test_e2e_v3.py::test_ac15_failure_in_each_job_keeps_previous_generation` (B1..B7), `::test_ac15_gate_failure_keeps_previous_generation`, `::test_ac15_crash_between_log_and_pointer`, `::test_concurrent_pipeline_commit_conflict`; `test_storage_v3.py::test_s10_*`; **HDFS thật**: `smoke_hdfs.py` check `crash_after_log_is_published` | PASS — crash được mô phỏng bằng exception trong tiến trình (kể cả trên HDFS thật); chưa thử kill -9 JVM hay mất NameNode/DataNode giữa lúc ghi |
| AC-16 | Hive external + partition pruning | `test_hive_embedded.py::test_ac16_hive_external_tables_partition_pruning` (Derby embedded: EXTERNAL, `inputFiles` chỉ thuộc partition lọc, có `PartitionFilters`) | PASS (embedded) |
| AC-16 | (metastore dùng chung) | `test_hive_p1.py::test_ac16_hive_external_reads_published_data` | SKIPPED — cần `HIVE_METASTORE_URIS` của một Hive Metastore ngoài, môi trường không có |

Ngoài AC, các lỗi S1–S12 / P1–P12 của v1 đều có regression test (xem bảng trong BUILD_AUDIT).

**Definition of Done (BRD 12.3):** FR-01..FR-12 và AC-01..AC-15 có test PASS trên dữ liệu giả lập
5 ngày × 2 mã, smoke HDFS/Kafka thật PASS, chạy một lệnh được (`scripts/verify_all.ps1`). Yêu cầu
"1 bộ dữ liệu thật dùng đánh giá tính thực tế" **chưa đáp ứng**, nên module chưa được xem là
production-ready.

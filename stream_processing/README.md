# Spark Structured Streaming – phát hiện Market Event (phần Speed Layer)

Phần xử lý realtime của đề tài *"Nền tảng Big Data phát hiện sự kiện bất thường & mối liên hệ giá/khối lượng – mật độ tin tức"*.
Bám theo mục 7 (Stream Processing) và 8 (Cơ chế phát hiện) của bản đề xuất.

Hai nguồn dữ liệu, chọn bằng biến môi trường `BIGDATA_SOURCE`:

- **`real` (mặc định)**: dữ liệu thật do nhánh `crawl-data` (Thắng) thu thập: nến 1 phút của **30 mã VN30** (11/2023 → nay)
  và **9.318 bài CafeF** (10/2025 → nay). Code nằm trong repo nhóm (`stream_processing/`) nên tự đọc `../data/bronze`;
  nếu để code ở chỗ khác thì clone repo nhóm cạnh thư mục code hoặc đặt `BIGDATA_TEAM_REPO`.
  Danh mục mã + alias lấy từ `data_ingestion/reference_symbols.csv` của nhóm.
- **`sim`**: 5 mã giả lập có kịch bản cấy sẵn và dữ liệu bẩn cố ý, dùng cho test có đáp án biết trước.

Kafka / HDFS / Elasticsearch được thay bằng thư mục file / Parquet cục bộ / SQLite để chạy được trên một máy.
Thông điệp phát lại đúng data contract của topic `stock-price-topic` / `news-topic` trong README của nhánh `crawl-data`,
nên khi nối Kafka thật chỉ cần thay `streaming/sources.py` (và `streaming/sinks.py` cho Elasticsearch).
Mỗi nguồn có thư mục làm việc riêng: `data/real/`, `data/sim/`.

## Chạy với dữ liệu thật

```powershell
.\.venv\Scripts\python -m batch.build_baseline     # baseline từ 250 ngày giao dịch trước REPLAY_DATE (~2 phút)
.\.venv\Scripts\python -m replay.real_day          # phát lại ngày REPLAY_DATE (mặc định 2026-11-25) -> data/real/stream_in
$env:BIGDATA_MAX_FILES=3; .\.venv\Scripts\python -m streaming.run --reset   # ~4 phút
.\.venv\Scripts\python -m streaming.report         # danh sách Market Event, dead-letter
```

Đổi ngày phát lại: `python -m replay.real_day --date 2026-11-20` (đổi luôn `BIGDATA_REPLAY_DATE` rồi dựng lại baseline,
để ngày phát lại không nằm trong dữ liệu baseline). Chạy giống realtime: `python -m replay.real_day --pace 15`
song song với `python -m streaming.run --mode continuous`.

### Những gì đã sửa để chạy được trên dữ liệu thật

| Vấn đề trong dữ liệu thật | Cách xử lý |
|---|---|
| Timestamp có múi giờ (`2026-11-25 09:15:00+07:00`), tin dạng `2026-11-25T10:59:00.000000` | `try_to_timestamp` không cần định dạng; chuỗi hỏng thành null → dead-letter, không làm sập job |
| 26/10–10/11/2026 mỗi phút có 2 nến (VNDirect `09:15:00` + Mirae `09:15:56`), làm volume bị cộng đôi | Làm tròn timestamp về phút rồi mới loại trùng; batch ưu tiên VNDirect |
| Phút không có giao dịch thì không có nến; window 14:45 chỉ có 1 nến ATC | Dùng mọi window cho baseline; `is_final` tính theo event time đã thấy, không theo số nến |
| Tin rất thưa theo từng mã (chỉ 17% bài nhắc tới mã VN30; cả năm vài lần một mã có ≥ 2 bài/30') | Baseline tin gộp mọi khung giờ của mã; ngưỡng tối thiểu 2 bài (giả lập: 4) |
| "VND" vừa là mã vừa là ký hiệu tiền đồng; "Dung Quất" chủ yếu là lọc dầu BSR, không phải Hòa Phát | Ticker VND chỉ tính khi có ngữ cảnh ("cổ phiếu VND", "(VND)"); bỏ alias "Dung Quất" |
| ~18% bài CafeF đăng lúc 00:00–01:00 | Ngoài khung baseline tin 07:00–20:00 nên không tạo event lúc nửa đêm |

## Chạy với dữ liệu giả lập

```powershell
$env:BIGDATA_SOURCE="sim"
.\.venv\Scripts\python -m simulator.gen_history      # 120 ngày lịch sử (không có sự kiện cấy)
.\.venv\Scripts\python -m batch.build_baseline
.\.venv\Scripts\python -m simulator.gen_live         # ngày "realtime" + sự kiện cấy sẵn + dữ liệu bẩn
.\.venv\Scripts\python -m streaming.run --reset
.\.venv\Scripts\python -m streaming.report
```

## Test

```powershell
.\.venv\Scripts\python -m pytest tests/test_detect.py     # 17 unit test (~1 phút)
.\.venv\Scripts\python -m pytest tests/test_e2e.py        # 7 test, dữ liệu giả lập có đáp án biết trước (~6-7 phút)
.\.venv\Scripts\python -m pytest tests/test_e2e_real.py   # 4 test, dữ liệu thật ngày có nến trùng nguồn (~5 phút)
```

Chạy `availableNow` với mặc định (1 file/micro-batch, sát thực tế nhất) chậm trên Windows vì mỗi thao tác checkpoint đều gọi winutils.
Đặt `BIGDATA_MAX_FILES=3` để đọc 3 file/lô, nhanh hơn khoảng 5 lần với kết quả phát hiện giống hệt (test e2e dùng giá trị này).

Lưu ý chế độ `continuous` trên máy Windows: 7 query chạy song song, mỗi vòng micro-batch mất ~14 giây
(chủ yếu do I/O checkpoint), nên dùng `--pace 15` trở lên để luồng theo kịp. Khi dừng (hết `--duration` hoặc Ctrl-C) Spark có thể
in vài `StackOverflowError` ở luồng của query – đó là nhiễu của bước ngắt batch đang chạy, không mất dữ liệu (ghi idempotent, batch dang dở chạy lại từ checkpoint).

## Đáp án kiểm thử với dữ liệu giả lập (`simulator/scenarios.py`)

| Mã | Giờ | Cấy vào | Kết quả mong đợi (và đã đạt) |
|---|---|---|---|
| FPT | 10:35 | giá +1.5%, volume ×8, 6 bài trong 22' | `PRICE+VOLUME+NEWS` |
| VCB | 13:20 | giá −1.2%, volume & tin bình thường | `PRICE_ONLY` |
| HPG | 11:00 | volume ×10, 5 bài trong 18', giá phẳng | `NEWS+VOLUME` |
| VNM | 14:00 | 6 bài trong 23', giá/volume bình thường | `NEWS_ONLY` |
| MWG | – | không cấy gì (nhóm đối chứng) | ≤ 4 event giả |

Dữ liệu bẩn cố ý chèn vào luồng: nến gửi trùng, nến đến trễ 44 phút (volume 9.999.999), JSON bị cắt dở,
nến volume âm / high < low / mã lạ, bài trùng và bài đăng lại ở nguồn khác, bài thiếu `published_at`.
Test e2e xác nhận cả 255 window khớp tuyệt đối với đáp án OHLCV và 6 bản ghi lỗi đều vào dead-letter đúng lý do.

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `config.py` | Ngưỡng, kích thước window, watermark, đường dẫn, danh sách mã + alias |
| `streaming/stock_stream.py` | Nến 1' → window 5' (event-time + watermark) → so baseline → `price_anomaly`, `volume_anomaly` |
| `streaming/news_stream.py` | Loại trùng → map bài→mã → đếm 5'/30'/1h → `news_burst` |
| `streaming/detect.py` | Công thức z-score / P99 / điều kiện flag (DataFrame thuần, test được) |
| `streaming/features.py` | Tính OHLCV theo window – **dùng chung** với batch để baseline và realtime cùng công thức |
| `streaming/sinks.py` | Serving cục bộ (SQLite): upsert theo khóa, view `market_event` (tổ hợp cờ, OPEN/COMPLETED, `*_after_30m`) |
| `streaming/dlq.py` | Dead-letter: bản ghi hỏng/sai giá trị kèm lý do |
| `batch/build_baseline.py` | Stand-in của B1–B5: dựng baseline mean/std/P95/P99 |
| `replay/real_day.py` | Phát lại 1 ngày thật từ Bronze theo data contract Kafka, kèm đáp án OHLCV đã bỏ trùng |
| `simulator/` | Sinh dữ liệu giả lập lịch sử + live, kịch bản bất thường cấy sẵn (`scenarios.py`) |

## Các quyết định thiết kế (và vì sao)

- **Update mode + ghi idempotent theo `(symbol, window_start)`**: window hiện tại hiện ngay trong micro-batch, bản mới nhất ghi đè;
  `is_final` (trong view) cho biết event time đã vượt qua cuối window. Không bị kẹt window cuối phiên chờ watermark (append mode phải chờ dữ liệu ngày hôm sau).
- **Baseline nạp trong `foreachBatch` và làm mới theo TTL**: static DataFrame đọc từ file không tự thấy file mới khi query đang chạy.
- **Không có baseline / ít hơn 30 mẫu / std ≈ 0 → không gắn flag** (không báo bừa).
- **Dedup bar theo `(symbol, phút)`, dedup tin theo hash nội dung**: collector gửi lại cùng một nến sẽ không làm volume bị cộng đôi;
  bài đăng lại ở nguồn khác (khác id/url) cũng bị gộp. Khi hai nguồn gửi cùng một phút, stream giữ bản đến trước.
- **Window tin tức 30'/1h trượt theo lưới 5'** và gán vào slot kết thúc → khóa `(symbol, slot)` khớp trực tiếp với window giá.
- **Market Event là VIEW** trên bảng `signals`: cờ của stock stream và news stream được gộp theo khóa; cờ đổi thì event tự đổi, không còn "event ma".
  `status` = COMPLETED khi đã thấy đủ 30' dữ liệu sau sự kiện (theo event-time, không theo đồng hồ máy).
- Timestamp luôn chuyển thành chuỗi **trong Spark** (theo session timezone `Asia/Ho_Chi_Minh`) trước khi `collect()`.

## Giới hạn đã biết

- `return` là `close/open - 1` **trong window** (không phải so với close của window trước) – streaming không có `lag`.
- Event liền kề: tin dồn dập làm cờ `news_burst` bật ở nhiều slot liên tiếp (cửa sổ 30' trượt) → cần gộp event liền kề khi hiển thị.
- Quy tắc "|z| ≥ 3 **hoặc** ≥ P99" của đề xuất sinh ~1% báo động giả theo thiết kế (xem số liệu nhóm đối chứng MWG).
- Chưa có: nguồn Kafka thật, ghi Elasticsearch, đo độ trễ end-to-end, sentiment.
- `return_after_30m` để trống khi 30' sau rơi vào giờ nghỉ trưa hoặc sau giờ đóng cửa.
- **Windows + Python 3.13**: Python worker của PySpark 4.0 crash → code không dùng UDF / `createDataFrame(list)`.
  Trên Windows cần `winutils.exe` + `hadoop.dll` (bản hadoop-3.3.6, ví dụ từ github.com/cdarlint/winutils) đặt vào
  `stream_processing/hadoop/bin/`; `spark_env.py` tự trỏ `HADOOP_HOME` tới đó. Hai file này không được commit lên repo.

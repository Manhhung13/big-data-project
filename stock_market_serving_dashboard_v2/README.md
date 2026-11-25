# Elasticsearch Serving Layer và Dashboard chứng khoán Việt Nam

Module thực hiện phần Serving Layer → End-user Dashboard trong đề xuất: truy vấn giá/volume, mật độ tin, timeline sự kiện và phân tích lịch sử. Có bộ nạp dữ liệu Parquet đang có trong thư mục và sink nhận kết quả Batch/Speed. Dữ liệu crawl và dữ liệu demo được lưu ở các index riêng.

**Đã nạp toàn bộ lịch sử ngày 11/11/2026:** 4.638.969 bản ghi giá của 30 mã (13/11/2023–10/11/2026), 994.330 feature 5 phút, 8.905 bài báo tương ứng 9.023 bản ánh xạ và 795 cửa sổ mật độ tin có bài. Số lượng đã được đối chiếu với nguồn, không có lỗi bulk. Chọn **Dữ liệu crawl → Toàn bộ lịch sử** trên dashboard để xem; bằng chứng ở `reports/full_import_verification.json`. Lệnh import mặc định vẫn chỉ chọn 30 ngày; dùng `--days 0 --load` để nạp lại toàn bộ.

**Chưa có trong dữ liệu được cung cấp:** baseline, anomaly flags, historical_events, event_analysis và đầu ra Spark Streaming. Bộ nạp Bronze chỉ tạo view giá, return 5 phút và đếm tin đã ánh xạ; không tự sinh z-score hoặc sự kiện. Kafka, Spark và HDFS thuộc upstream, chưa được triển khai trong module này.

## Chạy với dữ liệu crawl

Nhánh này không chứa bản sao dữ liệu Parquet. Nếu chưa có thư mục `big-data-project-crawl-data/`, lấy dữ liệu từ nhánh của nhóm bằng lệnh sau tại thư mục chứa README này:

```powershell
git clone --single-branch --branch crawl-data https://github.com/Manhhung13/big-data-project.git big-data-project-crawl-data
```

PowerShell, tại thư mục chứa README này, Python 3.11/3.12 và Docker Desktop:

```powershell
docker compose up -d
# Nếu chưa có .venv:
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:ES_DATASET = 'real'

# Dry run: đọc, kiểm tra và xuất báo cáo, không ghi Elasticsearch.
.\.venv\Scripts\python.exe -X utf8 import_parquet.py

# Nạp 30 ngày cuối theo thời gian trong nguồn, cho tất cả mã.
.\.venv\Scripts\python.exe -X utf8 import_parquet.py --load

.\.venv\Scripts\python.exe -m streamlit run dashboard.py --server.address=127.0.0.1
```

Mở http://localhost:8501. Chọn **Dữ liệu crawl** và mã cổ phiếu. Mặc định dashboard chọn 7 ngày cuối dữ liệu đã nạp, không phụ thuộc dữ liệu đó cũ bao lâu. Có tùy chọn 30 ngày, toàn bộ lịch sử **đã nạp**, ngày tùy chọn và 24 giờ gần hiện tại. Chế độ 24 giờ cập nhật mốc thời gian khi fragment chạy lại mỗi 15 giây. Dữ liệu lịch sử không tự biến thành dữ liệu realtime.

Ví dụ phạm vi nạp khác:

```powershell
# Toàn bộ lịch sử khoảng 4,64 triệu bản ghi giá; cần thêm thời gian và dung lượng ES.
.\.venv\Scripts\python.exe -X utf8 import_parquet.py --days 0 --load

# Chỉ một số mã, trong [01/10, 01/11) theo giờ Việt Nam.
.\.venv\Scripts\python.exe -X utf8 import_parquet.py --symbols FPT HPG VNM --start 2026-10-01 --end 2026-11-01 --load

# Nếu dữ liệu ở đường dẫn khác:
.\.venv\Scripts\python.exe -X utf8 import_parquet.py --data-root 'D:\duong-dan\big-data-project-crawl-data\data' --load
```

Mặc định `data-root` được tính theo vị trí script: `big-data-project-crawl-data/data`. Giá và feature được giới hạn bởi `--symbols`; tin được đọc và ánh xạ theo toàn bộ danh sách mã nguồn, kể cả khi chỉ nạp một vài mã giá. Các lần nạp cộng dồn vào index, không xóa dữ liệu ngoài khoảng được chọn.

`reports/import_report.json` ghi số dòng đầu vào, không hợp lệ, trùng khóa, thời gian đầu/cuối, số dòng chọn và số document gửi cho từng index. Chế độ `load` trả exit code khác 0 nếu có lỗi bulk. Không xem số document gửi là số document mới: replay cùng khóa có thể được xử lý `noop`.

## Dữ liệu thực tế và chuẩn hóa

Kiểm kê ngày 11/11/2026 từ 31 file Parquet:

| Nhóm | Kết quả |
|---|---|
| Giá | 30 mã, 4.638.969 dòng, 13/11/2023–10/11/2026 |
| Kiểm tra giá | Không phát hiện trùng `(symbol,timestamp)` hoặc vi phạm OHLCV trong các kiểm tra hiện có |
| Tin | 8.905 bài, một nguồn `cafef`, không có sẵn trường symbol |
| Múi giờ giá | Chuỗi timestamp có offset `+07:00`; giữ thời điểm gốc rồi chuyển UTC |
| Múi giờ tin | `published_at` không có timezone; mặc định diễn giải là giờ Việt Nam, có thể đổi `--news-timezone` |
| Ánh xạ ticker | 682 bài có ticker khớp chính xác trong tiêu đề/nội dung, 8.223 bài chưa ánh xạ, trên toàn bộ lịch sử tin |

Những số liệu này phản ánh file được cung cấp, không xác nhận nguồn đã đầy đủ hoặc giá đã điều chỉnh. `content` có thể chỉ là đoạn mô tả ngắn. Cần thống nhất đơn vị giá và volume từng nến hay lũy kế với người làm crawler trước khi dùng các tổng volume cho đánh giá thực nghiệm. Bộ nạp hiện giả định volume từng bản ghi có thể cộng được và giữ nguyên đơn vị nguồn; không tự chia hoặc nhân giá.

- Kiểm tra timestamp giá phải có timezone, OHLC hữu hạn/dương, low ≤ open/close ≤ high, volume ≥ 0. Dòng lỗi bị loại và đếm trong báo cáo.
- Giá được sort theo thời gian, trùng `(symbol,timestamp)` giữ dòng sau cùng. Không ép giây của nguồn về đầu phút, tránh gộp nhầm bản ghi.
- Feature resample theo cửa sổ 5 phút `[start,end)`. Return dùng close cửa sổ trước; null ở đầu chuỗi hoặc khi có gián đoạn qua nghỉ trưa/qua đêm. Không điền nến vào khoảng không có giá.
- Không có baseline nên không điền `False`/`0` vào cờ anomaly và z-score: trường chưa đánh giá được để thiếu/null.
- Bài báo khớp ticker **phân biệt hoa/thường, có ranh giới từ** trong title/content. Một bài khớp nhiều mã tạo một document mỗi `(article_id,symbol)`. Bài chưa khớp vẫn được giữ với symbol `__UNMAPPED__` và tìm được bằng tùy chọn tất cả bài.
- News density đếm bài đã dedup theo symbol trong cửa sổ 5 phút. Chỉ lưu cửa sổ có bài; thiếu cửa sổ không chứng minh rằng không có tin trên thị trường. Ngày trước khi nguồn tin bắt đầu không được coi là baseline không có tin.

Có thể bổ sung alias **đã rà soát** bằng file JSON, ví dụ `{"MWG": ["Thế Giới Di Động"]}`, rồi chạy `--aliases aliases.json`. Quy tắc và alias khớp được ghi vào từng bài. Đổi quy tắc ánh xạ cần rebuild view trong index mới hoặc một quy trình thay thế có kiểm soát: chạy lại import là bổ sung/noop, không xóa các ánh xạ cũ.

## Hợp đồng dữ liệu và hợp nhất Batch/Speed

`ES_URL` mặc định `http://localhost:9200`. `ES_DATASET` mặc định `real`:

| View | Index dữ liệu crawl | Khóa document |
|---|---|---|
| price | `real-stock-price-v1` | `symbol|UTC timestamp` |
| features | `real-stock-features-v1` | `symbol|UTC window_start` |
| news | `real-stock-news-v1` | `article_id|symbol` |
| density | `real-news-density-v1` | `symbol|5m/30m/60m|UTC window_start` |
| events | `real-market-events-v1` | `symbol_UTC event_time` |

Symbol được trim/uppercase; timestamp đồng nhất về UTC ISO8601 trước khi tạo ID, nên `10:00+07:00` và `03:00Z` cùng khóa. Khóa news giữ nguyên article_id do crawler cấp. Cần upstream cấp cùng article_id cho các lần crawl lại cùng bài; dedup URL/content giữa những ID khác nhau vẫn là trách nhiệm B2.

`init_indexes.py` tạo index thiếu và thêm mapping vào index có sẵn; không xóa document hoặc tự sửa kiểu field đã tồn tại. Nếu có xung đột kiểu dữ liệu, phải tạo version index mới và reindex có kiểm soát.

`serving_sink.py` nhận JSON Lines do upstream xuất:

```powershell
$env:ES_DATASET = 'real'
# Thay đường dẫn bằng file kết quả thực sự của nhóm; lệnh đầu chỉ validate.
.\.venv\Scripts\python.exe -X utf8 serving_sink.py events 'D:\exports\historical_events.jsonl'
.\.venv\Scripts\python.exe -X utf8 serving_sink.py events 'D:\exports\historical_events.jsonl' --load
# kind còn có price, features, news, density.
```

Mỗi document cần `symbol`, `origin` (`batch`/`speed`), `processed_at` có timezone và `source_version` nguyên dương tăng theo từng khóa và origin. Các trường đặc thù theo mapping ở `init_indexes.py`. Event cần `event_time`, ba cờ boolean, ít nhất một cờ true và `status` (`OPEN`/`COMPLETED`). Sink tính/kiểm tra `event_type` từ tổ hợp cờ, chuẩn hóa ID và không nhận chỉ số hậu sự kiện ở event OPEN. NaN/Infinity bị từ chối. CLI validate toàn file trước khi bắt đầu ghi.

Quy tắc phân xử được thực thi atomically bằng scripted upsert:

1. Batch là kết quả cuối cùng có thẩm quyền cho một khóa; Speed không được ghi đè Batch.
2. Với đầu ra upstream có version dương và cùng origin, chỉ `source_version` lớn hơn mới thay thế. Replay cùng version là noop, kể cả payload khác. Payload phải là snapshot đầy đủ; không phải partial patch.
3. `COMPLETED` không được quay lại `OPEN`. Upstream phải tính và cập nhật các chỉ số hậu sự kiện khi đã đủ quan sát; sink không tự hoàn thành event theo đồng hồ.
4. Bộ nạp Bronze dùng version 0; reimport được cập nhật các view Bronze còn version 0. Kết quả Batch chuẩn có version > 0 được quyền thay thế và được bảo vệ khỏi reimport Bronze. Import vào khoảng timestamp phải căn biên 5 phút để không tạo feature từ một phần cửa sổ.

Đây là **quy ước tích hợp của module**, cần nhóm Batch/Speed thống nhất trước khi kết nối. Chỉ gửi `origin=batch` sau khi view đã được chốt; cần quy trình riêng nếu muốn mở lại một cửa sổ đã chốt. Spark có thể gọi `normalize_document()` và `upsert_action()` trong `foreachBatch`/`foreachPartition`, rồi dùng bulk helper. Chưa có kết nối Spark đang chạy được kiểm chứng trong workspace này.

## Dashboard và giới hạn hiển thị

- **Tổng quan thị trường:** số event toàn kỳ, composite, số bài duy nhất xấp xỉ bằng cardinality (tránh cộng trùng bài liên quan nhiều mã), bài chưa ánh xạ, số event theo ngày Việt Nam và top mã.
- **Chi tiết cổ phiếu:** OHLC theo bucket (first open, max high, min low, last close), tổng volume, biến động đầu/cuối khoảng và Event Panel có lọc type/status tại Elasticsearch.
- **Tin tức:** mật độ từ cửa sổ nguồn 5 phút, tìm title/content, link mở bài, danh sách tin độc lập với event, liên kết ±30 phút khi có event.
- **Lịch sử:** tần suất/co-occurrence theo tổ hợp cờ, mean return trước/sau trên tất cả event COMPLETED có số đo, kèm cỡ mẫu. Đây là thống kê mô tả, chưa phải event study có kiểm định hay chứng minh nhân quả.

Khoảng ngày dashboard là `[đầu ngày bắt đầu, đầu ngày sau ngày kết thúc)` theo UTC+7. Riêng liên kết tin quanh event bao gồm cả hai biên `[T-30m,T+30m]`. Timezone lưu trong ES là UTC, hiển thị giờ Việt Nam. Chọn độ gộp tự động 5 phút/1 giờ/1 ngày để tránh quá nhiều histogram buckets.

Danh sách event/marker giới hạn 200, tin giới hạn 100 và luôn ghi tổng phù hợp. Thống kê dùng aggregation trên toàn bộ khoảng chọn, không tính từ danh sách bị giới hạn. Không có phân trang sâu trên UI hiện tại. Không có event được nạp nghĩa là thiếu kết quả upstream, không có nghĩa đã xác nhận thị trường bình thường.

## Demo riêng

Các index demo giữ tên cũ không có prefix `real-`; dữ liệu demo hiện có không bị xóa. `seed_demo.py` và `simulate_stream.py` từ chối chạy khi `ES_DATASET` khác `demo`.

```powershell
$env:ES_DATASET = 'demo'
.\.venv\Scripts\python.exe -X utf8 init_indexes.py
.\.venv\Scripts\python.exe -X utf8 seed_demo.py
.\.venv\Scripts\python.exe -X utf8 simulate_stream.py
```

Chọn **Demo giả lập** trong dashboard. Simulator cập nhật cùng nến mỗi 15 giây, giữ high/low tích lũy trong cửa sổ và article ID không trùng khi khởi động lại. Simulator không phải Kafka/Spark, không tuân theo lịch giao dịch thật, không hoàn thành event OPEN hoặc thực hiện event study. Demo lịch sử không có giá trị thực nghiệm. Sau khi thử, `Ctrl+C` và đặt lại `$env:ES_DATASET='real'` trước khi import dữ liệu thật.

## Kiểm chứng

```powershell
# Unit tests, không cần ES. Các bài integration/AppTest được skip có giải thích.
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -v

# ES phải chạy; real và demo phải đã nạp để kiểm tra dashboard.
$env:RUN_ES_TESTS = '1'
$env:RUN_DASHBOARD_TESTS = '1'
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -v
```

Integration test chỉ tạo/xóa index tạm `serving-test-<uuid>-*` do chính test tạo, không xóa index dữ liệu. Test xác minh replay/version/Batch authority, OPEN→COMPLETED, OHLCV aggregation, biên thời gian, density không cộng chồng, thống kê 220 event khi chỉ hiển thị 200, và liên kết tin đúng mã. AppTest thử real/demo, ngày tùy chọn, realtime, tìm tin và bộ lọc event.

Xem `REVIEW.md` và `reports/verification.json` để biết kết quả thực tế và phần chưa kiểm chứng. Bản sao mã trước sửa nằm trong `.review-backup/`. Không sửa file Parquet hoặc báo cáo Word gốc.

## Vận hành local và tài liệu

`compose.yaml` đang dùng Elasticsearch/Kibana 9.5.6, tắt auth và bind port vào loopback. Đây là cấu hình local. `docker compose down` dừng dịch vụ; thêm `-v` sẽ xóa dữ liệu volume, không dùng nếu cần giữ dữ liệu. Production cần thêm auth/TLS, quyền truy cập, lifecycle/retention, backup, giám sát và DLQ. ES phục vụ truy vấn, không thay Data Lake.

- [Elasticsearch Python bulk helpers](https://www.elastic.co/docs/reference/elasticsearch/clients/python/client-helpers)
- [Streamlit fragment](https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment)
- [Streamlit AppTest](https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest)
- [Streamlit Plotly chart và width](https://docs.streamlit.io/1.51.0/develop/api-reference/charts/st.plotly_chart)

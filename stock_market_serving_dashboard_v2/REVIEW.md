# Kết quả kiểm tra Serving Layer và Dashboard

**Cập nhật sau khi nạp toàn bộ ngày 11/11/2026:** Elasticsearch đã chứa đủ 4.638.969 bản ghi giá của 30 mã, 994.330 feature 5 phút, 9.023 bản ánh xạ của 8.905 bài báo và 795 cửa sổ mật độ tin có bài. Không có lỗi bulk; số lượng từng mã và mốc thời gian khớp file nguồn, 90 mẫu OHLCV được đối chiếu đều khớp. Xem `reports/full_import_report.json` và `reports/full_import_verification.json`. Số liệu 30 ngày ở phần dưới là lần nạp ban đầu.

Đối chiếu ngày 11/11/2026 với báo cáo đề xuất gốc, đặc biệt mục 5, 7–10, và ảnh phân công Elasticsearch Serving Layer → End-user Dashboard. Đã đọc các file ứng dụng gốc: `contract.py`, `dashboard.py`, `es_common.py`, `init_indexes.py`, `queries.py`, `seed_demo.py`, `simulate_stream.py`, `compose.yaml`, `requirements.txt`, `README.md`, `tests/test_contract.py`; đọc schema và kiểm tra toàn bộ 31 file Parquet. Không tính môi trường `.venv`, thư viện `.tools` và bộ skill `.agents` là mã ứng dụng.

Kết quả chính: module đã sử dụng được dữ liệu crawl lịch sử trong Elasticsearch riêng, hỗ trợ truy vấn và dashboard theo phạm vi được giao. Phần phát hiện anomaly và xử lý Spark realtime vẫn cần đầu ra của upstream; không thể kết luận đã hoàn thiện toàn bộ Lambda Architecture.

## Vấn đề và thay đổi

| Phát hiện trước sửa | Cách xử lý |
|---|---|
| Seed và simulator chỉ tạo số liệu giả, chưa đọc dữ liệu người dùng | Thêm `import_parquet.py`, tự tìm root dữ liệu, hỗ trợ phạm vi mã/ngày và báo cáo kiểm tra |
| UI cố định ba mã FPT/HPG/VNM | Đọc mã và thời gian min/max đã nạp từ ES |
| Chỉ chọn thời gian tính lùi từ hiện tại | Chọn ngày lịch sử, 7/30 ngày cuối dữ liệu, toàn bộ lịch sử đã nạp và 24h gần hiện tại |
| Tin chỉ xuất hiện khi có event | Có tìm title/content và danh sách tin độc lập, link mở bài, tùy chọn cả bài chưa ánh xạ |
| Chưa chuẩn hóa timezone trước khi tạo ID | Canonical UTC và symbol; các cách viết cùng thời điểm có cùng khóa |
| Chưa phân xử Batch/Speed | `serving_sink.py`: scripted upsert, source_version, Batch authority và không mở lại event COMPLETED |
| Thống kê từ danh sách tối đa 200 event và tổng hits có thể bị chặn | Aggregation đầy đủ, `track_total_hits=True`, công khai giới hạn danh sách, lọc type/status tại ES |
| Biểu đồ chỉ có close, marker gắn vào điểm gần nhất kể cả khoảng trống | OHLC aggregation và candlestick; marker chỉ ở bucket có dữ liệu chứa thời điểm event |
| Cảnh báo giả lập áp dụng cho mọi dữ liệu | Tách năm index `real-*` với năm index demo cũ và cho chọn bộ dữ liệu |
| Simulator có thể ghi high/low mất cực trị và trùng ID bài khi restart | Giữ extrema trong cửa sổ, UUID theo lần chạy, chỉ được chạy với ES_DATASET=demo |
| Thiếu dependency đọc Parquet/timezone Windows | Thêm pyarrow/tzdata/numpy, nâng mức tối thiểu Streamlit phù hợp API width |
| Chỉ bốn unit tests đơn giản | Bổ sung kiểm tra nhập liệu, sink, ES thật và Streamlit AppTest |

## Dữ liệu và kết quả nạp thực tế

Nguồn nằm ở `big-data-project-crawl-data/data/bronze/`. Có 30 file giá, tổng **4.638.969 dòng**, ngày **13/11/2023–10/11/2026**. Các kiểm tra đã chạy không phát hiện timestamp thiếu timezone/không parse được, trùng khóa symbol+timestamp, OHLC không hợp lệ, số không hữu hạn hoặc volume âm. Điều này không xác nhận dữ liệu thị trường là đầy đủ, chính xác hoặc đã điều chỉnh giá.

File tin có **8.905 bài CafeF** từ **27/10/2025–10/11/2026**, không có symbol. `published_at` là giờ không timezone, `crawled_at` có offset. Bộ nạp mặc định diễn giải giờ xuất bản là Asia/Ho_Chi_Minh; đây là giả định cần xác nhận với crawler, không phải metadata đã có sẵn. Không có trùng article_id/URL trong kiểm tra bộ nguồn hiện tại.

Lần nạp ban đầu chọn **12/10/2026–10/11/2026**, tức `[2026-10-11T17:00:00Z,2026-11-10T17:00:00Z)`:

| Index thật | Document |
|---|---:|
| real-stock-price-v1 | 142.691 |
| real-stock-features-v1 | 30.291 |
| real-stock-news-v1 | 777 |
| real-news-density-v1 | 63 |
| real-market-events-v1 | 0 |

777 document tin là các bản ánh xạ của **770 bài duy nhất**; 57 bài khớp ticker, 713 bài chưa ánh xạ. Một bài có thể khớp nhiều mã. Trên toàn bộ lịch sử, 682 bài khớp ticker và 8.223 chưa ánh xạ. Không gán bài kinh tế chung cho tất cả mã để làm tăng density. Cần từ điển alias và kiểm tra chất lượng ánh xạ để cải thiện recall.

Không tạo market_event khi chưa có baseline/flags. Các feature mới có close/volume/return quan sát được, không có z-score hoặc anomaly flag được bịa thêm. Lần nạp ban đầu chỉ chọn 30 ngày cuối; sau đó đã chạy `--days 0 --load` để nạp toàn bộ lịch sử theo yêu cầu. Dữ liệu Parquet gốc được giữ nguyên.

## Bằng chứng kiểm chứng

- Elasticsearch/Kibana local đang chạy; ES image 9.5.6. Index thật tách khỏi index demo có sẵn. Các lần nhập thành công không có bulk error.
- Replay cùng phạm vi không tăng số document. Các kiểm thử ES xác minh version cũ không ghi đè, Batch có thẩm quyền hơn Speed, Bronze version 0 không xóa kết quả Batch version dương, COMPLETED không quay về OPEN.
- Kiểm tra histogram xác minh first open/max high/min low/last close và sum volume, loại đúng mốc kết thúc `[start,end)`, không cộng chồng density 5m/30m/60m.
- Với 220 event trong index kiểm thử, danh sách chỉ trả 200 nhưng tổng và thống kê vẫn tính 220. Kiểm tra liên kết tin có cả hai biên ±30 phút và đúng symbol.
- AppTest kiểm tra bốn tab, real/demo, toàn bộ lịch sử, ngày tùy chọn, chế độ 24 giờ, bộ lọc composite và tìm tin. Các lỗi parse timestamp hỗn hợp trong demo đã được sửa.
- HTTP health của Streamlit trả `ok`; `pip check` không phát hiện dependency hỏng. Số kiểm thử và trạng thái cuối được ghi tại `reports/verification.json`.
- **Chưa kiểm tra ảnh giao diện/browser:** công cụ computer use tự dừng vì không xác định được URL Chrome đủ tin cậy. AppTest không thay thế kiểm tra ảnh, bố cục responsive hoặc trải nghiệm trong trình duyệt.

Không ghi kết quả này thành bằng chứng Spark/Kafka realtime hoặc kiểm thử tải. Việc ES/Streamlit chạy được không chứng minh end-to-end Lambda hoàn chỉnh. Bản sao mã trước sửa ở `.review-backup/`; Parquet và Word gốc không bị sửa.

## Cần phối hợp với nhóm upstream

1. Xuất Gold features/density/events/event_analysis theo hợp đồng trong README, thống nhất UTC, ID, cửa sổ không chồng lắp, source_version và lúc Batch được quyền chốt view.
2. Xác nhận timezone của tin, đơn vị/điều chỉnh giá và volume nguồn. Bộ nạp hiện cộng volume như volume từng nến; phải thay adapter nếu nguồn là lũy kế.
3. Cung cấp alias doanh nghiệp đã kiểm tra; dữ liệu hiện chỉ có một nguồn tin, chưa đạt phạm vi đề xuất 2–4 nguồn.
4. Gửi cập nhật OPEN→COMPLETED và chỉ số hậu sự kiện sau khi có đủ quan sát. Sink hỗ trợ cập nhật, không thay upstream tính baseline/event study.
5. Thử trực tiếp biểu đồ/bảng trong trình duyệt và đo độ trễ, tải truy vấn sau khi có dữ liệu upstream thật.

## Tài liệu kỹ thuật đã đối chiếu

- [Elasticsearch Python client helpers](https://www.elastic.co/docs/reference/elasticsearch/clients/python/client-helpers)
- [Streamlit AppTest](https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest)
- [Streamlit Plotly width API từ 1.51](https://docs.streamlit.io/1.51.0/develop/api-reference/charts/st.plotly_chart)

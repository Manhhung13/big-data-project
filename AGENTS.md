# Repository Guidelines

## Cấu trúc dự án

Mã ứng dụng nằm trong `stock_market_serving_dashboard_v2/`:
- `dashboard.py`, `queries.py`: giao diện Streamlit và truy vấn Elasticsearch.
- `contract.py`, `init_indexes.py`, `es_common.py`: khóa document, mapping, kết nối.
- `import_parquet.py`, `serving_sink.py`: nạp Parquet và nhận kết quả Batch/Speed.
- `tests/`: kiểm thử; `reports/`: báo cáo kiểm chứng.
- `big-data-project-crawl-data/data/bronze/`: dữ liệu nguồn.

`outputs/` ở workspace chứa minh họa. Giữ nguyên Parquet và bản đề xuất Word.

## Cài đặt và chạy

Dùng PowerShell, Python 3.11/3.12 và Docker Desktop:

```powershell
cd stock_market_serving_dashboard_v2
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d
$env:ES_DATASET = 'real'
.\.venv\Scripts\python.exe -X utf8 import_parquet.py
.\.venv\Scripts\python.exe -m streamlit run dashboard.py --server.address=127.0.0.1
```

Compose khởi động Elasticsearch/Kibana. Import mặc định kiểm tra 30 ngày cuối nguồn và ghi báo cáo; thêm `--load` để nạp Elasticsearch, `--days 0` để chọn toàn bộ lịch sử. Dashboard tại `http://localhost:8501`.

## Quy ước mã nguồn

Dùng 4 dấu cách; `snake_case` cho hàm/biến, `UPPER_CASE` cho hằng. Giữ hàm nhỏ theo module hiện có. Chưa có cấu hình formatter/linter. Chuẩn hóa symbol và thời gian UTC trước khi tạo ID; dùng helper trong `contract.py`. Không điền 0/False cho anomaly chưa được tính.

## Kiểm thử

Dùng `unittest`, đặt file `tests/test_*.py`, hàm `test_*`. Chạy từ thư mục ứng dụng:

```powershell
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -s tests -v
```

Bật `RUN_ES_TESTS=1` để kiểm thử Elasticsearch; bật `RUN_DASHBOARD_TESTS=1` khi đã nạp cả real/demo để chạy Streamlit AppTest. Gán bằng `$env:RUN_ES_TESTS = '1'`. Integration test chỉ xóa index tạm `serving-test-<uuid>-*`. Chưa đặt ngưỡng coverage; bổ sung kiểm thử liên quan khi đổi ID, timezone, upsert hoặc aggregation. Báo rõ test bị skip.

## Commit và pull request

Workspace chưa có lịch sử Git để xác nhận quy ước. Đề xuất commit ngắn như `fix: chuẩn hóa timestamp`. PR cần mô tả thay đổi, kết quả kiểm thử, issue nếu có và ảnh khi sửa UI. Không đưa `.venv/`, cache hoặc log vào commit.

## Cấu hình và dữ liệu

`ES_URL` mặc định `http://localhost:9200`; `ES_DATASET` chọn `real`/`demo`. Seed/simulator chỉ dùng demo. Không chạy `docker compose down -v` khi cần giữ dữ liệu. Compose chỉ dành cho local. Phân biệt bằng chứng đã lưu với trạng thái đang chạy; không mô tả simulator là Kafka/Spark thật.

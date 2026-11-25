# big-data-project

## Elasticsearch Serving Layer và Dashboard

Phần Serving Layer nằm trong [`stock_market_serving_dashboard_v2/`](stock_market_serving_dashboard_v2/README.md), gồm bộ nạp Parquet, hợp đồng dữ liệu Batch/Speed, truy vấn Elasticsearch, dashboard Streamlit và kiểm thử.

Dữ liệu nguồn được quản lý riêng tại nhánh `crawl-data`. Sau khi clone nhánh này, chạy tại thư mục gốc để lấy dữ liệu nếu chưa có:

```powershell
git clone --single-branch --branch crawl-data https://github.com/Manhhung13/big-data-project.git stock_market_serving_dashboard_v2/big-data-project-crawl-data
```

Xem README của module để cài đặt và chạy. Báo cáo được tạo tại máy khi chạy bộ nạp và không được quản lý bằng Git. Baseline, phát hiện bất thường và tích hợp Kafka/Spark vẫn cần đầu ra upstream.

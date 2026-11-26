# Big Data Project — VN30 Stock & Financial News (Lambda Architecture)

## Phân chia thư mục theo từng phần việc

- **`data_ingestion/`** *(Phần Thu thập & Nạp dữ liệu + Bronze Store)*:
  - Cào dữ liệu nến `1m` (30 mã VN30) và tin tức CafeF mỗi phút đẩy lên **Apache Kafka** (`stock-price-topic`, `news-topic`).
  - Lưu gộp định kỳ **1 giờ/lần** từ Kafka xuống các tệp Parquet trong `data/bronze/`.
  - Đóng gói trọn bộ bằng Docker (`data_ingestion/docker-compose.yml` & `data_ingestion/Dockerfile`).
- **`data/bronze/`** *(Kho dữ liệu thô Parquet)*:
  - `data/bronze/stock/{SYMBOL}_1m.parquet`: 30 mã VN30 (lịch sử 3 năm + dữ liệu cập nhật mới).
  - `data/bronze/news/historical_news.parquet`: Tin tức tài chính CafeF (lịch sử 1 năm + dữ liệu cập nhật mới).

## Khởi chạy phần Thu thập dữ liệu (Docker)

```powershell
docker compose -f .\data_ingestion\docker-compose.yml up -d --build
```

Xem chi tiết hướng dẫn & Data Contract cho tầng Streaming / Spark Batch tại [data_ingestion/README.md](file:///d:/Study/big-data-project/data_ingestion/README.md).

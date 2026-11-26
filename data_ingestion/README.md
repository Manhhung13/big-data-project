# DATA COLLECTION & INGESTION LAYER (APACHE KAFKA & DATA SOURCES)

Module phụ trách toàn bộ việc thu thập, chuẩn hóa và nạp dữ liệu chứng khoán (OHLCV) và tin tức tài chính (CafeF) **100% dữ liệu thực tế** (không sử dụng dữ liệu giả lập/mô phỏng) phục vụ cho kiến trúc Lambda (Speed Layer & Batch Layer) của Đồ án Big Data.

---

## 1. CẤU TRÚC THƯ MỤC MODULE

```text
data_ingestion/
├── Dockerfile                   # Image Python 3.11 cho Collector, Bronze Sink và Backfill CLI
├── .dockerignore                # Loại trừ file tạm/cache khi build Docker
├── docker-compose.yml           # Toàn bộ stack: Zookeeper, Kafka, Collector (1m) & Bronze Sink (1h)
├── requirements.txt             # Danh sách thư viện Python cần thiết
├── config.py                    # Cấu hình Kafka brokers, đường dẫn data/bronze, 30 mã VN30, timezone
├── generate_reference_data.py   # Script tạo bảng Reference Data cho 30 mã VN30
├── reference_symbols.csv        # Bảng ánh xạ symbol - company_name - aliases - industry (cho Spark NLP)
├── auto_collector_1m.py         # Producer cào realtime 1 phút/lần đẩy lên Kafka
├── bronze_sink_consumer.py      # Consumer đọc Kafka, hẹn giờ 1 giờ/lần gộp vào data/bronze/
├── backfill_history.py          # Script backfill lịch sử (cổ phiếu 1D & tin tức CafeF)
├── backfill_symbol_3y_1m.py     # Script backfill nến 1m cho 1 mã cổ phiếu
├── backfill_vn30_3y_1m.py       # Script backfill nến 1m cho toàn bộ 30 mã VN30 (hỗ trợ --days)
├── read_bronze_data.py          # Công cụ CLI đọc và kiểm tra các tệp Parquet trong data/bronze/
├── replay_producer.py           # Producer phát lại dữ liệu phục vụ kiểm thử luồng streaming
└── README.md                    # Tài liệu hướng dẫn & Hợp đồng dữ liệu (Data Contract)
```

---

## 2. DỮ LIỆU ĐÃ THU THẬP & CƠ CHẾ HOẠT ĐỘNG (100% REAL DATA)

Toàn bộ dữ liệu trong thư mục `../data/bronze/` là **dữ liệu thật 100%**, không sử dụng dữ liệu ngẫu nhiên hay giả lập:

### 2.1. Dữ liệu Giá Cổ phiếu Nến 1 Phút (`data/bronze/stock/*_1m.parquet`)
* **Nguồn dữ liệu:** Datafeed TradingView UDF backend của CTCK (VNDirect `dchart-api.vndirect.com.vn` làm nguồn chính, Mirae Asset `masboard.masvn.com` làm nguồn dự phòng).
* **Khung nến:** Nến 1 phút (`interval="1m"`).
* **Phạm vi thời gian:** Từ **`13/11/2023` đến `26/11/2026`** (trọn vẹn >3 năm, đã backfill bổ sung liên tục đến hiện tại).
* **Quy mô:** Đầy đủ **30/30 mã VN30**, tổng cộng **~4,78 triệu nến 1 phút thực tế** (~50 MB Parquet).

### 2.2. Dữ liệu Tin tức Tài chính CafeF (`data/bronze/news/historical_news.parquet`)
* **Nguồn dữ liệu:** Endpoint Timeline chuyên mục Thị trường chứng khoán của CafeF:
  $$\text{URL: } \texttt{https://cafef.vn/timelinelist/18831/\{page\}.chn}$$
* **Phạm vi thời gian:** Từ **`27/10/2025` đến `26/11/2026`** (>1 năm liên tục).
* **Quy mô:** **~9.318 bài báo tài chính thực tế**, đầy đủ `article_id`, `title`, `url`, `content` (sapo), `published_at`, `crawled_at`, `source`.

---

## 3. CƠ CHẾ BẢO VỆ DỮ LIỆU & CHỐNG TRÙNG LẶP (IDEMPOTENCY)

Hệ thống được thiết kế để bảo toàn tính toàn vẹn dữ liệu, **không bị mất dữ liệu cũ và không bị trùng lặp dòng**:

1. **Khi chạy lại `backfill_history.py`:**
   - Script tự động đọc file Parquet cũ (nếu đã tồn tại), ghép dữ liệu mới vào.
   - Áp dụng `drop_duplicates(subset=['timestamp'])` đối với cổ phiếu và `drop_duplicates(subset=['article_id'])` đối với tin tức.
   - **Kết quả:** Có thể chạy lại nhiều lần tùy thích, dữ liệu lịch sử được giữ nguyên, chỉ bổ sung nến/bài báo mới và không bị duplicate.

2. **Khi chạy ngầm hoặc restart `auto_collector_1m.py`:**
   - **Với giá cổ phiếu:** Duy trì bộ nhớ và cache đĩa `.last_stock_timestamps.json`. Mỗi phút quét nến mới nhất, nếu timestamp nến của mã trùng khớp với nến đã gửi trước đó (ngoài giờ giao dịch, giờ nghỉ trưa, ngày cuối tuần), script tự động bỏ qua không đẩy trùng lặp lên Kafka. Chỉ khi phiên giao dịch phát sinh nến mới mới đẩy lên `stock-price-topic`.
   - **Với tin tức CafeF:** Sử dụng `collections.deque(maxlen=1000)` kết hợp `set` để lưu trữ và bảo toàn chuẩn xác thứ tự 1.000 bài báo mới nhất ra file `.seen_news_ids.json`. Mã hóa URL sang ID dùng `hashlib.md5(url.encode()).hexdigest()[:12]` cố định 100% giữa các lần khởi động lại. Bài báo nào đã từng bắn vào Kafka sẽ bị bỏ qua, chỉ có bài mới tinh mới được đẩy tiếp vào topic `news-topic`.

---

## 4. HƯỚNG DẪN CÀI ĐẶT & CHẠY SCRIPT

### Cách chạy trọn gói bằng Docker (Khuyên dùng — không cần `.venv`)
Từ thư mục `data_ingestion/` (hoặc truyền `-f data_ingestion/docker-compose.yml` từ thư mục gốc):
```bash
cd data_ingestion

# Build image & khởi chạy: Zookeeper, Kafka, Collector (1 phút/lần) và Bronze Sink (1 giờ/lần)
docker compose up -d --build

# Theo dõi log Collector & Bronze Sink:
docker compose logs -f collector bronze-sink

# Kiểm tra dữ liệu Bronze Store trực tiếp qua container:
docker compose exec bronze-sink python data_ingestion/read_bronze_data.py --summary
docker compose exec bronze-sink python data_ingestion/read_bronze_data.py --symbol FPT
```

---

### Cách chạy thủ công từng thành phần (nếu dùng môi trường Python cục bộ)

### Bước 1: Chuẩn bị môi trường Python
```bash
pip install -r requirements.txt
```
*(File `requirements.txt` đã cấu hình `--extra-index-url https://vnstocks.com/api/packages` để tự động tải `vnstock` chuẩn).*

### Bước 2: Khởi động Hạ tầng Kafka (Docker)
```bash
docker compose up -d
```
Container `init-kafka` sẽ tự động khởi tạo 2 topics:
* `stock-price-topic` (3 partitions, replication factor 1)
* `news-topic` (3 partitions, replication factor 1)

Kiểm tra topics đã tạo:
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Bước 3: Tạo bảng Reference Data
```bash
python generate_reference_data.py
```
Sinh file `reference_symbols.csv` gồm 30 mã VN30 có tên công ty, aliases nhận diện thực thể và ngành.

### Bước 4: Chạy thu thập lịch sử 1 năm (Backfill Bronze)
```bash
# Cào toàn bộ 1 năm (cả 30 mã cổ phiếu 1D và 8.900+ bài báo CafeF):
python backfill_history.py

# Hoặc chỉ cào riêng 30 mã cổ phiếu 1 năm (~2.5 phút):
python backfill_history.py --stock-only

# Hoặc chỉ cào riêng tin tức CafeF 1 năm qua (~1 phút):
python backfill_history.py --news-only --days 365
```

### Bước 4.1: Chạy thu thập 1 - 3 năm nến 1 phút (1m) thực tế (Chunking)
```bash
# Cào 3 năm nến 1m cho mã FPT (~160.000 nến trong ~1 phút):
python backfill_symbol_3y_1m.py --symbol FPT --years 3 --chunk 30

# Cào 1 năm nến 1m cho bất kỳ mã nào trong VN30 (ví dụ HPG, VNM...):
python backfill_symbol_3y_1m.py --symbol HPG --years 1 --chunk 30
```

### Bước 5: Chạy Ingestion Realtime 1 phút/lần (Producer -> Kafka)
```bash
python auto_collector_1m.py
```
- **Giây thứ 10:** Quét giá nến 1m thực tế mới nhất của 30 mã VN30 từ TradingView UDF **VNDirect (`dchart-api.vndirect.com.vn`, nguồn chính)** / **Mirae Asset (`masboard.masvn.com`, dự phòng)** (có cơ chế tự lấy bù nến đóng muộn) đẩy vào `stock-price-topic` (Key: `symbol`).
- **Giây thứ 20:** Quét CafeF, đối chiếu cache `.seen_news_ids.json`, đẩy bài báo mới vào `news-topic` (Key: `article_id`).

*(Nếu test ngoài giờ giao dịch, dùng `python replay_producer.py --bars 10 --speed 1 --include-news` để phát dữ liệu mẫu vào cả 2 topics).*

### Bước 6: Chạy nhánh lưu Bronze Store (Hẹn giờ 1 giờ/lần) & Bàn giao Kafka cho Streaming
Từ Kafka, dữ liệu sẵn sàng cho **2 nhánh độc lập**:
1. **Nhánh Bronze Store (`bronze-store-group` — Hẹn giờ 1 giờ lưu gộp 1 lần):**
   ```bash
   python bronze_sink_consumer.py --flush-interval 3600
   ```
   Gom dữ liệu từ `stock-price-topic` & `news-topic`, mỗi 1 giờ (`3600s`) tự động gộp trực tiếp và khử trùng lặp vào `../data/bronze/stock/{SYMBOL}_1m.parquet` và `../data/bronze/news/historical_news.parquet`.
2. **Nhánh Streaming (của thành viên khác):**
   - Kết nối tới `localhost:9092` (hoặc `kafka:29092` nếu chạy chung Docker network `data_ingestion_default`), dùng `group.id` riêng đọc liên tục mỗi phút từ `stock-price-topic` và `news-topic`.
3. **Kiểm tra dữ liệu Bronze:**
   ```bash
   python read_bronze_data.py --summary
   python read_bronze_data.py --symbol FPT
   python read_bronze_data.py --news
   ```

---

## 5. HỢP ĐỒNG DỮ LIỆU (DATA CONTRACT) CHO TẦNG SPARK / HDFS

### 5.1. Thông số kết nối Kafka
* **Bootstrap Server (Ngoài Docker / Máy host):** `localhost:9092`
* **Bootstrap Server (Trong Docker network / Spark container):** `kafka:29092`
* **Topic giá cổ phiếu:** `stock-price-topic` (Key: `symbol`)
* **Topic tin tức:** `news-topic` (Key: `article_id`)

### 5.2. Schema JSON - Topic `stock-price-topic`
```json
{
  "symbol": "FPT",
  "timestamp": "2026-11-10 14:45:00+07:00",
  "open": 50.9,
  "high": 50.9,
  "low": 50.9,
  "close": 50.9,
  "volume": 335700,
  "source": "vnstock",
  "ingested_at": "2026-11-10T20:26:09.909482+07:00"
}
```
*PySpark StructType:*
```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

stock_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", StringType(), True)
])
```

### 5.3. Schema JSON - Topic `news-topic`
```json
{
  "article_id": "188261110145158006",
  "title": "Bóng dáng công ty chứng khoán liên quan Tân Hoàng Minh tại khu đất vàng...",
  "url": "https://cafef.vn/bong-dang-cong-ty-chung-khoan-lien-quan-tan-hoang-minh-188261110145158006.chn",
  "content": "Chứng khoán UP đã trở thành cổ đông lớn của Bất động sản...",
  "published_at": "2026-11-10T19:12:00.000000",
  "crawled_at": "2026-11-10T20:26:09.909482+07:00",
  "source": "cafef"
}
```
*PySpark StructType:*
```python
from pyspark.sql.types import StructType, StructField, StringType

news_schema = StructType([
    StructField("article_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("crawled_at", StringType(), True),
    StructField("source", StringType(), True)
])
```

### 5.4. Dữ liệu tĩnh bàn giao trong `data/bronze/`
1. **`reference_symbols.csv`**: Dùng cho Spark NLP regex matching hoặc join bài báo sang mã cổ phiếu (`symbol, company_name, aliases, industry`).
2. **`../data/bronze/stock/`**:
   - **30 file nến 1 phút (`{symbol}_1m.parquet`)**: Gồm **~4,78 triệu nến 1 phút** (>3 năm + dữ liệu bù 1 tháng gần nhất & cập nhật mỗi giờ, ~50 MB). Dùng cho Spark Batch phân tích vi mô, tính Baseline Store và huấn luyện mô hình.
3. **`../data/bronze/news/historical_news.parquet`**: **~9.318 bài báo tài chính** thực tế (>1 năm liên tục + cập nhật mỗi giờ).

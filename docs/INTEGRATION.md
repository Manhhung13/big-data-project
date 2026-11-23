# Hợp đồng tích hợp — Speed Layer, Serving (Elasticsearch), Dashboard, Kafka producer

Batch Layer là nguồn sự thật lịch sử. Module khác **chỉ đọc** qua các điểm tích hợp dưới đây, không
quét trực tiếp `_versions/` và không đọc bảng Gold "đang sống" để làm baseline. Mọi ví dụ dùng dữ
liệu giả lập.

## 1. Speed Layer — baseline đã công bố

### 1.1 Con trỏ `_system/baselines/current.json`

Được thay nguyên tử bởi `stock-batch publish-baseline --version <v>`; schema:
[`contracts/gold/speed_baseline.schema.json`](../contracts/gold/speed_baseline.schema.json). Ví dụ:

```json
{
  "baseline_version": "bl_2026-10-07_40526ac7_9507a2aa",
  "valid_for_trading_date": "2026-10-07",
  "train_end": "2026-10-06",
  "row_count": 506,
  "status_counts": {"READY": 219, "ZERO_STD": 287},
  "usable_ratio": 1.0,
  "catalog_generation": 2,
  "source_run_id": "20261119T052800_ab12cd34",
  "export_path": "hdfs://namenode:8020/data/_system/baselines/versions/bl_2026-10-07_40526ac7_9507a2aa",
  "export_format": "parquet",
  "dataset_partitions": {"2026-10-07": "hdfs://.../gold/stock_baseline/_versions/run_id=.../baseline_date=2026-10-07"},
  "status": "PUBLISHED", "schema_version": "1.0", "published_at": "2026-11-19T05:30:00+00:00",
  "previous_version": null
}
```

- Dòng baseline nằm ở `export_path` (Parquet **bất biến**, schema dòng:
  [`speed_baseline_rows.schema.json`](../contracts/gold/speed_baseline_rows.schema.json)). Mọi bản
  đã publish còn trong `_system/baselines/history/` để truy lại.
- `baseline_version = <prefix>_<ngày hiệu lực>_<config_hash nghiệp vụ 8>_<hash nội dung 8>`:
  **đánh địa chỉ theo nội dung** — chạy lại y hệt cho ra đúng version cũ; version đổi ⇔ số liệu đổi.
- `train_end < valid_for_trading_date` luôn đúng (kiểm tra cả ở quality gate và khi publish).
- Chính sách publish: tỉ lệ cohort dùng được (`READY` + `ZERO_STD`) ≥ `baseline.publish_min_usable_ratio`
  (dev/prod 0.8, test 0.5). Không cho lùi về ngày cũ hơn bản hiện hành trừ khi `--allow-older`.

### 1.2 Cách Speed Layer dùng

1. Khi khởi động (và định kỳ, ví dụ mỗi phút), đọc `current.json`; nếu `baseline_version` khác bản
   đang giữ thì nạp `export_path` vào bộ nhớ, **ghim** version đó cho cả phiên/ngày.
2. Khóa tra cứu: `(symbol, session_id, time_bucket, metric, window_size)`; `time_bucket` là giờ VN
   `HH:mm` của **đầu** bar 5 phút (bar `[10:30, 10:35)` → `10:30`; sự kiện gắn mốc `window_end = 10:35`).
   `metric/window_size`: `return_simple/5m`, `volume_sum/5m`, `news_count/30m` (B6 dùng 30m cho tin;
   bản xuất cũng có `news_count/5m` và `1h`).
3. Trạng thái cohort:
   - `READY`: z = (x − mean) / std (std mẫu, ddof=1).
   - `ZERO_STD`: không tính z; dùng quy tắc phân vị (x ≥ p99 và x > mean, kèm ngưỡng tối thiểu).
   - `SPARSE` (sample_count < `min_samples`) hoặc **không có dòng** → `UNKNOWN_INSUFFICIENT_BASELINE`
     (NO_BASELINE), **không** được coi là bình thường.
4. Nếu `valid_for_trading_date` < ngày đang chấm: dùng bản đó nếu policy của nhóm cho phép (ghi rõ
   trong event), hoặc gắn `NO_BASELINE`. **Không** tự dựng baseline từ dữ liệu realtime.
5. Ghi `baseline_version` vào mọi event realtime để so với lịch sử sau này.

Quy tắc cờ dùng chung với B6 (`rules.signal_state`): price = |z| ≥ 3 hoặc (x ≥ p99, x > mean, x > 0);
volume/news = x ≥ min_count và (z ≥ 3 hoặc (x ≥ p99, x > mean)).

Hợp đồng dòng: `window_size` của `return_simple`/`volume_sum` luôn là `5m`; `news_count` có đủ `5m`,
`30m`, `1h` (mọi cửa sổ trong `news.windows_minutes`). Speed Layer nên bỏ qua cặp (metric, window_size)
mình không dùng thay vì coi là lỗi schema.

### 1.3 Khi chưa có baseline (giai đoạn đầu dự án)

- Với policy dev/prod (`baseline.min_samples: 20`, `baseline.train_trading_days: 60`), khi lịch sử
  có **ít hơn khoảng 20 ngày giao dịch**, gần như mọi cohort là `SPARSE`. Khi đó tỉ lệ cohort dùng
  được < `publish_min_usable_ratio` (0.8) nên `publish-baseline` bị **từ chối**, và B6 sinh **0 event**
  (mọi tín hiệu là `UNKNOWN_INSUFFICIENT_BASELINE`). Đây là hành vi đúng, không phải lỗi.
- Speed Layer phải xử lý được việc `_system/baselines/current.json` **chưa tồn tại** (hoặc
  `LakeReader.published_baseline()` trả `None`, `baseline_rows()` ném `LookupError`): gắn
  `NO_BASELINE` cho mọi event, không dừng tiến trình, và đọc lại con trỏ định kỳ.
- Muốn demo tích hợp sớm (dữ liệu giả lập 5 ngày): chạy `--env test` (`min_samples: 2`,
  `publish_min_usable_ratio: 0.5`), ví dụ
  `... --env test seed-demo`, `... --env test run --job all --start 2026-10-01 --end 2026-10-07`,
  `... --env test publish-baseline --version <bl_2026-10-07_...>`. Đừng dùng số liệu `--env test` để
  hiệu chỉnh ngưỡng thật.

## 2. Serving / Elasticsearch — export ghim generation

```powershell
docker compose --profile tools run --rm spark-job --env dev export --dataset historical_events --start 2026-10-01 --end 2026-10-07 --format jsonl
```

- Ghi vào `_system/exports/<dataset>/generation=<g>/<start>_<end>/` (hoặc `--out`), mỗi dòng JSON một
  document, timestamp ISO-8601 UTC (`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`), kèm `_export_manifest.json`
  (generation, run nguồn, số dòng, cột bị loại).
- Mỗi document có `_id` ổn định qua các lần chạy lại và `_catalog_generation`:

| Dataset | `_id` | Index gợi ý |
|---|---|---|
| `historical_events` | `event_id` (sha256 của symbol + event_time + rule_version) | `stock-events` |
| `event_analysis` | sha256(`event_id|horizon|analysis_version`)[:40] | `stock-event-analysis` |
| `news` | `article_id` | `stock-news` |
| `news_stock_mapping` | sha256(`article_id|symbol|mapping_version`)[:40] | `stock-news-mapping` |
| `stock_features` | sha256(`symbol|window_start|interval`)[:40] | `stock-features-5m` |
| `news_density` | sha256(`symbol|window_end|window_size`)[:40] | `stock-news-density` |
| `stock_baseline` | sha256(`baseline_version|symbol|session_id|time_bucket|metric|window_size`)[:40] | `stock-baseline` |

Bulk index với `_id` này là idempotent (index lại = ghi đè cùng document).

**Lưu ý quan trọng — `_id` nằm trong thân document của file export.** Elasticsearch 8 từ chối trường
`_id` trong `_source` ("Field [_id] is a metadata field and cannot be added inside a document"). Vì vậy
**không** đẩy thẳng từng dòng JSONL vào `_bulk` thô hay Logstash: phải tách `_id` ra làm metadata của
action bulk (`{"index": {"_index": ..., "_id": ...}}`) và bỏ nó khỏi document. Thư viện
`elasticsearch.helpers.bulk` làm việc này tự động khi action có khóa `_id`. Ví dụ (chạy ngoài module
này; cần `pip install elasticsearch`):

```python
import glob, json
from elasticsearch import Elasticsearch, helpers

EXPORT_DIR = '/path/to/_system/exports/historical_events/generation=2/2026-10-01_2026-10-07'  # copy về local
es = Elasticsearch('http://localhost:9200')

def actions(index):
    for path in sorted(glob.glob(f'{EXPORT_DIR}/part-*.json')):
        with open(path, encoding='utf-8') as fh:
            for line in fh:
                doc = json.loads(line)
                doc_id = doc.pop('_id')                       # metadata, not part of _source
                yield {'_op_type': 'index', '_index': index, '_id': doc_id, '_source': doc}

ok, errors = helpers.bulk(es, actions('stock-events'), raise_on_error=False)
print(ok, errors[:3])   # chạy lại cùng export => cùng _id => ghi đè, không nhân bản
```

Với Logstash: đọc file bằng `json_lines` codec, chuyển `_id` sang `[@metadata][doc_id]`
(`mutate { rename => { "_id" => "[@metadata][doc_id]" } }`) và đặt `document_id => "%{[@metadata][doc_id]}"`
trong output `elasticsearch`. Nên ghi `_catalog_generation` vào document để biết bản đang hiển thị
đến từ snapshot nào.

Mapping trường gợi ý (ES 8): `symbol`, `event_type`, `status`, `*_state`, `horizon`, `baseline_version`,
`rule_version`, `event_id`, `article_id`, `source` → `keyword`; `event_time`, `event_end`,
`window_start`, `window_end`, `published_at`, `crawled_at` → `date`; `event_date`, `trade_date`,
`publish_date` → `date` (format `yyyy-MM-dd`); `z_*`, `return_*`, `completeness`, `mean`, `std` →
`double`; `*_count`, `volume_sum` → `long`; `*_flag`, `is_complete` → `boolean`; `title` → `text` +
`keyword`; `related_article_ids` → `keyword` (mảng).

Ý nghĩa cần hiển thị đúng trên Dashboard:
- `*_flag = null` nghĩa là **không đủ căn cứ** (`*_state = UNKNOWN_*`), khác `false` (NORMAL).
- `status = PARTIAL_BASELINE` khi ít nhất một tín hiệu UNKNOWN.
- `event_analysis.is_complete = false` luôn kèm `missing_reason` (ví dụ `HORIZON_CROSSES_SESSION`,
  ADR-14); `return_after` NULL nghĩa là chưa đủ dữ liệu, không phải 0.
- Liên hệ tin–giá là **đồng thời theo thời gian**, không phải quan hệ nhân quả.

**Chính sách nội dung tin (BRD 11.1):** export `news` **mặc định loại `content_text`**. Chỉ dùng
`--include-content` cho nguồn đã có quyền lưu/hiển thị toàn văn; nếu không, Dashboard chỉ hiển thị
tiêu đề, nguồn, URL gốc và thời gian.

## 3. Đọc trực tiếp bằng `LakeReader` (Spark)

```python
from stock_batch.config import load_config
from stock_batch.lake import LakeReader

cfg = load_config('dev')                       # hoặc LakeReader(spark, data_root='hdfs://namenode:8020/data')
reader = LakeReader(spark, cfg)                # ghim generation mới nhất tại thời điểm tạo
print(reader.generation, reader.run_id, reader.datasets())
events = reader.read('historical_events', start='2026-10-01', end='2026-10-07')
features = reader.read('stock_features', partitions=['2026-10-07'])
pointer = reader.published_baseline()          # dict hoặc None
rows = reader.baseline_rows()                  # DataFrame của bản đã publish (LookupError nếu chưa có)
old = LakeReader(spark, cfg, generation=1)     # đọc lại một generation cũ (audit)
```

`LakeReader` không bao giờ ghi. Hai lần đọc trên cùng một đối tượng luôn cùng snapshot.

## 4. Kafka producer (crawler) — hợp đồng đầu vào

Topic `stock-price-topic` và `news-topic` (tên cấu hình ở `kafka.*`). Value là JSON UTF-8;
schema: [`contracts/kafka/stock-price-topic.schema.json`](../contracts/kafka/stock-price-topic.schema.json),
[`contracts/kafka/news-topic.schema.json`](../contracts/kafka/news-topic.schema.json); ví dụ:
[`examples/KAFKA_PAYLOADS.jsonl`](../examples/KAFKA_PAYLOADS.jsonl).

- Bắt buộc `schema_version: "1.0"`. Payload không phải JSON object → `INVALID_JSON`; thiếu
  `schema_version` → `MISSING_SCHEMA_VERSION`; cả hai vẫn được giữ nguyên trong Bronze và
  `bronze/bad_records`.
- Giá: `bar_start` ISO-8601 có múi giờ (UTC khuyến nghị), bar 1 phút `[start, start+1m)`;
  `source_price_unit` ∈ {`VND`, `THOUSAND_VND`}; nếu gửi `bar_end` thì phải bằng `bar_start + 1 phút`.
  Bản ghi trùng `(symbol, bar_start)` được chọn theo `source_priority`, `source_updated_at`,
  `kafka_timestamp`, offset.
- Tin: `url`, `title`, `source`, `crawled_at` bắt buộc; `published_at` nên có (thiếu → không được đếm
  vào mật độ tin); nội dung gửi `content_text` **hoặc** `content_html` (tùy chọn, HTML sẽ được làm
  sạch: bỏ thẻ/script/style, giải mã entity, chuẩn hoá Unicode NFC); `raw_html_ref` nếu HTML gốc lưu
  nơi khác và có quyền; `rights_policy` (tùy chọn) mô tả quyền sử dụng nội dung.
- Key gợi ý: `symbol` (giá), canonical URL hoặc `article_id` (tin). Header Kafka được lưu thành
  `map<string,string>` (khóa trùng: giữ giá trị đầu tiên).
- Gửi lại (at-least-once) an toàn: Bronze chống trùng theo `(topic, partition, offset)`; một bản sửa
  của bài báo nên gửi lại với `crawled_at` mới hơn (bản nhận sau cùng thắng).

### 4.1 Dữ liệu tham chiếu hiện chỉ là DEMO — Collector cần biết

`reference/symbol_master.csv` chỉ có **3 mã** (FPT, HPG, VNM), còn `reference/trading_calendar.csv` chỉ
có **5 ngày giao dịch HOSE** (2026-10-01, 02, 05, 06, 07; phiên 09:00–11:30 và 13:00–14:45). Đây là dữ
liệu minh họa, **không** phải danh mục hay lịch chính thức. Dữ liệu thật nằm ngoài phạm vi này vẫn được
lưu nguyên trong Bronze, nhưng B1 sẽ đưa vào `quarantine`:

| Trường hợp | `reason_code` |
|---|---|
| Mã không có trong `symbol_master.csv` | `UNKNOWN_SYMBOL` |
| Mã có nhưng ngày giao dịch ngoài `effective_from`–`effective_to` | `SYMBOL_NOT_EFFECTIVE` |
| Ngày/giờ không thuộc phiên nào trong `trading_calendar.csv` (kể cả mọi ngày ngoài 5 ngày demo) | `OUTSIDE_TRADING_SESSION` |

Tin tức vẫn vào `silver/news`, nhưng chỉ được map tới các mã có alias trong `symbol_master.csv`.

**Định dạng file** (CSV, UTF-8, có header, giờ là giờ địa phương Asia/Ho_Chi_Minh):

```text
symbol_master.csv:    symbol,exchange,company_name,aliases,effective_from,effective_to
                      aliases ngăn cách bằng '|', so khớp không phân biệt hoa/thường (Unicode),
                      nguyên từ; effective_to để trống = còn hiệu lực; các khoảng hiệu lực của
                      cùng một mã KHÔNG được chồng nhau (đổi sàn/đổi mã = thêm dòng mới).
trading_calendar.csv: exchange,trading_date,session_id,session_start,session_end,is_trading_day,calendar_version
                      một dòng / sàn / ngày / phiên (morning, afternoon); session_* dạng HH:mm;
                      is_trading_day = true|false.
```

**Các bước mở rộng lên 30–100 mã và lịch chính thức:**

1. Lấy danh mục mã và lịch giao dịch từ **nguồn chính thức** (HOSE/HNX/UPCoM hoặc nhà cung cấp dữ
   liệu có giấy phép). Không tự suy ra lịch từ dữ liệu giá (ngày thiếu bar ≠ ngày nghỉ).
2. Sinh hai file đúng định dạng trên, đặt `calendar_version` mới. Ngày lễ/nghỉ thì không có dòng
   (hoặc `is_trading_day=false`); phiên đặc biệt ghi giờ thật. Mã đổi sàn/đổi tên thì thêm dòng với
   khoảng hiệu lực không chồng nhau.
3. Trỏ cấu hình tới file mới (`market.calendar_file`, `market.symbol_file`) hoặc thay file trong
   `reference/`. `config_hash` gồm digest của hai file này, nên `baseline_version` mới sẽ khác bản
   demo. Đó là chủ đích.
4. Chạy `validate-config`. Khoảng hiệu lực chồng nhau bị báo lỗi khi B1/B2 đọc symbol master.
5. Backfill: `run --job all --start <đầu lịch sử> --end <hôm nay>`. Sau đó kiểm tra `quarantine` theo
   `reason_code`: `UNKNOWN_SYMBOL`/`OUTSIDE_TRADING_SESSION` còn nhiều thì file tham chiếu thiếu mã/ngày.
6. Chạy với cluster thật: file tham chiếu hiện được driver đọc từ đĩa local (`repository_root()`), nên
   ở cluster mode cần đưa chúng lên đường dẫn mà mọi executor đọc được.

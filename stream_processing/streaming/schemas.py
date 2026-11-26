"""Schema message + kiểm tra chất lượng bản ghi (valid / invalid -> dead-letter)."""
from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from config import SYMBOLS

# Cột _corrupt_record giữ nguyên dòng JSON hỏng để đẩy sang dead-letter.
STOCK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),      # thời điểm nến 1 phút (event time, giờ VN)
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("source", StringType()),
    StructField("ingested_at", StringType()),   # theo data contract của nhánh crawl-data
    StructField("ingest_ts", StringType()),     # tên cũ của bộ giả lập
    StructField("_corrupt_record", StringType()),
])

NEWS_SCHEMA = StructType([
    StructField("article_id", StringType()),
    StructField("title", StringType()),
    StructField("content", StringType()),
    StructField("published_at", StringType()),
    StructField("source", StringType()),
    StructField("url", StringType()),
    StructField("crawled_at", StringType()),
    StructField("_corrupt_record", StringType()),
])


def _first_reason(*pairs) -> Column:
    """Chuỗi when(...).when(...) -> lý do lỗi đầu tiên, null nếu bản ghi hợp lệ."""
    expr = None
    for cond, reason in pairs:
        expr = F.when(cond, reason) if expr is None else expr.when(cond, reason)
    return expr


def event_time(col: str) -> Column:
    """Parse timestamp chấp nhận mọi dạng gặp trong dữ liệu thật và giả lập:
    '2026-11-25 09:15:00+07:00', '2026-11-25T10:59:00.000000', '2026-11-09 10:35:00'.
    Không có múi giờ thì hiểu là giờ Việt Nam (session timezone). Chuỗi hỏng -> null (try_), không làm sập job."""
    return F.try_to_timestamp(F.col(col))


def parse_stock(raw: DataFrame) -> DataFrame:
    """Thêm cột ts (timestamp) và invalid_reason (null nếu hợp lệ).

    ts được làm tròn xuống phút: nến 1 phút của nguồn Mirae mang timestamp lệch giây (09:15:56) trong khi
    VNDirect là 09:15:00 - cùng một nến. Tròn phút thì bước loại trùng (symbol, ts) bắt được cả hai."""
    df = raw.withColumn("ts", F.date_trunc("minute", event_time("timestamp")))
    o, h, l, c, v = (F.col(x) for x in ("open", "high", "low", "close", "volume"))
    reason = _first_reason(
        (F.col("_corrupt_record").isNotNull(), "corrupt_json"),
        (~F.col("symbol").isin(*SYMBOLS.keys()) | F.col("symbol").isNull(), "unknown_symbol"),
        (F.col("ts").isNull(), "bad_timestamp"),
        (o.isNull() | h.isNull() | l.isNull() | c.isNull() | (o <= 0) | (h <= 0) | (l <= 0) | (c <= 0), "bad_price"),
        (v.isNull() | (v < 0), "bad_volume"),
        ((h < l) | (o > h) | (o < l) | (c > h) | (c < l), "ohlc_inconsistent"),
    )
    return df.withColumn("invalid_reason", reason)


def parse_news(raw: DataFrame) -> DataFrame:
    df = raw.withColumn("published_ts", event_time("published_at"))
    reason = _first_reason(
        (F.col("_corrupt_record").isNotNull(), "corrupt_json"),
        (F.col("article_id").isNull(), "missing_article_id"),
        (F.col("published_ts").isNull(), "bad_published_at"),
        (F.col("title").isNull() | (F.trim(F.col("title")) == ""), "missing_title"),
    )
    return df.withColumn("invalid_reason", reason)

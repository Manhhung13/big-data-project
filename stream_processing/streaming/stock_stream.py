"""Stock stream (mục 7.1): nến 1 phút -> window 5 phút -> so baseline -> flag Price/Volume anomaly."""
from pyspark.sql import DataFrame, functions as F

from config import PATHS, STOCK_WATERMARK, TS_FORMAT
from streaming.detect import detect_stock
from streaming.features import window_features
from streaming.schemas import STOCK_SCHEMA, parse_stock
from streaming.sources import read_json_stream


def build_stock_windows(spark) -> DataFrame:
    """Streaming DataFrame: mỗi dòng là trạng thái hiện tại của một (symbol, window 5 phút)."""
    parsed = parse_stock(read_json_stream(spark, PATHS["stream_in_stock"], STOCK_SCHEMA))
    return window_features(
        parsed.filter(F.col("invalid_reason").isNull())
        .withWatermark("ts", STOCK_WATERMARK)                       # event-time + watermark (xử lý dữ liệu trễ)
        .dropDuplicatesWithinWatermark(["symbol", "ts"])            # collector gửi lại cùng một nến -> không cộng đôi volume
    )


def make_stock_handler(provider, sink):
    def handle(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        # Timestamp -> chuỗi ngay trong Spark (theo session timezone). collect() timestamp ở Python
        # sẽ dùng timezone của máy chạy Python, dễ lệch giờ.
        out = (
            detect_stock(batch_df, provider.get())
            .select(
                "symbol",
                F.date_format("window_start", TS_FORMAT).alias("window_start"),
                F.date_format("window_end", TS_FORMAT).alias("window_end"),
                "time_bucket", "open", "high", "low", "close", "volume", "n_bars", "ret",
                "return_z", "volume_z",
                F.col("price_anomaly").cast("int").alias("price_anomaly"),
                F.col("volume_anomaly").cast("int").alias("volume_anomaly"),
                F.date_format("last_bar_ts", TS_FORMAT).alias("last_bar_ts"),
            )
        )
        rows = [r.asDict() for r in out.collect()]
        sink.upsert("signals", ["symbol", "window_start"], rows)
        sink.set_meta_max("max_event_time", max(r["last_bar_ts"] for r in rows))
    return handle


def start_stock_stream(spark, provider, sink, trigger: dict):
    # update mode: window hiện tại được đẩy ra ngay mỗi micro-batch (phát hiện sớm, volume tăng dần),
    # ghi idempotent theo (symbol, window_start) nên bản mới nhất luôn thắng; view market_event tính is_final.
    return (
        build_stock_windows(spark).writeStream
        .queryName("stock_windows")
        .outputMode("update")
        .foreachBatch(make_stock_handler(provider, sink))
        .option("checkpointLocation", str(PATHS["checkpoints"] / "stock_windows"))
        .trigger(**trigger)
        .start()
    )

"""Tính feature theo window. Dùng CHUNG cho Spark Batch (baseline) và Spark Streaming
để baseline và dữ liệu realtime được tính bằng cùng một công thức (tránh lệch train/serve)."""
from pyspark.sql import DataFrame, functions as F

from config import STOCK_WINDOW


def ohlcv_aggs():
    """Gộp nến 1 phút -> nến theo window. open/close lấy theo thứ tự thời gian, không theo thứ tự đến."""
    return [
        F.min_by("open", "ts").alias("open"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.max_by("close", "ts").alias("close"),
        F.sum("volume").alias("volume"),
        F.count(F.lit(1)).alias("n_bars"),
        F.max("ts").alias("last_bar_ts"),
    ]


def window_features(df: DataFrame, window_duration: str = STOCK_WINDOW) -> DataFrame:
    """df: symbol, ts, open, high, low, close, volume (đã withWatermark nếu là stream)."""
    return (
        df.groupBy(F.window("ts", window_duration), "symbol")
        .agg(*ohlcv_aggs())
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open", "high", "low", "close", "volume", "n_bars", "last_bar_ts",
        )
        .withColumn("ret", F.col("close") / F.col("open") - F.lit(1.0))
        .withColumn("time_bucket", F.date_format("window_start", "HH:mm"))
    )

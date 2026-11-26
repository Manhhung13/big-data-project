"""Phát hiện bất thường: so giá trị hiện tại với baseline theo (symbol, time_bucket).

Toàn bộ là biến đổi DataFrame thuần (không phụ thuộc streaming) nên dùng được trong
foreachBatch và test được bằng DataFrame tĩnh.

Quy tắc (mục 8.1, ngưỡng khởi tạo):
  price : |z_return| >= Z  hoặc  |return| >= P99(|return|) lịch sử
  volume: z_volume  >= Z   hoặc  volume   >= P99(volume)
  news  : (z_news30 >= Z hoặc count30 >= P99) VÀ count30 >= NEWS_MIN_COUNT_30M
Không có baseline / baseline quá ít mẫu / std ~ 0 (z-score không tính được)  ->  KHÔNG gắn flag (không báo bừa),
kể cả luật percentile.
"""
from pyspark.sql import Column, DataFrame, functions as F

from config import MIN_BASELINE_SAMPLES, NEWS_MIN_COUNT_30M, STD_EPS, Z_THRESHOLD


def safe_z(value: Column, mean: Column, std: Column, n: Column) -> Column:
    """z-score, trả null nếu baseline thiếu / ít mẫu / độ lệch chuẩn ~ 0."""
    return F.when(
        n.isNotNull() & (n >= MIN_BASELINE_SAMPLES) & std.isNotNull() & (std > STD_EPS),
        (value - mean) / std,
    )


def _true_or_false(cond: Column) -> Column:
    """Điều kiện null (thiếu baseline) -> False."""
    return F.coalesce(cond, F.lit(False))


def _reliable(z: Column) -> Column:
    """Baseline đáng tin <=> z-score tính được (đủ mẫu và std > 0).
    Dùng chung cho cả luật z-score lẫn luật percentile: baseline suy biến thì không luật nào được phép báo."""
    return z.isNotNull()


def detect_stock(df: DataFrame, baseline: DataFrame) -> DataFrame:
    """df: đầu ra window_features. Thêm return_z, volume_z, price_anomaly, volume_anomaly."""
    b = baseline.select(
        "symbol", "time_bucket", "n_samples_stock",
        "mean_return", "std_return", "p99_abs_return",
        "mean_volume", "std_volume", "p99_volume",
    )
    d = df.join(F.broadcast(b), ["symbol", "time_bucket"], "left")
    n = F.col("n_samples_stock")
    d = (
        d.withColumn("return_z", safe_z(F.col("ret"), F.col("mean_return"), F.col("std_return"), n))
        .withColumn("volume_z", safe_z(F.col("volume"), F.col("mean_volume"), F.col("std_volume"), n))
        .withColumn(
            "price_anomaly",
            _true_or_false(
                _reliable(F.col("return_z"))
                & ((F.abs(F.col("return_z")) >= Z_THRESHOLD) | (F.abs(F.col("ret")) >= F.col("p99_abs_return")))
            ),
        )
        .withColumn(
            "volume_anomaly",
            _true_or_false(
                _reliable(F.col("volume_z"))
                & ((F.col("volume_z") >= Z_THRESHOLD) | (F.col("volume") >= F.col("p99_volume")))
            ),
        )
    )
    return d.drop("n_samples_stock", "mean_return", "std_return", "p99_abs_return",
                  "mean_volume", "std_volume", "p99_volume")


def detect_news(df: DataFrame, baseline: DataFrame) -> DataFrame:
    """df: symbol, time_bucket, news_30m. Thêm news_z_30m, news_burst."""
    b = baseline.select("symbol", "time_bucket", "n_samples_news",
                        "mean_news_30m", "std_news_30m", "p99_news_30m")
    d = df.join(F.broadcast(b), ["symbol", "time_bucket"], "left")
    n = F.col("n_samples_news")
    cnt = F.col("news_30m")
    d = (
        d.withColumn("news_z_30m", safe_z(cnt, F.col("mean_news_30m"), F.col("std_news_30m"), n))
        .withColumn(
            "news_burst",
            _true_or_false(
                _reliable(F.col("news_z_30m"))
                & (cnt >= NEWS_MIN_COUNT_30M)
                & ((F.col("news_z_30m") >= Z_THRESHOLD) | (cnt >= F.col("p99_news_30m")))
            ),
        )
    )
    return d.drop("n_samples_news", "mean_news_30m", "std_news_30m", "p99_news_30m")

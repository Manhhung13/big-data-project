"""Spark Batch dựng baseline theo (symbol, time_bucket) - bản rút gọn của B1-B5 để cấp dữ liệu cho Speed Layer.

  python -m batch.build_baseline                       # dữ liệu thật (Bronze của nhánh crawl-data)
  BIGDATA_SOURCE=sim python -m batch.build_baseline    # dữ liệu giả lập

Dùng CHUNG streaming.schemas / features / news_mapping với phần streaming nên baseline và dữ liệu
realtime được parse, làm sạch và tính feature bằng cùng một công thức.

Dữ liệu thật: lấy BASELINE_DAYS ngày giao dịch ngay trước REPLAY_DATE (ngày phát lại không nằm trong baseline).
Nến trùng nguồn (VNDirect + Mirae cùng một phút) được loại, ưu tiên VNDirect.
Tin tức được điền 0 cho slot không có bài (nếu bỏ slot 0 thì mean/std bị lệch lên, burst khó bị phát hiện).
"""
from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from config import (BASELINE_DAYS, NEWS_BASELINE_BY_BUCKET, NEWS_BASELINE_HOURS, NEWS_SLIDE, PATHS,
                    REPLAY_DATE, SOURCE, SOURCE_PRIORITY, SYMBOLS, TS_FORMAT)
from spark_env import get_spark
from streaming.features import window_features
from streaming.news_mapping import content_hash, news_text, symbols_in_text
from streaming.schemas import NEWS_SCHEMA, event_time, parse_news

STOCK_CSV_SCHEMA = StructType([
    StructField("symbol", StringType()), StructField("timestamp", StringType()),
    StructField("open", DoubleType()), StructField("high", DoubleType()),
    StructField("low", DoubleType()), StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
])


def _stats(col: str, name: str, percentiles: tuple) -> list:
    exprs = [F.mean(col).alias(f"mean_{name}"), F.stddev_samp(col).alias(f"std_{name}")]
    exprs += [F.percentile_approx(col, p).alias(f"p{int(p * 100)}_{name}") for p in percentiles]
    return exprs


# ------------------------------------------------------------------------------------------ giá
def read_stock(spark: SparkSession) -> DataFrame:
    """Nến 1 phút sạch: symbol, ts (tròn phút), open, high, low, close, volume - mỗi (symbol, ts) đúng 1 dòng."""
    if SOURCE == "sim":
        df = spark.read.option("header", True).schema(STOCK_CSV_SCHEMA).csv(str(PATHS["history_stock"]))
        return df.withColumn("ts", F.date_trunc("minute", event_time("timestamp")))

    df = (spark.read.parquet(str(PATHS["bronze_stock"]))
          .withColumn("ts", F.date_trunc("minute", event_time("timestamp")))
          .withColumn("volume", F.col("volume").cast("double"))
          .filter(F.col("symbol").isin(*SYMBOLS.keys()) & F.col("ts").isNotNull()))
    # Ưu tiên nguồn theo SOURCE_PRIORITY khi một phút có nhiều nến (26/10-10/11/2026 có cả VNDirect lẫn Mirae).
    prio = F.coalesce(*[F.when(F.col("source") == s, F.lit(i)) for i, s in enumerate(SOURCE_PRIORITY)],
                      F.lit(len(SOURCE_PRIORITY)))
    w = Window.partitionBy("symbol", "ts").orderBy(prio, F.col("timestamp"))
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    # BASELINE_DAYS ngày giao dịch ngay trước ngày phát lại.
    days = (df.select(F.to_date("ts").alias("d")).distinct()
            .filter(F.col("d") < F.lit(REPLAY_DATE).cast("date"))
            .orderBy(F.col("d").desc()).limit(BASELINE_DAYS))
    return df.join(days, F.to_date("ts") == days.d, "inner").drop("d")


def stock_baseline(stock: DataFrame) -> DataFrame:
    # Mọi window đều dùng: với dữ liệu thật, phút không có nến nghĩa là không có giao dịch (volume 0),
    # nên window ít nến vẫn là số liệu thật (vd window 14:45 chỉ có 1 nến ATC).
    w = window_features(stock).withColumn("abs_ret", F.abs("ret"))
    return w.groupBy("symbol", "time_bucket").agg(
        F.count(F.lit(1)).alias("n_samples_stock"),
        F.mean("ret").alias("mean_return"),
        F.stddev_samp("ret").alias("std_return"),
        F.percentile_approx("abs_ret", 0.99).alias("p99_abs_return"),
        *_stats("volume", "volume", (0.95, 0.99)),
    )


# ------------------------------------------------------------------------------------------ tin
def read_news(spark: SparkSession) -> DataFrame:
    if SOURCE == "sim":
        return (spark.read.schema(NEWS_SCHEMA).option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record").json(str(PATHS["history_news"])))
    raw = spark.read.parquet(str(PATHS["bronze_news"]))
    return raw.select(*[F.col(f.name) if f.name in raw.columns else F.lit(None).cast("string").alias(f.name)
                        for f in NEWS_SCHEMA.fields])


def news_baseline(spark: SparkSession, stock: DataFrame) -> DataFrame:
    arts = (
        parse_news(read_news(spark)).filter(F.col("invalid_reason").isNull())
        .withColumn("content_hash", content_hash(F.col("title"), F.col("content")))
        .dropDuplicates(["content_hash"])
        .withColumn("symbol", F.explode(symbols_in_text(news_text(F.col("title"), F.col("content")), SYMBOLS)))
    )
    slide = f"INTERVAL {NEWS_SLIDE}"
    c5 = (arts.groupBy(F.window("published_ts", "5 minutes", NEWS_SLIDE), "symbol").count()
          .select("symbol", (F.col("window.end") - F.expr(slide)).alias("slot_start"), F.col("count").alias("news_5m")))
    c30 = (arts.groupBy(F.window("published_ts", "30 minutes", NEWS_SLIDE), "symbol").count()
           .select("symbol", (F.col("window.end") - F.expr(slide)).alias("slot_start"), F.col("count").alias("news_30m")))

    # Lưới đầy đủ (ngày giao dịch của baseline x slot x mã) để slot không có bài được tính là 0.
    lo, hi = NEWS_BASELINE_HOURS
    days = stock.select(F.to_date("ts").alias("d")).distinct()
    slots = days.select(F.explode(F.sequence(
        F.to_timestamp(F.concat(F.col("d").cast("string"), F.lit(f" {lo}:00")), TS_FORMAT),
        F.to_timestamp(F.concat(F.col("d").cast("string"), F.lit(f" {hi}:00")), TS_FORMAT) - F.expr(slide),
        F.expr(slide),
    )).alias("slot_start"))
    # Không dùng spark.createDataFrame(list): trên Windows + Python 3.13 Python worker của PySpark 4.0 bị crash.
    syms = spark.range(1).select(F.explode(F.array(*[F.lit(s) for s in SYMBOLS])).alias("symbol"))
    grid = slots.crossJoin(syms)

    daily = (grid.join(c5, ["symbol", "slot_start"], "left").join(c30, ["symbol", "slot_start"], "left")
             .fillna(0, ["news_5m", "news_30m"])
             .withColumn("time_bucket", F.date_format("slot_start", "HH:mm")))
    stats = [*_stats("news_5m", "news_5m", (0.95, 0.99)), *_stats("news_30m", "news_30m", (0.95, 0.99))]
    if NEWS_BASELINE_BY_BUCKET:
        return daily.groupBy("symbol", "time_bucket").agg(F.countDistinct(F.to_date("slot_start")).alias("n_samples_news"), *stats)
    # Tin thật quá thưa theo từng khung giờ -> gộp mọi slot trong ngày của mã, rồi gán cùng giá trị cho mọi khung giờ.
    per_symbol = daily.groupBy("symbol").agg(F.countDistinct(F.to_date("slot_start")).alias("n_samples_news"), *stats)
    buckets = daily.select("time_bucket").distinct()
    return per_symbol.crossJoin(buckets)


def main():
    spark = get_spark("build-baseline", cores=4)
    stock = read_stock(spark).cache()
    n_days = stock.select(F.to_date("ts")).distinct().count()
    base = stock_baseline(stock).join(news_baseline(spark, stock), ["symbol", "time_bucket"], "full_outer")
    base.coalesce(1).write.mode("overwrite").parquet(str(PATHS["baseline"]))
    out = spark.read.parquet(str(PATHS["baseline"]))
    print(f"[baseline] source={SOURCE} days={n_days} symbols={out.select('symbol').distinct().count()} "
          f"rows={out.count()} -> {PATHS['baseline']}")
    spark.stop()


if __name__ == "__main__":
    main()

"""News stream (mục 7.2): dedup -> map bài -> mã -> đếm theo window 5m/30m/1h -> News Burst."""
from pyspark.sql import DataFrame, functions as F

from config import NEWS_SLIDE, NEWS_WATERMARK, NEWS_WINDOWS, PATHS, SYMBOLS, TS_FORMAT
from streaming.detect import detect_news
from streaming.news_mapping import content_hash, news_text, symbols_in_text
from streaming.schemas import NEWS_SCHEMA, parse_news
from streaming.sources import read_json_stream


def build_articles(spark) -> DataFrame:
    """Bài đã làm sạch: hợp lệ, loại trùng, mỗi dòng = (bài, mã) - một bài nhắc nhiều mã thì tách nhiều dòng."""
    parsed = parse_news(read_json_stream(spark, PATHS["stream_in_news"], NEWS_SCHEMA))
    return (
        parsed.filter(F.col("invalid_reason").isNull())
        .withColumn("content_hash", content_hash(F.col("title"), F.col("content")))
        .withWatermark("published_ts", NEWS_WATERMARK)
        # Trùng theo nội dung: bắt cả bài gửi lại (cùng article_id) lẫn bài đăng lại ở nguồn khác (khác id/url).
        .dropDuplicatesWithinWatermark(["content_hash"])
        .withColumn("symbols", symbols_in_text(news_text(F.col("title"), F.col("content")), SYMBOLS))
        .withColumn("symbol", F.explode("symbols"))    # bài không nhắc mã nào -> bị loại tại đây
        .drop("symbols")
    )


def news_density(articles: DataFrame, window_duration: str) -> DataFrame:
    """Đếm số bài theo (symbol, window). Window luôn trượt theo lưới 5 phút để khớp window giá."""
    return articles.groupBy(F.window("published_ts", window_duration, NEWS_SLIDE), "symbol").count()


def _slot_frame(batch_df: DataFrame, kind: str) -> DataFrame:
    """Window kết thúc tại E -> gán vào slot 5 phút [E-5', E): 'số bài trong N phút tính đến hết slot đó'.
    Nhờ vậy khóa (symbol, slot) khớp trực tiếp với window giá 5 phút."""
    return (
        batch_df.select(
            "symbol",
            (F.col("window.end") - F.expr(f"INTERVAL {NEWS_SLIDE}")).alias("slot_start"),
            F.col("window.end").alias("slot_end"),
            F.col("count").cast("int").alias(f"news_{kind}"),
        )
        .withColumn("time_bucket", F.date_format("slot_start", "HH:mm"))
    )


def make_density_handler(kind: str, provider, sink):
    def handle(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        df = _slot_frame(batch_df, kind)
        if kind == "30m":                       # chỉ cửa sổ 30' dùng để phát hiện News Burst
            df = detect_news(df, provider.get())
        cols = [
            "symbol",
            F.date_format("slot_start", TS_FORMAT).alias("window_start"),
            F.date_format("slot_end", TS_FORMAT).alias("window_end"),
            "time_bucket",
            f"news_{kind}",
        ]
        if kind == "30m":
            cols += ["news_z_30m", F.col("news_burst").cast("int").alias("news_burst")]
        rows = [r.asDict() for r in df.select(*cols).collect()]
        sink.upsert("signals", ["symbol", "window_start"], rows)
    return handle


def make_articles_handler(sink):
    def handle(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        rows = [
            r.asDict() for r in batch_df.select(
                "article_id", "symbol", "title",
                F.date_format("published_ts", TS_FORMAT).alias("published_at"),
                "source", "url",
            ).collect()
        ]
        sink.upsert("articles", ["article_id", "symbol"], rows)
    return handle


def start_news_streams(spark, provider, sink, trigger: dict):
    """3 query density (5m/30m/1h) + 1 query lưu bài liên quan để dashboard tra cứu."""
    queries = []
    for kind, duration in NEWS_WINDOWS.items():
        # update mode: số đếm chỉ tăng nên cờ burst đơn điệu; ghi đè theo khóa là an toàn.
        q = (
            news_density(build_articles(spark), duration).writeStream
            .queryName(f"news_density_{kind}")
            .outputMode("update")
            .foreachBatch(make_density_handler(kind, provider, sink))
            .option("checkpointLocation", str(PATHS["checkpoints"] / f"news_density_{kind}"))
            .trigger(**trigger)
            .start()
        )
        queries.append(q)
    queries.append(
        build_articles(spark).writeStream
        .queryName("news_articles")
        .outputMode("append")
        .foreachBatch(make_articles_handler(sink))
        .option("checkpointLocation", str(PATHS["checkpoints"] / "news_articles"))
        .trigger(**trigger)
        .start()
    )
    return queries

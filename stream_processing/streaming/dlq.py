"""Dead-letter: bản ghi lỗi (JSON hỏng, sai schema/giá trị) được giữ lại kèm lý do thay vì làm hỏng job."""
from pyspark.sql import DataFrame, functions as F

from config import PATHS, TS_FORMAT
from streaming.schemas import NEWS_SCHEMA, STOCK_SCHEMA, parse_news, parse_stock
from streaming.sources import read_json_stream


def _handler(topic: str, raw_cols: list, sink):
    def handle(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        rows = [
            {"topic": topic, "reason": r["invalid_reason"], "raw": r["raw"],
             "received_at": r["received_at"]}
            for r in batch_df.select(
                "invalid_reason",
                F.coalesce(F.col("_corrupt_record"), F.to_json(F.struct(*raw_cols))).alias("raw"),
                F.date_format(F.current_timestamp(), TS_FORMAT).alias("received_at"),
            ).collect()
        ]
        sink.insert("dlq", rows)
    return handle


def start_dlq_streams(spark, sink, trigger: dict):
    specs = [
        ("stock-price-topic", PATHS["stream_in_stock"], STOCK_SCHEMA, parse_stock,
         ["symbol", "timestamp", "open", "high", "low", "close", "volume"]),
        ("news-topic", PATHS["stream_in_news"], NEWS_SCHEMA, parse_news,
         ["article_id", "title", "published_at", "source", "url"]),
    ]
    queries = []
    for topic, path, schema, parser, raw_cols in specs:
        bad = parser(read_json_stream(spark, path, schema)).filter(F.col("invalid_reason").isNotNull())
        queries.append(
            bad.writeStream
            .queryName(f"dlq_{topic}")
            .outputMode("append")
            .foreachBatch(_handler(topic, raw_cols, sink))
            .option("checkpointLocation", str(PATHS["checkpoints"] / f"dlq_{topic}"))
            .trigger(**trigger)
            .start()
        )
    return queries

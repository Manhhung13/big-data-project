"""Chạy toàn bộ streaming job.

  python -m streaming.run --mode availableNow --reset   # xử lý hết dữ liệu đang có rồi dừng (dùng để test/demo nhanh)
  python -m streaming.run --mode continuous              # chạy liên tục, dùng cùng simulator.gen_live --pace N
"""
import argparse
import shutil
import time

from config import BASELINE_TTL_SECONDS, PATHS, TRIGGER_SECONDS
from spark_env import get_spark
from streaming.baseline import BaselineProvider
from streaming.dlq import start_dlq_streams
from streaming.news_stream import start_news_streams
from streaming.sinks import SqliteSink
from streaming.stock_stream import start_stock_stream


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["availableNow", "continuous"], default="availableNow")
    ap.add_argument("--reset", action="store_true", help="xóa checkpoint + serving db trước khi chạy")
    ap.add_argument("--duration", type=int, default=0, help="continuous: tự dừng sau N giây (0 = chạy mãi)")
    args = ap.parse_args()

    if args.reset:
        shutil.rmtree(PATHS["checkpoints"], ignore_errors=True)
        for suffix in ("", "-wal", "-shm"):
            p = PATHS["serving_db"].with_name(PATHS["serving_db"].name + suffix)
            p.unlink(missing_ok=True)

    spark = get_spark("market-event-streaming")
    sink = SqliteSink(PATHS["serving_db"])
    sink.init_schema()
    provider = BaselineProvider(spark, PATHS["baseline"], BASELINE_TTL_SECONDS)
    provider.get()  # fail sớm nếu chưa có baseline

    trigger = {"availableNow": True} if args.mode == "availableNow" else {"processingTime": f"{TRIGGER_SECONDS} seconds"}
    queries = [start_stock_stream(spark, provider, sink, trigger)]
    queries += start_news_streams(spark, provider, sink, trigger)
    queries += start_dlq_streams(spark, sink, trigger)
    print(f"[run] started {len(queries)} queries: {[q.name for q in queries]}", flush=True)

    try:
        if args.mode == "availableNow":
            for q in queries:
                q.awaitTermination()
        else:
            deadline = time.time() + args.duration if args.duration else None
            while deadline is None or time.time() < deadline:
                for q in queries:
                    # Một query chết (kể cả do java.lang.Error như StackOverflowError, khi đó exception() có thể là None)
                    # phải làm cả job dừng và báo lỗi, không được âm thầm chạy tiếp với thiếu query.
                    if not q.isActive:
                        raise RuntimeError(f"query '{q.name}' đã dừng bất thường: {q.exception()}")
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        # Dừng giữa lúc đang xử lý batch sẽ ngắt batch đó. An toàn: ghi theo khóa idempotent và Spark chạy lại batch dang dở
        # từ checkpoint khi khởi động lại. Spark có thể in vài StackOverflowError ở bước phân loại exception lúc tắt - chỉ là nhiễu.
        for q in queries:
            try:
                if q.isActive:
                    q.stop()
            except Exception as e:  # noqa: BLE001
                print(f"[run] stop {q.name}: {e}", flush=True)
        spark.stop()
    print("[run] done", flush=True)


if __name__ == "__main__":
    main()

import os
import sys
import json
import time
import signal
import argparse
from datetime import datetime

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import (
    KAFKA_BROKERS,
    STOCK_TOPIC,
    NEWS_TOPIC,
    BRONZE_CONSUMER_GROUP,
    BRONZE_STOCK_DIR,
    BRONZE_NEWS_DIR,
    BRONZE_FLUSH_INTERVAL_SECONDS,
    TZ_VN,
)
from bronze_storage import merge_stock_records, merge_news_records

running = True


def flush_buffers(stock_records: list[dict], news_records: list[dict]) -> tuple[int, int]:
    """
    Gộp dữ liệu mới nhận từ Kafka trực tiếp vào các phân vùng theo tháng:
    - data/bronze/stock/year=YYYY/month=MM/{SYMBOL}_1m.parquet
    - data/bronze/news/year=YYYY/month=MM/news.parquet
    """
    _, added_stock = merge_stock_records(stock_records, stock_dir=BRONZE_STOCK_DIR)
    _, added_news = merge_news_records(news_records, news_dir=BRONZE_NEWS_DIR)
    return added_stock, added_news


def shutdown_handler(signum, frame):
    """Đánh dấu dừng vòng lặp an toàn để flush nốt dữ liệu đang giữ trước khi thoát."""
    global running
    print("\n[Bronze Sink] Nhận tín hiệu dừng. Đang lưu nốt dữ liệu đệm xuống Parquet...")
    running = False


def run_bronze_sink(
    group_id: str = BRONZE_CONSUMER_GROUP,
    flush_interval: float = BRONZE_FLUSH_INTERVAL_SECONDS,
    from_beginning: bool = True,
    once: bool = False,
):
    """Chạy Consumer lưu Bronze Store: Đọc Kafka (stock & news) và hẹn giờ lưu theo tháng mỗi 1 giờ."""
    offset_reset = "earliest" if from_beginning else "latest"

    consumer = None
    for attempt in range(1, 16):
        if not running:
            return
        try:
            consumer = KafkaConsumer(
                STOCK_TOPIC,
                NEWS_TOPIC,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset=offset_reset,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            break
        except KafkaError as e:
            print(f"[Bronze Sink] Chờ Kafka ({KAFKA_BROKERS}) lần {attempt}/15: {e}")
            time.sleep(2)

    if consumer is None:
        print(f"[Bronze Sink][Lỗi] Không thể kết nối Kafka Brokers {KAFKA_BROKERS} sau nhiều lần thử.")
        sys.exit(1)

    os.makedirs(BRONZE_STOCK_DIR, exist_ok=True)
    os.makedirs(BRONZE_NEWS_DIR, exist_ok=True)

    interval_desc = f"{flush_interval:.0f}s (~{flush_interval / 3600:.2f}h)"
    print("=================================================================")
    print("   KAFKA -> BRONZE STORE (PHÂN VÙNG THEO THÁNG YEAR/MONTH)       ")
    print(f"   - Kafka Brokers : {KAFKA_BROKERS}")
    print(f"   - Topics        : {STOCK_TOPIC}, {NEWS_TOPIC}")
    print(f"   - Consumer Group: {group_id} (auto_offset_reset={offset_reset})")
    print(f"   - Stock Store   : {os.path.join(BRONZE_STOCK_DIR, 'year=YYYY', 'month=MM', '{SYMBOL}_1m.parquet')}")
    print(f"   - News Store    : {os.path.join(BRONZE_NEWS_DIR, 'year=YYYY', 'month=MM', 'news.parquet')}")
    print(f"   - Chu kỳ lưu    : Mỗi {interval_desc}" + (" [CHẾ ĐỘ CHẠY 1 LẦN]" if once else ""))
    print("=================================================================\n")

    stock_buffer = []
    news_buffer = []
    last_flush_ts = time.time()
    empty_polls = 0

    try:
        while running:
            records_pack = consumer.poll(timeout_ms=1000, max_records=500)
            received_now = 0
            for tp, messages in records_pack.items():
                for msg in messages:
                    payload = msg.value
                    if not isinstance(payload, dict):
                        continue
                    if tp.topic == STOCK_TOPIC:
                        stock_buffer.append(payload)
                        received_now += 1
                    elif tp.topic == NEWS_TOPIC:
                        news_buffer.append(payload)
                        received_now += 1

            if once:
                if not consumer.assignment():
                    continue
                if received_now == 0:
                    empty_polls += 1
                    if empty_polls >= 3:
                        break
                else:
                    empty_polls = 0
                continue

            elapsed = time.time() - last_flush_ts
            if elapsed >= flush_interval:
                if stock_buffer or news_buffer:
                    s_added, n_added = flush_buffers(stock_buffer, news_buffer)
                    consumer.commit()
                    now_str = datetime.now(TZ_VN).strftime("%H:%M:%S")
                    print(
                        f"[{now_str}][BRONZE SINK] Đã gộp vào phân vùng tháng & commit offset: "
                        f"nhận {len(stock_buffer)} stock (+{s_added} dòng mới), "
                        f"{len(news_buffer)} news (+{n_added} bài mới)."
                    )
                    stock_buffer.clear()
                    news_buffer.clear()
                else:
                    now_str = datetime.now(TZ_VN).strftime("%H:%M:%S")
                    print(f"[{now_str}][BRONZE SINK] Chu kỳ {flush_interval:.0f}s: Không có dữ liệu mới từ Kafka.")
                last_flush_ts = time.time()

        if stock_buffer or news_buffer:
            s_added, n_added = flush_buffers(stock_buffer, news_buffer)
            consumer.commit()
            now_str = datetime.now(TZ_VN).strftime("%H:%M:%S")
            print(
                f"[{now_str}][BRONZE SINK] Đã lưu gộp batch cuối vào phân vùng tháng & commit offset: "
                f"{len(stock_buffer)} stock (+{s_added} mới), {len(news_buffer)} news (+{n_added} mới)."
            )
    finally:
        consumer.close()
        print("[Bronze Sink] Đã đóng kết nối Kafka an toàn.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    parser = argparse.ArgumentParser(description="Kafka Consumer hẹn giờ gộp theo tháng vào Bronze Store Parquet")
    parser.add_argument("--group-id", type=str, default=BRONZE_CONSUMER_GROUP, help="Kafka Consumer Group ID")
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=BRONZE_FLUSH_INTERVAL_SECONDS,
        help="Chu kỳ lưu xuống Parquet (giây, mặc định: 3600 = 1 giờ)",
    )
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="Chỉ đọc message mới từ thời điểm khởi động nếu chưa có offset",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Rút hết dữ liệu đang chờ trong Kafka, lưu gộp ngay lập tức rồi thoát",
    )
    args = parser.parse_args()

    run_bronze_sink(
        group_id=args.group_id,
        flush_interval=args.flush_interval,
        from_beginning=not args.latest_only,
        once=args.once,
    )

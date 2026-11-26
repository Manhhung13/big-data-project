import os
import sys
import json
import time
import glob
import signal
import argparse
import pandas as pd
from datetime import datetime

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_BROKERS, STOCK_TOPIC, NEWS_TOPIC, BRONZE_STOCK_DIR, BRONZE_NEWS_DIR, TZ_VN

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FALLBACK_STOCK_DIR = os.path.join(SCRIPT_DIR, "historical_bronze", "stock")

def get_kafka_producer():
    """Khởi tạo KafkaProducer cho Replay engine."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else str(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1,
            retries=3,
            request_timeout_ms=5000
        )
        print(f"[Kafka Replay] Kết nối thành công tới Kafka Brokers: {KAFKA_BROKERS}")
        return producer
    except KafkaError as e:
        print(f"[Kafka Replay] Không thể kết nối tới {KAFKA_BROKERS} ({e}). Sẽ chạy ở chế độ DRY-RUN (in ra màn hình).")
        return None
    except Exception as e:
        print(f"[Kafka Replay] Lỗi: {e}. Sẽ chạy ở chế độ DRY-RUN.")
        return None

producer = get_kafka_producer()

def _load_news_queue(limit: int = 50) -> list[dict]:
    """Tải một số bài báo gần nhất từ Bronze News (ưu tiên phân vùng tháng mới nhất) để demo."""
    news_files = sorted(glob.glob(os.path.join(BRONZE_NEWS_DIR, "year=*", "month=*", "news.parquet")))
    if news_files:
        news_file = news_files[-1]
    else:
        news_file = os.path.join(BRONZE_NEWS_DIR, "historical_news.parquet")
    if not os.path.exists(news_file):
        return []
    try:
        df_news = pd.read_parquet(news_file).head(limit)
        return df_news.to_dict(orient="records")
    except Exception:
        return []

def replay_market_data(speed_seconds: float = 2.0, loop: bool = False, max_bars: int = None, include_news: bool = False):
    """
    Phát lại dữ liệu lịch sử theo từng mốc thời gian (minute bar).
    speed_seconds=2 nghĩa là cứ 2 giây ngoài đời thực sẽ bắn 1 phút dữ liệu của toàn bộ các mã vào Kafka.
    """
    month_dirs = sorted(glob.glob(os.path.join(BRONZE_STOCK_DIR, "year=*", "month=*")))
    if month_dirs:
        files = sorted(glob.glob(os.path.join(month_dirs[-1] if max_bars else os.path.join(BRONZE_STOCK_DIR, "year=*", "month=*"), "*_1m.parquet")))
    else:
        files = sorted(glob.glob(os.path.join(BRONZE_STOCK_DIR, "*_1m.parquet")))
    if not files:
        print(f"[Cảnh báo] Chưa có dữ liệu lịch sử tại: {BRONZE_STOCK_DIR}!")
        print("Hãy chạy 'python backfill_vn30_3y_1m.py' trước để tạo dữ liệu.")
        return

    print(f"[Replay Engine] Đang tải {len(files)} tệp Parquet từ {BRONZE_STOCK_DIR}...")
    dfs = []
    for f in files:
        try:
            df_item = pd.read_parquet(f)
            if not df_item.empty:
                # Nếu có giới hạn số nến phát, chỉ lấy phần đuôi mới nhất để khởi động tức thì
                if max_bars:
                    df_item = df_item.tail(max_bars)
                dfs.append(df_item)
        except Exception as e:
            print(f"  Lỗi khi đọc file {os.path.basename(f)}: {e}")

    if not dfs:
        print("[Lỗi] Không có dữ liệu hợp lệ trong các tệp parquet.")
        return

    full_df = pd.concat(dfs, ignore_index=True)
    full_df['timestamp'] = full_df['timestamp'].astype(str)

    # Sắp xếp theo mốc thời gian và gom nhóm theo từng phút
    grouped = list(full_df.sort_values('timestamp').groupby('timestamp'))
    if max_bars and len(grouped) > max_bars:
        grouped = grouped[-max_bars:]
    total_bars = len(grouped)

    news_items = _load_news_queue(limit=max(20, total_bars * 2)) if include_news else []
    mode_str = "KAFKA" if producer else "DRY-RUN"

    print("=================================================================")
    print("        BẮT ĐẦU REPLAY GIẢ LẬP DỮ LIỆU THỊ TRƯỜNG REALTIME       ")
    print(f"   - Kafka Topic    : {STOCK_TOPIC}" + (f" + {NEWS_TOPIC}" if include_news else ""))
    print(f"   - Chế độ         : {mode_str}")
    print(f"   - Tốc độ         : {speed_seconds} giây / 1 nến phút")
    print(f"   - Tổng số nến    : {total_bars} mốc thời gian")
    print(f"   - Lặp vô tận     : {'BẬT' if loop else 'TẮT'}")
    print("=================================================================")

    iteration = 0
    news_idx = 0
    while True:
        iteration += 1
        bar_count = 0

        for bar_time, group in grouped:
            bar_count += 1
            current_time_str = datetime.now(TZ_VN).isoformat()

            for _, row in group.iterrows():
                symbol = str(row['symbol'])
                payload = {
                    "symbol": symbol,
                    "timestamp": bar_time,
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": int(row['volume']),
                    "source": "replay_historical",
                    "ingested_at": current_time_str
                }
                if producer:
                    producer.send(STOCK_TOPIC, key=symbol, value=payload)

            news_sent_cnt = 0
            if include_news and news_items:
                item = news_items[news_idx % len(news_items)]
                news_idx += 1
                news_payload = {
                    "article_id": str(item.get("article_id", f"replay_{news_idx}")),
                    "title": str(item.get("title", "")),
                    "url": str(item.get("url", "")),
                    "content": str(item.get("content", "")),
                    "published_at": str(item.get("published_at", current_time_str)),
                    "crawled_at": current_time_str,
                    "source": "cafef_replay"
                }
                if producer:
                    producer.send(NEWS_TOPIC, key=news_payload["article_id"], value=news_payload)
                news_sent_cnt = 1

            if producer:
                producer.flush()

            news_note = f" + {news_sent_cnt} news" if include_news else ""
            print(f"[{bar_count}/{total_bars}][REPLAY][{mode_str}] Đã phát {len(group)} mã{news_note} tại mốc: {bar_time}")
            time.sleep(speed_seconds)

        if not loop:
            print(f"\n[Hoàn tất] Đã phát lại toàn bộ {bar_count} mốc thời gian dữ liệu lịch sử thành công!\n")
            break
        else:
            print(f"\n[Vòng lặp {iteration}] Kết thúc mốc dữ liệu. Tự động lặp lại từ đầu...\n")

def shutdown_handler(signum, frame):
    print("\n[Hệ thống] Nhận tín hiệu dừng Replay. Đang giải phóng kết nối...")
    if producer:
        try:
            producer.flush()
            producer.close()
            print("[Kafka Replay] Đã đóng Kafka Producer an toàn.")
        except Exception:
            pass
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    parser = argparse.ArgumentParser(description="Script phát lại dữ liệu thực tế phục vụ kiểm thử Kafka -> Bronze & Stream")
    parser.add_argument("--speed", type=float, default=2.0, help="Số giây giữa mỗi nến phút (mặc định 2.0 giây)")
    parser.add_argument("--loop", action="store_true", help="Lặp lại dữ liệu liên tục sau khi phát hết")
    parser.add_argument("--bars", type=int, default=None, help="Số lượng mốc nến mới nhất muốn phát (mặc định tất cả)")
    parser.add_argument("--include-news", action="store_true", help="Phát xen kẽ tin tức CafeF vào news-topic")
    args = parser.parse_args()

    replay_market_data(
        speed_seconds=args.speed,
        loop=args.loop,
        max_bars=args.bars,
        include_news=args.include_news,
    )

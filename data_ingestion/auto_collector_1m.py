import os
import sys
import json
import time
import socket
import signal
import hashlib
import requests
import pandas as pd
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import re

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import KAFKA_BROKERS, STOCK_TOPIC, NEWS_TOPIC, SYMBOLS, CAFEF_URL, HEADERS, TZ_VN

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.environ.get("COLLECTOR_STATE_DIR", SCRIPT_DIR)
os.makedirs(STATE_DIR, exist_ok=True)
CACHE_NEWS_FILE = os.path.join(STATE_DIR, ".seen_news_ids.json")
CACHE_STOCK_FILE = os.path.join(STATE_DIR, ".last_stock_timestamps.json")

# ==============================================================================
# 1. QUẢN LÝ CACHE TIN TỨC (DÙNG DEQUE MAXLEN=1000 & SET ĐỂ BẢO TOÀN THỨ TỰ MỚI NHẤT)
# ==============================================================================
seen_article_ids_deque = deque(maxlen=1000)
seen_article_ids_set = set()

def load_seen_article_ids():
    """Tải danh sách các article_id đã từng gửi từ file cache để tránh trùng khi restart."""
    if os.path.exists(CACHE_NEWS_FILE):
        try:
            with open(CACHE_NEWS_FILE, "r", encoding="utf-8") as f:
                ids = json.load(f)
                for item_id in ids:
                    s_id = str(item_id)
                    seen_article_ids_deque.append(s_id)
                    seen_article_ids_set.add(s_id)
        except Exception:
            pass

def record_seen_article_id(article_id: str):
    """Ghi nhận article_id mới, giữ chuẩn xác tối đa 1.000 bài mới nhất theo thứ tự thời gian."""
    if article_id not in seen_article_ids_set:
        if len(seen_article_ids_deque) == seen_article_ids_deque.maxlen:
            oldest_id = seen_article_ids_deque.popleft()
            seen_article_ids_set.discard(oldest_id)
        seen_article_ids_deque.append(article_id)
        seen_article_ids_set.add(article_id)

def save_seen_article_ids():
    """Lưu 1.000 article_id mới nhất ra file cache JSON theo đúng thứ tự nạp vào."""
    try:
        with open(CACHE_NEWS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(seen_article_ids_deque), f, ensure_ascii=False)
    except Exception:
        pass

load_seen_article_ids()

# ==============================================================================
# 2. QUẢN LÝ CACHE TIMESTAMP CỔ PHIẾU (CHỐNG GỬI TRÙNG NẾN NGOÀI GIỜ GIAO DỊCH)
# ==============================================================================
last_stock_timestamps = {}

def load_last_stock_timestamps():
    """Tải mốc thời gian nến cuối cùng đã gửi của từng mã để chống gửi lặp khi khởi động lại."""
    global last_stock_timestamps
    if os.path.exists(CACHE_STOCK_FILE):
        try:
            with open(CACHE_STOCK_FILE, "r", encoding="utf-8") as f:
                last_stock_timestamps = json.load(f)
        except Exception:
            last_stock_timestamps = {}

def save_last_stock_timestamps():
    """Lưu mốc thời gian nến cuối cùng của các mã ra đĩa."""
    try:
        with open(CACHE_STOCK_FILE, "w", encoding="utf-8") as f:
            json.dump(last_stock_timestamps, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

load_last_stock_timestamps()

# ==============================================================================
# ==============================================================================
# 3. HTTP SESSION & KAFKA PRODUCER
# ==============================================================================
from requests.adapters import HTTPAdapter

http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
http_session.mount("https://", _adapter)
http_session.mount("http://", _adapter)


_last_kafka_probe_ts = 0.0
_KAFKA_RETRY_COOLDOWN = 15.0


def _is_kafka_reachable() -> bool:
    """Kiểm tra nhanh TCP port Kafka trong <=1s để tránh KafkaProducer block ~60s khi Broker chưa mở."""
    for broker in KAFKA_BROKERS:
        try:
            host, port_str = broker.split(":", 1)
            with socket.create_connection((host, int(port_str)), timeout=1.0):
                return True
        except Exception:
            continue
    return False


def get_kafka_producer(quiet: bool = False):
    """Khởi tạo KafkaProducer an toàn, có thông báo nếu Broker chưa sẵn sàng."""
    global _last_kafka_probe_ts
    _last_kafka_probe_ts = time.time()

    if not _is_kafka_reachable():
        if not quiet:
            print(f"[Cảnh báo Kafka] Broker {KAFKA_BROKERS} chưa phản hồi TCP. Sẽ chạy ở chế độ DRY-RUN.")
        return None

    try:
        prod = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else str(k).encode('utf-8'),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1,
            retries=2,
            request_timeout_ms=3000,
            max_block_ms=3000,
        )
        print(f"[Kafka] Kết nối thành công tới Kafka Brokers: {KAFKA_BROKERS}")
        return prod
    except KafkaError as e:
        if not quiet:
            print(f"[Cảnh báo Kafka] Không thể kết nối tới {KAFKA_BROKERS} ({e}). Sẽ chạy ở chế độ DRY-RUN.")
        return None
    except Exception as e:
        if not quiet:
            print(f"[Lỗi Kafka] {e}. Sẽ chạy ở chế độ DRY-RUN.")
        return None


producer = get_kafka_producer()


def ensure_kafka_producer():
    """Thử kết nối lại Kafka nếu trước đó đang chạy ở chế độ DRY-RUN (có cooldown tránh block)."""
    global producer
    if producer is None and (time.time() - _last_kafka_probe_ts) >= _KAFKA_RETRY_COOLDOWN:
        producer = get_kafka_producer(quiet=True)
    return producer


def _reset_kafka_producer():
    """Đóng và đặt lại producer về None khi gửi Kafka thất bại để tự kết nối lại ở chu kỳ sau."""
    global producer
    if producer is not None:
        try:
            producer.close(timeout=1)
        except Exception:
            pass
        producer = None


# ==============================================================================
# 4. THU THẬP NẾN CỔ PHIẾU 1 PHÚT (REALTIME INGESTION)
# ==============================================================================
def fetch_recent_bars(symbol: str, max_bars: int = 10) -> list[dict]:
    """
    Quét tối đa `max_bars` nến 1m thực tế mới nhất của mã cổ phiếu (sắp xếp tăng dần theo thời gian):
    - Nguồn chính : TradingView UDF VNDirect (dchart-api.vndirect.com.vn, giá sẵn đơn vị nghìn đồng)
    - Nguồn dự phòng: TradingView UDF Mirae Asset (masboard.masvn.com, giá VND -> chia 1000)
    """
    now = datetime.now(TZ_VN)
    now_ts = int(now.timestamp())
    lookback_ts = now_ts - 86400 * 5  # Đủ qua cuối tuần/ngày lễ để luôn có nến cuối gần nhất
    errors = []

    # 1. Nguồn chính: VNDirect dchart (ổn định 30/30 mã, timestamp chuẩn :00, giá nghìn đồng)
    try:
        url_vnd = "https://dchart-api.vndirect.com.vn/dchart/history"
        r = http_session.get(
            url_vnd,
            params={"symbol": symbol, "resolution": "1", "from": lookback_ts, "to": now_ts},
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and data.get("t"):
                start_idx = max(0, len(data["t"]) - max_bars)
                bars = []
                for idx in range(start_idx, len(data["t"])):
                    dt = datetime.fromtimestamp(int(data["t"][idx]), tz=TZ_VN)
                    bars.append({
                        "symbol": symbol,
                        "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S+07:00"),
                        "open": round(float(data["o"][idx]), 2),
                        "high": round(float(data["h"][idx]), 2),
                        "low": round(float(data["l"][idx]), 2),
                        "close": round(float(data["c"][idx]), 2),
                        "volume": int(data["v"][idx]),
                        "source": "vnd_tradingview",
                        "ingested_at": now.isoformat(),
                    })
                return bars
            errors.append(f"VND status={data.get('s')}")
        else:
            errors.append(f"VND HTTP {r.status_code}")
    except Exception as e:
        errors.append(f"VND {type(e).__name__}")

    # 2. Nguồn dự phòng: Mirae Asset (chặn redirect lỗi sang mapsboard.masvn.com)
    try:
        url_mas = "https://masboard.masvn.com/api/v1/tradingview/history"
        r = http_session.get(
            url_mas,
            params={"symbol": symbol, "resolution": "1", "from": lookback_ts, "to": now_ts},
            headers={"User-Agent": HEADERS["User-Agent"], "Referer": "https://masboard.masvn.com/"},
            allow_redirects=False,
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and data.get("t"):
                start_idx = max(0, len(data["t"]) - max_bars)
                bars = []
                for idx in range(start_idx, len(data["t"])):
                    dt = datetime.fromtimestamp(int(round(float(data["t"][idx]))), tz=TZ_VN)
                    bars.append({
                        "symbol": symbol,
                        "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S+07:00"),
                        "open": round(float(data["o"][idx]) / 1000.0, 2),
                        "high": round(float(data["h"][idx]) / 1000.0, 2),
                        "low": round(float(data["l"][idx]) / 1000.0, 2),
                        "close": round(float(data["c"][idx]) / 1000.0, 2),
                        "volume": int(data["v"][idx]),
                        "source": "mas_tradingview",
                        "ingested_at": now.isoformat(),
                    })
                return bars
            errors.append(f"MAS status={data.get('s')}")
        else:
            errors.append(f"MAS HTTP {r.status_code}")
    except Exception as e:
        errors.append(f"MAS {type(e).__name__}")

    print(f"[Stock Warning] Không lấy được dữ liệu mã {symbol}: {'; '.join(errors)}")
    return []


dry_run_stock_ts = {}
dry_run_news_ids = set()


def job_collect_stock_quotes():
    """Tác vụ 1: Quét song song 30 mã VN30 vào giây thứ 10 mỗi phút (chờ nến phút đóng hẳn)."""
    start_time = time.time()
    prod = ensure_kafka_producer()
    bars_by_symbol = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(fetch_recent_bars, sym): sym for sym in SYMBOLS}
        for future in as_completed(future_to_symbol):
            sym = future_to_symbol[future]
            try:
                bars = future.result()
                if bars:
                    bars_by_symbol[sym] = bars
            except Exception as e:
                print(f"[Stock Error] Lỗi quét mã {sym}: {e}")

    # Lọc các nến mới (nếu API trễ vài giây ở chu kỳ trước, tự động lấy bù mọi nến chưa gửi)
    new_candles = []
    sources_used = set()
    for sym, bars in bars_by_symbol.items():
        sources_used.add(bars[-1]["source"])
        seen_ts = last_stock_timestamps.get(sym) if prod else dry_run_stock_ts.get(sym, last_stock_timestamps.get(sym))
        if not seen_ts:
            # Lần đầu chạy: chỉ đẩy cây nến mới nhất
            new_candles.append(bars[-1])
        else:
            for bar in bars:
                if bar["timestamp"] > seen_ts:
                    new_candles.append(bar)

    # Đẩy các nến mới vào Kafka topic stock-price-topic
    sent_ok = False
    if prod and new_candles:
        try:
            for payload in new_candles:
                prod.send(STOCK_TOPIC, key=payload['symbol'], value=payload)
            prod.flush()
            sent_ok = True
        except Exception as e:
            print(f"[Kafka Error] Lỗi gửi stock lên Kafka: {e}")
            _reset_kafka_producer()

    # Cập nhật & lưu cache bền vững CHỈ KHI đã đẩy Kafka thành công
    if sent_ok:
        for payload in new_candles:
            sym = payload['symbol']
            if payload['timestamp'] > last_stock_timestamps.get(sym, ""):
                last_stock_timestamps[sym] = payload['timestamp']
        save_last_stock_timestamps()
    elif not prod and new_candles:
        for payload in new_candles:
            sym = payload['symbol']
            if payload['timestamp'] > dry_run_stock_ts.get(sym, ""):
                dry_run_stock_ts[sym] = payload['timestamp']

    duration = time.time() - start_time
    now_str = datetime.now(TZ_VN).strftime("%H:%M:%S")
    mode_str = "KAFKA" if sent_ok or (prod and not new_candles) else "DRY-RUN"
    src_note = f" | Nguồn: {','.join(sorted(sources_used))}" if sources_used else ""
    if new_candles:
        print(f"[{now_str}][STOCK COLLECTOR][{mode_str}] Đã đẩy {len(new_candles)} nến mới ({len(bars_by_symbol)}/{len(SYMBOLS)} mã) vào '{STOCK_TOPIC}' (Mất {duration:.2f}s{src_note})")
    else:
        print(f"[{now_str}][STOCK COLLECTOR][{mode_str}] Ngoài giờ giao dịch hoặc chưa có nến mới ({len(bars_by_symbol)}/{len(SYMBOLS)} mã giữ nguyên mốc nến cũ, mất {duration:.2f}s{src_note}). Bỏ qua gửi trùng.")


# ==============================================================================
# 5. THU THẬP TIN TỨC CAFEF (REALTIME INGESTION)
# ==============================================================================
def job_collect_news():
    """Tác vụ 2: Quét tin tức tài chính CafeF vào giây thứ 15 mỗi phút."""
    start_time = time.time()
    prod = ensure_kafka_producer()
    new_articles = []
    now = datetime.now(TZ_VN)
    batch_ids = set()

    try:
        res = http_session.get(CAFEF_URL, headers=HEADERS, timeout=8)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, "html.parser")
            boxes = soup.select(".box-category-item") or soup.select(".tlitem")

            for b in boxes:
                data_id = b.get("data-id")
                a_tag = b.find("h3").find("a") if b.find("h3") else b.find("a")
                if not a_tag:
                    continue

                href = a_tag.get("href", "")
                full_url = f"https://cafef.vn{href}" if href.startswith("/") else href

                # Bóc tách article_id cố định; dùng MD5 xác định thay vì hàm hash() ngẫu nhiên
                if not data_id:
                    match = re.search(r'-(\d+)\.chn', href)
                    data_id = match.group(1) if match else hashlib.md5(full_url.encode('utf-8')).hexdigest()[:12]

                article_id = str(data_id)
                if article_id in seen_article_ids_set or article_id in batch_ids or (not prod and article_id in dry_run_news_ids):
                    continue
                batch_ids.add(article_id)

                title = a_tag.get_text(strip=True)
                sapo_elem = b.find(class_=re.compile(r'sapo|summary', re.I))
                content = sapo_elem.get_text(strip=True) if sapo_elem else title
                time_elem = b.find(class_=re.compile(r'time|date', re.I))
                pub_at = time_elem.get("title") or time_elem.get_text(strip=True) if time_elem else now.isoformat()

                payload = {
                    "article_id": article_id,
                    "title": title,
                    "url": full_url,
                    "content": content,
                    "published_at": str(pub_at),
                    "crawled_at": now.isoformat(),
                    "source": "cafef"
                }
                new_articles.append(payload)
        else:
            print(f"[News Warning] CafeF trả về HTTP {res.status_code}")

    except Exception as e:
        print(f"[News Error] Lỗi khi cào tin CafeF: {e}")

    # Đẩy tin tức mới vào Kafka topic news-topic
    sent_ok = False
    if prod and new_articles:
        try:
            for payload in new_articles:
                prod.send(NEWS_TOPIC, key=payload['article_id'], value=payload)
            prod.flush()
            sent_ok = True
        except Exception as e:
            print(f"[Kafka Error] Lỗi gửi tin tức lên Kafka: {e}")
            _reset_kafka_producer()

    # Chỉ ghi nhận cache bền vững ra đĩa khi đã đẩy Kafka thành công
    if sent_ok:
        for payload in new_articles:
            record_seen_article_id(payload['article_id'])
        save_seen_article_ids()
    elif not prod and new_articles:
        for payload in new_articles:
            dry_run_news_ids.add(payload['article_id'])

    duration = time.time() - start_time
    now_str = datetime.now(TZ_VN).strftime("%H:%M:%S")
    mode_str = "KAFKA" if sent_ok or (prod and not new_articles) else "DRY-RUN"
    if new_articles:
        print(f"[{now_str}][NEWS COLLECTOR][{mode_str}] Phát hiện và đẩy {len(new_articles)} bài báo mới vào '{NEWS_TOPIC}' (Mất {duration:.2f}s | Nguồn: cafef)")
    else:
        print(f"[{now_str}][NEWS COLLECTOR][{mode_str}] Không có bài báo mới phát sinh trên CafeF (Mất {duration:.2f}s).")


# ==============================================================================
# 6. QUẢN LÝ DỪNG CHƯƠNG TRÌNH
# ==============================================================================
def shutdown_handler(signum, frame):
    print("\n[Hệ thống] Nhận tín hiệu dừng chương trình. Đang đóng kết nối Kafka...")
    if producer:
        try:
            producer.flush()
            producer.close()
            print("[Kafka] Đã đóng Kafka Producer an toàn.")
        except Exception:
            pass
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    print("=================================================================")
    print("   KHỞI ĐỘNG HỆ THỐNG THU THẬP TỰ ĐỘNG (DATA INGESTION LAYER)    ")
    print(f"   - Kafka Brokers : {KAFKA_BROKERS}")
    print(f"   - Stock Topic   : {STOCK_TOPIC} (giây 10)")
    print(f"   - News Topic    : {NEWS_TOPIC} (giây 20)")
    print(f"   - Số lượng mã   : {len(SYMBOLS)} mã VN30")
    print(f"   - Chống trùng lặp: Đã bật cho cả Cổ phiếu & CafeF")
    print("=================================================================")

    # Chạy thử nghiệm ngay 1 vòng đầu tiên để kiểm tra pipeline
    print("\n[Kiểm tra tức thì] Thực thi 1 vòng thu thập khởi động...")
    job_collect_stock_quotes()
    job_collect_news()
    print("[Kiểm tra tức thì] Khởi động thành công! Bắt đầu lập lịch APScheduler...\n")

    scheduler = BlockingScheduler(timezone=TZ_VN)
    # Lập lịch: Giây thứ 10 quét giá cổ phiếu (chờ nến phút đóng hoàn chỉnh trên sàn)
    scheduler.add_job(job_collect_stock_quotes, 'cron', second='10', id='stock_job', max_instances=2)
    # Lập lịch: Giây thứ 20 quét tin tức CafeF
    scheduler.add_job(job_collect_news, 'cron', second='20', id='news_job', max_instances=2)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        shutdown_handler(None, None)

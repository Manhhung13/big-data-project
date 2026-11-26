import os
from datetime import timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
BRONZE_STOCK_DIR = os.path.join(BRONZE_DIR, "stock")
BRONZE_NEWS_DIR = os.path.join(BRONZE_DIR, "news")
REFERENCE_SYMBOLS_PATH = os.path.join(BASE_DIR, "reference_symbols.csv")
BRONZE_FLUSH_INTERVAL_SECONDS = int(os.environ.get("BRONZE_FLUSH_INTERVAL_SECONDS", "3600"))

_env_brokers = os.environ.get("KAFKA_BROKERS", "").strip()
KAFKA_BROKERS = (
    [b.strip() for b in _env_brokers.split(",") if b.strip()]
    if _env_brokers
    else ['localhost:9092']
)
KAFKA_INTERNAL_BROKERS = ['kafka:29092']
STOCK_TOPIC = 'stock-price-topic'
NEWS_TOPIC = 'news-topic'

# 2 Consumer Groups độc lập sau Kafka (Fan-out Architecture)
BRONZE_CONSUMER_GROUP = 'bronze-store-group'
STREAM_CONSUMER_GROUP = 'stream-processing-group'

TZ_VN = timezone(timedelta(hours=7))

# Danh sách 30 mã cổ phiếu thanh khoản cao (VN30) theo đúng phạm vi báo cáo (30-100 mã)
SYMBOLS = [
    'FPT', 'VNM', 'VIC', 'VHM', 'HPG', 'VCB', 'BID', 'CTG', 'TCB', 'MBB',
    'VPB', 'ACB', 'SSI', 'VND', 'MSN', 'MWG', 'GAS', 'PLX', 'POW', 'SAB',
    'STB', 'HDB', 'VIB', 'SHB', 'TPB', 'LPB', 'GVR', 'BCM', 'VRE', 'VJC'
]

CAFEF_URL = "https://cafef.vn/thi-truong-chung-khoan.chn"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}


"""Cấu hình trung tâm cho phần Spark Structured Streaming.

Mọi ngưỡng / kích thước window / đường dẫn đều nằm ở đây để hiệu chỉnh mà không phải sửa logic
(đề xuất mục 8.1: ngưỡng chỉ là giả định khởi tạo, sẽ hiệu chỉnh theo dữ liệu thực).

Hai nguồn dữ liệu, chọn bằng biến môi trường BIGDATA_SOURCE:
  real (mặc định): dữ liệu thật VN30 + CafeF do nhánh crawl-data (Thắng) thu thập, đọc từ repo nhóm
  sim            : dữ liệu giả lập 5 mã có kịch bản cấy sẵn (dùng cho test e2e có đáp án biết trước)
"""
import csv
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = os.environ.get("BIGDATA_SOURCE", "real").lower()
assert SOURCE in ("real", "sim"), f"BIGDATA_SOURCE phải là real hoặc sim, nhận '{SOURCE}'"

# Thư mục làm việc của pipeline (baseline, input stream, checkpoint, serving) - tách riêng theo nguồn.
DATA = Path(os.environ.get("BIGDATA_DATA", ROOT / "data" / SOURCE))

# Repo nhóm (https://github.com/Manhhung13/big-data-project) - chứa dữ liệu Bronze thật.
# Code nằm trong repo nhóm (thư mục stream_processing/) thì đọc thẳng ../data/bronze;
# nằm ngoài thì tìm repo nhóm clone cạnh thư mục code. Đặt BIGDATA_TEAM_REPO để chỉ định khác.
_DEFAULT_TEAM_REPO = ROOT.parent if (ROOT.parent / "data" / "bronze").exists() else ROOT.parent / "big-data-project"
TEAM_REPO = Path(os.environ.get("BIGDATA_TEAM_REPO", _DEFAULT_TEAM_REPO))
BRONZE = TEAM_REPO / "data" / "bronze"

PATHS = {
    # --- dữ liệu thật (Bronze của nhánh crawl-data) ---
    "bronze_stock": BRONZE / "stock",                       # {SYMBOL}_1m.parquet, nến 1 phút
    "bronze_news": BRONZE / "news" / "historical_news.parquet",
    "reference_symbols": TEAM_REPO / "data_ingestion" / "reference_symbols.csv",
    # --- dữ liệu giả lập ---
    "history_stock": DATA / "history" / "stock.csv",
    "history_news": DATA / "history" / "news.jsonl",
    # --- pipeline ---
    "baseline": DATA / "baseline",                          # Parquet - đầu ra của batch dựng baseline
    "stream_in_stock": DATA / "stream_in" / "stock",        # thay cho Kafka stock-price-topic
    "stream_in_news": DATA / "stream_in" / "news",          # thay cho Kafka news-topic
    "checkpoints": DATA / "checkpoints",
    "serving_db": DATA / "serving" / "serving.db",          # thay cho Elasticsearch
    "live_truth": DATA / "live" / "ground_truth.json",
    "live_bars_clean": DATA / "live" / "bars_clean.csv",
}

TZ = "Asia/Ho_Chi_Minh"
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"   # định dạng timestamp khi ghi ra serving

# --- Reference data: mã cổ phiếu + alias để map bài viết -> mã (mục 7.2) ---
# Mỗi mã: "ticker" match phân biệt hoa/thường, đúng word-boundary; "names" match không phân biệt hoa/thường.
# "ticker_context": với mã trùng từ thông dụng, ticker chỉ được tính khi đi kèm ngữ cảnh này.
SIM_SYMBOLS = {
    "FPT": {"names": ["Tập đoàn FPT", "FPT Corp"]},
    "VCB": {"names": ["Vietcombank", "Ngân hàng Ngoại thương"]},
    "HPG": {"names": ["Hòa Phát", "Tập đoàn Hòa Phát"]},
    "VNM": {"names": ["Vinamilk", "Sữa Việt Nam"]},
    "MWG": {"names": ["Thế Giới Di Động", "Mobile World"]},
}

# Sửa alias của reference_symbols.csv sau khi đo trên 9.318 bài CafeF thật:
ALIAS_EXCLUDE = {
    "HPG": ["Dung Quất"],   # phần lớn bài "Dung Quất" nói về lọc dầu BSR / cảng biển, không phải Hòa Phát
}
TICKER_CONTEXT = {
    # "VND" còn là ký hiệu tiền đồng ("bơm thêm VND") -> chỉ nhận "cổ phiếu VND", "mã VND", "(VND)"
    "VND": r"(?:[Cc]ổ phiếu|[Mm]ã|CP)\s+VND\b|\(VND\)",
}


def _load_reference_symbols(path) -> dict:
    out = {}
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sym = row["symbol"].strip()
            names = [a.strip() for a in row["aliases"].split("|")
                     if a.strip() and a.strip() != sym and a.strip() not in ALIAS_EXCLUDE.get(sym, [])]
            entry = {"names": names}
            if sym in TICKER_CONTEXT:
                entry["ticker_context"] = TICKER_CONTEXT[sym]
            out[sym] = entry
    return out


SYMBOLS = SIM_SYMBOLS if SOURCE == "sim" else _load_reference_symbols(PATHS["reference_symbols"])

# --- Dữ liệu thật: ngày phát lại làm "realtime" và cửa sổ lịch sử dựng baseline ---
REPLAY_DATE = os.environ.get("BIGDATA_REPLAY_DATE", "2026-11-25")   # ngày giao dịch đầy đủ gần nhất
BASELINE_DAYS = int(os.environ.get("BIGDATA_BASELINE_DAYS", "250"))  # ~1 năm giao dịch trước REPLAY_DATE
# Hai nguồn nến trùng nhau 26/10-10/11/2026: ưu tiên VNDirect (timestamp tròn phút, nguồn chính).
SOURCE_PRIORITY = ["vnd_tradingview", "mas_tradingview"]

# --- Stock stream ---
STOCK_WINDOW = "5 minutes"
STOCK_WATERMARK = "2 minutes"      # chấp nhận bar đến trễ tối đa ~2 phút
BARS_PER_WINDOW = 5                # chỉ dùng cho dữ liệu giả lập (dữ liệu thật có phút không khớp lệnh -> không có nến)

# --- News stream ---
NEWS_WATERMARK = "10 minutes"      # bài đến trễ (crawl lag) tối đa ~10 phút
NEWS_SLIDE = "5 minutes"           # mọi window news trượt theo lưới 5 phút
NEWS_WINDOWS = {                   # tên -> độ dài window
    "5m": "5 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
}

# --- Phát hiện bất thường (mục 8.1) ---
Z_THRESHOLD = 3.0
MIN_BASELINE_SAMPLES = 30          # baseline < 30 mẫu thì không đủ tin cậy -> không gắn flag
STD_EPS = 1e-12                    # std ~ 0 thì không tính z-score
# Tin thật rất thưa theo từng mã (cả năm chỉ vài lần một mã có >= 2 bài trong 30') nên:
#  - ngưỡng tối thiểu thấp hơn dữ liệu giả lập,
#  - baseline tin gộp mọi khung giờ của mã (theo từng khung giờ thì gần như toàn số 0, std = 0).
NEWS_MIN_COUNT_30M = 4 if SOURCE == "sim" else 2
NEWS_BASELINE_BY_BUCKET = SOURCE == "sim"

# --- Streaming runtime ---
BASELINE_TTL_SECONDS = 600         # tải lại baseline mỗi 10 phút (Batch Layer cập nhật định kỳ)
TRIGGER_SECONDS = 2
# Số file đọc mỗi micro-batch. 1 = sát thực tế nhất (mỗi file = 5 phút dữ liệu); tăng lên để chạy test nhanh hơn.
MAX_FILES_PER_TRIGGER = int(os.environ.get("BIGDATA_MAX_FILES", "1"))

# Baseline tin tức chỉ tính cho các slot trong khung giờ này (slot ngoài khung -> không detect).
# CafeF đăng ~18% bài lúc 00:00-01:00 (đăng hẹn giờ) -> nằm ngoài khung, không tạo event lúc nửa đêm.
NEWS_BASELINE_HOURS = ("07:00", "20:00")

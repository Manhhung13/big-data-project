import os
import sys
import time
import glob
import argparse
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from config import SYMBOLS, BRONZE_STOCK_DIR, HEADERS, TZ_VN
from bronze_storage import merge_stock_records

os.makedirs(BRONZE_STOCK_DIR, exist_ok=True)

http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
http_session.mount("https://", _adapter)
http_session.mount("http://", _adapter)


def fetch_chunk_tradingview(symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """
    Truy xuất nến 1m thực tế từ API TradingView backend của CTCK:
    - Nguồn chính : VNDirect dchart (giá sẵn đơn vị nghìn đồng)
    - Nguồn dự phòng: Mirae Asset (giá VND -> chia 1000, chặn redirect lỗi)
    """
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    # 1. Nguồn chính: VNDirect dchart
    url_vnd = "https://dchart-api.vndirect.com.vn/dchart/history"
    try:
        r = http_session.get(
            url_vnd,
            params={"symbol": symbol, "resolution": "1", "from": start_ts, "to": end_ts},
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and data.get("t"):
                timestamps = [
                    datetime.fromtimestamp(int(ts), tz=TZ_VN).strftime("%Y-%m-%d %H:%M:%S+07:00")
                    for ts in data["t"]
                ]
                return pd.DataFrame({
                    "timestamp": timestamps,
                    "open": [round(float(x), 2) for x in data["o"]],
                    "high": [round(float(x), 2) for x in data["h"]],
                    "low": [round(float(x), 2) for x in data["l"]],
                    "close": [round(float(x), 2) for x in data["c"]],
                    "volume": [int(x) for x in data["v"]],
                    "symbol": symbol,
                    "source": "vnd_tradingview",
                })
    except Exception:
        pass

    # 2. Nguồn dự phòng: Mirae Asset
    url_mas = "https://masboard.masvn.com/api/v1/tradingview/history"
    try:
        r = http_session.get(
            url_mas,
            params={"symbol": symbol, "resolution": "1", "from": start_ts, "to": end_ts},
            headers={"User-Agent": HEADERS["User-Agent"], "Referer": "https://masboard.masvn.com/"},
            allow_redirects=False,
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and data.get("t"):
                timestamps = [
                    datetime.fromtimestamp(int(round(float(ts))), tz=TZ_VN).strftime("%Y-%m-%d %H:%M:%S+07:00")
                    for ts in data["t"]
                ]
                return pd.DataFrame({
                    "timestamp": timestamps,
                    "open": [round(float(x) / 1000.0, 2) for x in data["o"]],
                    "high": [round(float(x) / 1000.0, 2) for x in data["h"]],
                    "low": [round(float(x) / 1000.0, 2) for x in data["l"]],
                    "close": [round(float(x) / 1000.0, 2) for x in data["c"]],
                    "volume": [int(x) for x in data["v"]],
                    "symbol": symbol,
                    "source": "mas_tradingview",
                })
    except Exception:
        pass

    return pd.DataFrame()


def crawl_symbol_1m(symbol: str, days: int = 1095, chunk_days: int = 30, skip_if_complete: bool = False) -> tuple[int, int]:
    """
    Thu thập nến 1m trong `days` ngày gần nhất cho 1 mã cổ phiếu và gộp (idempotent)
    vào phân vùng theo tháng `year=YYYY/month=MM/{SYMBOL}_1m.parquet`.
    """
    existing_files = sorted(glob.glob(os.path.join(BRONZE_STOCK_DIR, "year=*", "month=*", f"{symbol}_1m.parquet")))
    if skip_if_complete and existing_files:
        try:
            existing_rows = sum(len(pd.read_parquet(f, columns=["timestamp"])) for f in existing_files)
            if existing_rows >= 100000:
                print(f"  [Đã tồn tại] Mã {symbol} đã có sẵn {existing_rows:,} nến. Bỏ qua.", flush=True)
                return existing_rows, 0
        except Exception:
            pass

    end_dt = datetime.now(TZ_VN)
    start_dt = end_dt - timedelta(days=days)

    all_chunks = []
    curr_start = start_dt
    t0 = time.time()

    while curr_start < end_dt:
        curr_end = min(curr_start + timedelta(days=chunk_days), end_dt)
        df = fetch_chunk_tradingview(symbol, curr_start, curr_end)
        if df is not None and not df.empty:
            all_chunks.append(df)
        curr_start = curr_end + timedelta(seconds=1)
        time.sleep(0.1)

    if not all_chunks:
        print(f"  [Thất bại] Không lấy được dữ liệu nến 1m cho {symbol}.", flush=True)
        return 0, 0

    new_df = pd.concat(all_chunks, ignore_index=True)
    new_df["timestamp"] = new_df["timestamp"].astype(str)
    new_df = new_df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)

    touched_rows, added = merge_stock_records(new_df, stock_dir=BRONZE_STOCK_DIR)
    elapsed = time.time() - t0
    first_ts = new_df["timestamp"].iloc[0][:10]
    last_ts = new_df["timestamp"].iloc[-1][:16]
    print(
        f"  -> {symbol}: Ghi phân vùng tháng ({touched_rows:,} dòng vùng chạm, +{added:,} mới) "
        f"trong {elapsed:.1f}s | {first_ts} -> {last_ts}",
        flush=True,
    )
    return touched_rows, added


def main():
    parser = argparse.ArgumentParser(description="Backfill nến 1m cho toàn bộ danh mục VN30 vào Bronze Store")
    parser.add_argument("--years", type=int, default=3, help="Số năm lùi về quá khứ nếu không truyền --days (mặc định 3 năm)")
    parser.add_argument("--days", type=int, default=None, help="Số ngày gần đây muốn backfill (ví dụ: 30 cho 1 tháng)")
    parser.add_argument("--chunk", type=int, default=15, help="Số ngày mỗi chunk request (mặc định 15 ngày)")
    parser.add_argument("--skip-existing", action="store_true", help="Bỏ qua mã đã có >= 100.000 nến")
    args = parser.parse_args()

    lookback_days = args.days if args.days is not None else args.years * 365
    symbols = SYMBOLS
    total = len(symbols)
    print("=================================================================")
    print(f" BACKFILL NẾN 1 PHÚT ({lookback_days} NGÀY GẦN NHẤT) CHO {total} MÃ VN30")
    print(f" - Danh mục mã   : {', '.join(symbols)}")
    print(f" - Thư mục lưu trữ: {BRONZE_STOCK_DIR}")
    print("=================================================================\n")

    t_start = time.time()
    success_count = 0
    total_candles = 0
    total_added = 0

    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx:02d}/{total}] Đang xử lý mã {sym}...", flush=True)
        count, added = crawl_symbol_1m(sym, days=lookback_days, chunk_days=args.chunk, skip_if_complete=args.skip_existing)
        if count > 0:
            success_count += 1
            total_candles += count
            total_added += added

    total_time = time.time() - t_start
    print("\n=================================================================")
    print(" HOÀN TẤT BACKFILL NẾN 1 PHÚT CHO TOÀN BỘ VN30!")
    print(f" - Số mã thành công: {success_count}/{total} mã")
    print(f" - Tổng nến mới bù : +{total_added:,} nến")
    print(f" - Tổng nến trong kho: {total_candles:,} nến")
    print(f" - Tổng thời gian  : {total_time:.1f} giây")
    print(f" - Vị trí lưu trữ  : {BRONZE_STOCK_DIR}")
    print("=================================================================\n")


if __name__ == "__main__":
    main()

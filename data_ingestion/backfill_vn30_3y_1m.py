import os
import sys
import time
import uuid
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

os.makedirs(BRONZE_STOCK_DIR, exist_ok=True)

http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
http_session.mount("https://", _adapter)
http_session.mount("http://", _adapter)


def _atomic_write_parquet(df: pd.DataFrame, final_path: str):
    tmp_path = f"{final_path}.{uuid.uuid4().hex[:8]}.tmp"
    df.to_parquet(tmp_path, index=False, compression="snappy")
    os.replace(tmp_path, final_path)


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
    Thu thập nến 1m trong `days` ngày gần nhất cho 1 mã cổ phiếu và gộp (idempotent) vào {SYMBOL}_1m.parquet.
    Trả về (tổng số dòng trong file, số dòng mới được thêm).
    """
    output_file = os.path.join(BRONZE_STOCK_DIR, f"{symbol}_1m.parquet")

    if skip_if_complete and os.path.exists(output_file):
        try:
            existing_df = pd.read_parquet(output_file, columns=["timestamp"])
            if len(existing_df) >= 100000:
                print(f"  [Đã tồn tại] Mã {symbol} đã có sẵn {len(existing_df):,} nến. Bỏ qua.", flush=True)
                return len(existing_df), 0
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

    if os.path.exists(output_file):
        old_df = pd.read_parquet(output_file)
        old_len = len(old_df)
        combined = pd.concat([old_df, new_df], ignore_index=True)
        combined["timestamp"] = combined["timestamp"].astype(str)
        combined = combined.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
        _atomic_write_parquet(combined, output_file)
        added = max(0, len(combined) - old_len)
        total_rows = len(combined)
        first_ts = combined["timestamp"].iloc[0][:10]
        last_ts = combined["timestamp"].iloc[-1][:16]
    else:
        full_df = new_df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
        _atomic_write_parquet(full_df, output_file)
        added = len(full_df)
        total_rows = len(full_df)
        first_ts = full_df["timestamp"].iloc[0][:10]
        last_ts = full_df["timestamp"].iloc[-1][:16]

    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    elapsed = time.time() - t0
    print(
        f"  -> {symbol}: Tổng {total_rows:,} nến (+{added:,} mới, {size_mb:.2f} MB) "
        f"trong {elapsed:.1f}s | {first_ts} -> {last_ts}",
        flush=True,
    )
    return total_rows, added


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

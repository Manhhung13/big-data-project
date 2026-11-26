import os
import sys
import time
import argparse
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from config import BRONZE_STOCK_DIR

STOCK_OUTPUT_DIR = BRONZE_STOCK_DIR
os.makedirs(STOCK_OUTPUT_DIR, exist_ok=True)

TZ_VN = timezone(timedelta(hours=7))

HEADERS_MAS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://masboard.masvn.com/"
}

HEADERS_VND = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def fetch_chunk_tradingview(symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """
    Truy xuất nến 1m thực tế từ API TradingView backend của CTCK (MAS / VNDirect).
    Không phụ thuộc vào thư viện vnstock, không bị killswitch, không bị rate-limit khách vãng lai.
    """
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    
    # 1. Thử nguồn MAS (Mirae Asset)
    url_mas = "https://masboard.masvn.com/api/v1/tradingview/history"
    try:
        r = requests.get(
            url_mas,
            params={"symbol": symbol, "resolution": "1", "from": start_ts, "to": end_ts},
            headers=HEADERS_MAS,
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and data.get("t"):
                timestamps = [
                    datetime.fromtimestamp(ts, tz=TZ_VN).strftime("%Y-%m-%d %H:%M:%S+07:00")
                    for ts in data["t"]
                ]
                df = pd.DataFrame({
                    "timestamp": timestamps,
                    "open": [round(float(x) / 1000.0, 2) for x in data["o"]],
                    "high": [round(float(x) / 1000.0, 2) for x in data["h"]],
                    "low": [round(float(x) / 1000.0, 2) for x in data["l"]],
                    "close": [round(float(x) / 1000.0, 2) for x in data["c"]],
                    "volume": [int(x) for x in data["v"]],
                    "symbol": symbol,
                    "source": "mas_tradingview"
                })
                return df
    except Exception:
        pass

    # 2. Nguồn dự phòng: VNDirect dchart
    url_vnd = "https://dchart-api.vndirect.com.vn/dchart/history"
    try:
        r = requests.get(
            url_vnd,
            params={"symbol": symbol, "resolution": "1", "from": start_ts, "to": end_ts},
            headers=HEADERS_VND,
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("s") == "ok" and data.get("t"):
                timestamps = [
                    datetime.fromtimestamp(ts, tz=TZ_VN).strftime("%Y-%m-%d %H:%M:%S+07:00")
                    for ts in data["t"]
                ]
                df = pd.DataFrame({
                    "timestamp": timestamps,
                    "open": [round(float(x), 2) for x in data["o"]],
                    "high": [round(float(x), 2) for x in data["h"]],
                    "low": [round(float(x), 2) for x in data["l"]],
                    "close": [round(float(x), 2) for x in data["c"]],
                    "volume": [int(x) for x in data["v"]],
                    "symbol": symbol,
                    "source": "vnd_tradingview"
                })
                return df
    except Exception:
        pass

    return pd.DataFrame()


def fetch_symbol_3y_1m(symbol: str = "FPT", years: int = 3, chunk_days: int = 30):
    """
    Cào trọn vẹn 1-3 năm nến 1 phút (1m) thực tế 100% bằng kỹ thuật Chunking (Lựa chọn 1).
    Lưu trực tiếp ra Parquet chuẩn HDFS Bronze Store.
    """
    now = datetime.now()
    end_dt = now
    start_dt = end_dt - timedelta(days=365 * years)
    
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")
    output_file = os.path.join(STOCK_OUTPUT_DIR, f"{symbol}_1m.parquet")

    total_days = (end_dt - start_dt).days
    total_chunks = (total_days // chunk_days) + 1

    print("=================================================================")
    print(f"   BẮT ĐẦU CÀO TRỌN VẸN {years} NĂM NẾN 1 PHÚT (1M) CHO MÃ: {symbol}")
    print(f"   - Khoảng thời gian: {start_str} -> {end_str}")
    print(f"   - Kích thước chunk: {chunk_days} ngày / request")
    print(f"   - Tổng số chunks  : ~{total_chunks} lượt gọi")
    print(f"   - Tệp lưu trữ     : {output_file}")
    print("=================================================================\n")

    all_chunks = []
    curr_start = start_dt
    chunk_idx = 0
    t0 = time.time()

    while curr_start < end_dt:
        chunk_idx += 1
        curr_end = min(curr_start + timedelta(days=chunk_days), end_dt)
        s_str = curr_start.strftime("%Y-%m-%d")
        e_str = curr_end.strftime("%Y-%m-%d")
        
        df = fetch_chunk_tradingview(symbol, curr_start, curr_end)
        if df is not None and not df.empty:
            all_chunks.append(df)
            cum_rows = sum(len(x) for x in all_chunks)
            print(f"[{chunk_idx:02d}/{total_chunks}] {s_str} -> {e_str}: +{len(df):4d} nến | Tổng tích lũy: {cum_rows:6d} nến", flush=True)
        else:
            print(f"[{chunk_idx:02d}/{total_chunks}] {s_str} -> {e_str}: Không có nến (Sàn đóng cửa hoặc kỳ nghỉ)", flush=True)

        curr_start = curr_end + timedelta(days=1)
        time.sleep(0.3)

    if not all_chunks:
        print(f"[Thất bại] Không thu thập được dữ liệu cho mã {symbol}.")
        return

    # Nối và chuẩn hóa dữ liệu
    print("\nĐang xử lý chuẩn hóa và loại bỏ trùng lặp dữ liệu...")
    full_df = pd.concat(all_chunks, ignore_index=True)
    full_df = full_df.drop_duplicates(subset=['timestamp'], keep='last')
    full_df = full_df.sort_values(by=['timestamp']).reset_index(drop=True)

    # Lưu ra Parquet Bronze
    full_df.to_parquet(output_file, index=False)
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    duration_sec = time.time() - t0

    print("\n=================================================================")
    print(f" HOÀN TẤT THU THẬP {years} NĂM NẾN 1 PHÚT CHO {symbol}!")
    print(f"   - Tổng số nến 1m     : {len(full_df):,} dòng dữ liệu".replace(",", "."))
    print(f"   - Mốc thời gian đầu  : {full_df['timestamp'].iloc[0]}")
    print(f"   - Mốc thời gian cuối : {full_df['timestamp'].iloc[-1]}")
    print(f"   - Dung lượng Parquet : {file_size_mb:.2f} MB")
    print(f"   - Thời gian thực thi : {duration_sec:.1f} giây")
    print(f"   - Vị trí tệp lưu     : {output_file}")
    print("=================================================================\n")
    print("5 nến đầu tiên:")
    print(full_df.head(5)[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']])
    print("\n5 nến gần nhất:")
    print(full_df.tail(5)[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cào nến 1m lịch sử 1-3 năm theo Lựa chọn 1 cho 1 mã cổ phiếu")
    parser.add_argument("--symbol", type=str, default="FPT", help="Mã cổ phiếu (mặc định FPT)")
    parser.add_argument("--years", type=int, default=3, help="Số năm lùi về quá khứ (mặc định 3 năm)")
    parser.add_argument("--chunk", type=int, default=30, help="Số ngày mỗi chunk (mặc định 30 ngày)")
    args = parser.parse_args()

    fetch_symbol_3y_1m(symbol=args.symbol, years=args.years, chunk_days=args.chunk)

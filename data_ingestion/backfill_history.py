import os
import sys
import time
import re
import hashlib
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from config import SYMBOLS, CAFEF_URL, HEADERS, TZ_VN, BRONZE_STOCK_DIR, BRONZE_NEWS_DIR

STOCK_OUTPUT_DIR = BRONZE_STOCK_DIR
NEWS_OUTPUT_DIR = BRONZE_NEWS_DIR

os.makedirs(STOCK_OUTPUT_DIR, exist_ok=True)
os.makedirs(NEWS_OUTPUT_DIR, exist_ok=True)

# Zone ID chuyên mục "Thị trường chứng khoán" trên CafeF
CAFEF_ZONE_ID = 18831
CAFEF_TIMELINE_URL = "https://cafef.vn/timelinelist/{zone_id}/{page}.chn"

def save_or_merge_stock_parquet(new_df: pd.DataFrame, file_path: str):
    """Gộp dữ liệu mới với dữ liệu cũ, tự động loại bỏ trùng lặp theo timestamp (Idempotent)."""
    if os.path.exists(file_path):
        try:
            old_df = pd.read_parquet(file_path)
            combined_df = pd.concat([old_df, new_df], ignore_index=True)
            combined_df['timestamp'] = combined_df['timestamp'].astype(str)
            combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
            combined_df = combined_df.sort_values(by=['timestamp'])
            combined_df.to_parquet(file_path, index=False)
            new_added = len(combined_df) - len(old_df)
            return len(combined_df), new_added
        except Exception as e:
            print(f"    [Merge Warning] Không thể merge với file cũ ({e}), ghi file mới.")
    
    new_df['timestamp'] = new_df['timestamp'].astype(str)
    new_df = new_df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp')
    new_df.to_parquet(file_path, index=False)
    return len(new_df), len(new_df)

def fetch_stock_1year_real(symbol: str, start_date: str, end_date: str, interval: str = "1D") -> pd.DataFrame:
    """
    Thu thập 100% dữ liệu OHLCV thực tế từ vnstock.
    Xử lý cơ chế Rate-limit (10 request/phút) của cấp Guest bằng cách tạm dừng và retry tự động.
    """
    from vnstock import Market
    m = Market()
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df = m.equity(symbol).ohlcv(start=start_date, end=end_date, interval=interval)
            if df is not None and not df.empty:
                df = df.copy()
                df['symbol'] = symbol
                df['source'] = 'vnstock'
                if 'time' in df.columns:
                    df.rename(columns={'time': 'timestamp'}, inplace=True)
                df['timestamp'] = df['timestamp'].astype(str)
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            err_msg = str(e)
            if "hạn mức theo phút" in err_msg or "EntitlementError" in err_msg or "Rate" in err_msg:
                wait_time = 62
                print(f"  [Rate Limit] Chạm giới hạn 10 calls/phút của vnstock. Tạm dừng {wait_time}s rồi tiếp tục...")
                time.sleep(wait_time)
            else:
                print(f"  [Lỗi gọi vnstock] {symbol} (Lần thử {attempt+1}/{max_retries}): {e}")
                time.sleep(3)
                
    return pd.DataFrame()

def backfill_stock_history(start_date=None, end_date=None, interval="1D"):
    """
    Cào 100% dữ liệu OHLCV lịch sử 1 năm (mặc định nến ngày 1D ~ 249 phiên giao dịch)
    cho toàn bộ 30 mã VN30 từ vnstock mà không dùng bất kỳ dữ liệu giả lập nào.
    """
    now = datetime.now(TZ_VN)
    if not end_date:
        end_date = now.strftime("%Y-%m-%d")
    if not start_date:
        start_date = (now - timedelta(days=365)).strftime("%Y-%m-%d")

    print(f"\n=================================================================")
    print(f"   BẮT ĐẦU CÀO DỮ LIỆU CỔ PHIẾU THỰC TẾ 1 NĂM ({interval})          ")
    print(f"   - Khoảng thời gian: {start_date} -> {end_date}")
    print(f"   - Số lượng mã     : {len(SYMBOLS)} mã VN30")
    print(f"   - Nguồn dữ liệu   : vnstock (Thực tế 100%)")
    print(f"=================================================================\n")

    total_records = 0
    success_count = 0

    for idx, symbol in enumerate(SYMBOLS, 1):
        print(f"[{idx:02d}/{len(SYMBOLS)}] Đang tải lịch sử {symbol}...", end=" ", flush=True)
        try:
            df = fetch_stock_1year_real(symbol, start_date=start_date, end_date=end_date, interval=interval)
            if df is not None and not df.empty:
                file_path = os.path.join(STOCK_OUTPUT_DIR, f"{symbol}_{interval}.parquet")
                total_rows, new_added = save_or_merge_stock_parquet(df, file_path)
                total_records += total_rows
                success_count += 1
                t_start = df['timestamp'].iloc[0][:10]
                t_end = df['timestamp'].iloc[-1][:10]
                print(f"Thành công! {len(df)} phiên ({t_start} -> {t_end}) | File: {total_rows} dòng")
            else:
                print(f"Không có dữ liệu.")
            time.sleep(0.5)
        except Exception as e:
            print(f"Thất bại: {e}")

    print(f"\n Hoàn tất tải Stock History! Thành công {success_count}/{len(SYMBOLS)} mã, tổng cộng: {total_records} phiên tại {STOCK_OUTPUT_DIR}\n")

def parse_cafef_page(page: int) -> list:
    """Tải và bóc tách danh sách bài báo của một trang timeline từ CafeF."""
    url = CAFEF_TIMELINE_URL.format(zone_id=CAFEF_ZONE_ID, page=page)
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code != 200:
            return []
        
        soup = BeautifulSoup(res.text, "html.parser")
        boxes = soup.select(".box-category-item") or soup.select(".tlitem")
        
        records = []
        for b in boxes:
            a_tag = b.find("h3").find("a") if b.find("h3") else b.find("a")
            if not a_tag:
                continue
            
            title = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")
            full_url = f"https://cafef.vn{href}" if href.startswith("/") else href
            
            data_id = b.get("data-id")
            if not data_id:
                match = re.search(r'-(\d+)\.chn', href)
                data_id = match.group(1) if match else hashlib.md5(full_url.encode('utf-8')).hexdigest()[:12]
                
            sapo_elem = b.find(class_=re.compile(r'sapo|summary', re.I))
            content = sapo_elem.get_text(strip=True) if sapo_elem else title
            
            time_elem = b.find(class_=re.compile(r'time|date', re.I))
            published_at = time_elem.get("title") or time_elem.get_text(strip=True) if time_elem else ""
            
            records.append({
                "article_id": str(data_id),
                "title": title,
                "url": full_url,
                "content": content,
                "published_at": str(published_at),
                "crawled_at": datetime.now(TZ_VN).isoformat(),
                "source": "cafef"
            })
        return records
    except Exception:
        return []

def backfill_news_history(days_back=365, max_pages=600, batch_size=25):
    """
    Cào 100% tin tức tài chính thực tế từ CafeF lùi về quá khứ 1 năm (365 ngày).
    Sử dụng endpoint timeline chuyên mục chứng khoán kết hợp ThreadPool đa luồng.
    """
    now = datetime.now(TZ_VN)
    cutoff_date = now - timedelta(days=days_back)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")

    print(f"\n=================================================================")
    print(f"   BẮT ĐẦU CÀO TIN TỨC CAFEF THỰC TẾ 1 NĂM QUA                   ")
    print(f"   - Mốc thời gian lùi về: từ {now.strftime('%Y-%m-%d')} về {cutoff_str}")
    print(f"   - Nguồn dữ liệu       : CafeF Timeline (Zone {CAFEF_ZONE_ID})")
    print(f"=================================================================\n")

    all_articles = []
    reached_cutoff = False
    current_page = 1

    while current_page <= max_pages and not reached_cutoff:
        pages_chunk = list(range(current_page, min(current_page + batch_size, max_pages + 1)))
        print(f"Đang cào song song các trang từ {pages_chunk[0]} đến {pages_chunk[-1]}...", end=" ", flush=True)
        
        chunk_articles = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_page = {executor.submit(parse_cafef_page, p): p for p in pages_chunk}
            for future in as_completed(future_to_page):
                items = future.result()
                if items:
                    chunk_articles.extend(items)

        if not chunk_articles:
            print("Không tìm thấy bài viết nào thêm. Dừng cào.")
            break

        all_articles.extend(chunk_articles)
        print(f"Lấy được {len(chunk_articles)} bài (Tổng: {len(all_articles)} bài).")

        # Kiểm tra mốc ngày của các bài trong chunk vừa cào
        dates_in_chunk = [it['published_at'][:10] for it in chunk_articles if it['published_at'] and len(it['published_at']) >= 10]
        if dates_in_chunk:
            oldest_in_chunk = min(dates_in_chunk)
            print(f"   -> Mốc tin cũ nhất trong lượt này: {oldest_in_chunk}")
            if oldest_in_chunk <= cutoff_str:
                print(f"  [Hoàn tất mục tiêu] Đã chạm mốc 1 năm trước ({oldest_in_chunk} <= {cutoff_str}).")
                reached_cutoff = True
                break

        current_page += batch_size
        time.sleep(1)  # Lịch sự với server CafeF

    if not all_articles:
        print("[Cảnh báo] Không thu thập được bài báo nào.")
        return

    df_news = pd.DataFrame(all_articles)
    output_parquet = os.path.join(NEWS_OUTPUT_DIR, "historical_news.parquet")
    
    # Merge và loại bỏ trùng lặp nếu file đã tồn tại
    if os.path.exists(output_parquet):
        try:
            old_news = pd.read_parquet(output_parquet)
            combined_news = pd.concat([old_news, df_news], ignore_index=True)
            combined_news['article_id'] = combined_news['article_id'].astype(str)
            combined_news = combined_news.drop_duplicates(subset=['article_id'], keep='last')
            combined_news = combined_news.sort_values(by=['published_at'], ascending=False)
            combined_news.to_parquet(output_parquet, index=False)
            new_added = len(combined_news) - len(old_news)
            print(f"\n Đã lưu {len(combined_news)} bài báo thực tế vào: {output_parquet} (+{new_added} bài mới)\n")
            return
        except Exception as e:
            print(f"  [Merge News Warning] {e}")

    df_news.drop_duplicates(subset=['article_id'], keep='last').sort_values(by=['published_at'], ascending=False).to_parquet(output_parquet, index=False)
    print(f"\n Đã lưu {len(df_news)} bài báo thực tế 1 năm qua vào: {output_parquet}\n")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Thu thập 100% dữ liệu lịch sử 1 năm từ vnstock và CafeF cho Bronze Data")
    parser.add_argument("--stock-only", action="store_true", help="Chỉ cào giá cổ phiếu")
    parser.add_argument("--news-only", action="store_true", help="Chỉ cào tin tức CafeF")
    parser.add_argument("--interval", type=str, default="1D", help="Khung nến giá cổ phiếu: '1D' (1 năm) hoặc '1m' (30 ngày)")
    parser.add_argument("--days", type=int, default=365, help="Số ngày lùi về quá khứ (mặc định 365 ngày = 1 năm)")
    args = parser.parse_args()

    if args.stock_only:
        backfill_stock_history(interval=args.interval)
    elif args.news_only:
        backfill_news_history(days_back=args.days)
    else:
        backfill_stock_history(interval=args.interval)
        backfill_news_history(days_back=args.days)

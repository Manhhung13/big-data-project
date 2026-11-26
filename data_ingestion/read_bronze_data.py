import os
import sys
import glob
import argparse
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

from config import BRONZE_STOCK_DIR as STOCK_DIR, BRONZE_NEWS_DIR as NEWS_DIR


def inspect_dataframe(df: pd.DataFrame, label: str, source_desc: str, size_mb: float, head: int = 5, tail: int = 5):
    print("=================================================================")
    print(f" THÔNG TIN DỮ LIỆU BRONZE: {label}")
    print(f" - Nguồn            : {source_desc}")
    print(f" - Dung lượng       : {size_mb:.2f} MB")
    print(f" - Tổng số dòng     : {len(df):,} dòng".replace(",", "."))
    print(f" - Danh sách cột    : {list(df.columns)}")
    print("=================================================================\n")

    print(f"--- {head} DÒNG ĐẦU TIÊN (HEAD) ---")
    print(df.head(head))

    print(f"\n--- {tail} DÒNG CUỐI CÙNG (TAIL) ---")
    print(df.tail(tail))

    if 'close' in df.columns and 'volume' in df.columns and not df.empty:
        print("\n--- THỐNG KÊ CƠ BẢN (DESCRIBE) ---")
        print(df[['open', 'high', 'low', 'close', 'volume']].describe())


def inspect_file(file_path: str, head: int = 5, tail: int = 5):
    if not os.path.exists(file_path):
        print(f"[Lỗi] Không tìm thấy tệp: {file_path}")
        return
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    df = pd.read_parquet(file_path)
    inspect_dataframe(df, os.path.basename(file_path), file_path, size_mb, head=head, tail=tail)


def print_summary():
    stock_files = sorted(glob.glob(os.path.join(STOCK_DIR, "*.parquet")))
    news_file = os.path.join(NEWS_DIR, "historical_news.parquet")

    stock_rows = sum(len(pd.read_parquet(f, columns=["timestamp"])) for f in stock_files) if stock_files else 0
    stock_mb = sum(os.path.getsize(f) for f in stock_files) / (1024 * 1024) if stock_files else 0.0
    news_rows = len(pd.read_parquet(news_file, columns=["article_id"])) if os.path.exists(news_file) else 0
    news_mb = os.path.getsize(news_file) / (1024 * 1024) if os.path.exists(news_file) else 0.0

    print("=================================================================")
    print("                 TỔNG QUAN BRONZE DATA STORE                     ")
    print("=================================================================")
    print(f" 1. Stock ({STOCK_DIR}): {len(stock_files)} files | {stock_rows:,} dòng ({stock_mb:.2f} MB)")
    print(f" 2. News  ({NEWS_DIR}): {'1 file' if os.path.exists(news_file) else '0 files'} | {news_rows:,} dòng ({news_mb:.2f} MB)")
    print("=================================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Đọc và xem nhanh nội dung tệp Parquet trong Bronze Store")
    parser.add_argument("--symbol", type=str, default=None, help="Mã cổ phiếu (ví dụ: FPT, VNM, HPG)")
    parser.add_argument("--type", type=str, default="1m", choices=["1m", "1D"], help="Loại nến: 1m hoặc 1D (mặc định 1m)")
    parser.add_argument("--news", action="store_true", help="Xem tệp tin tức CafeF (historical_news.parquet)")
    parser.add_argument("--summary", action="store_true", help="Thống kê tổng số file và dòng trong Bronze Store")
    parser.add_argument("--path", type=str, default=None, help="Đường dẫn file Parquet tùy ý")
    parser.add_argument("--rows", type=int, default=5, help="Số dòng xem mẫu ở đầu và cuối (mặc định 5)")
    args = parser.parse_args()

    if args.summary:
        print_summary()
    else:
        target_path = args.path
        if not target_path:
            if args.news:
                target_path = os.path.join(NEWS_DIR, "historical_news.parquet")
            elif args.symbol:
                target_path = os.path.join(STOCK_DIR, f"{args.symbol.upper()}_{args.type}.parquet")
            else:
                target_path = os.path.join(STOCK_DIR, f"FPT_{args.type}.parquet")

        inspect_file(target_path, head=args.rows, tail=args.rows)

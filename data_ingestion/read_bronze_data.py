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


def _find_stock_files(symbol: str, interval: str = "1m", month: str = None) -> list[str]:
    sym = symbol.upper()
    if month:
        year, mon = month.split("-")
        pattern = os.path.join(STOCK_DIR, f"year={year}", f"month={mon}", f"{sym}_{interval}.parquet")
        return sorted(glob.glob(pattern))
    files = sorted(glob.glob(os.path.join(STOCK_DIR, "year=*", "month=*", f"{sym}_{interval}.parquet")))
    if not files:
        files = sorted(glob.glob(os.path.join(STOCK_DIR, f"{sym}_{interval}.parquet")))
    return files


def _find_news_files(month: str = None) -> list[str]:
    if month:
        year, mon = month.split("-")
        pattern = os.path.join(NEWS_DIR, f"year={year}", f"month={mon}", "news.parquet")
        return sorted(glob.glob(pattern))
    files = sorted(glob.glob(os.path.join(NEWS_DIR, "year=*", "month=*", "news.parquet")))
    if not files:
        legacy = os.path.join(NEWS_DIR, "historical_news.parquet")
        if os.path.exists(legacy):
            files = [legacy]
    return files


def inspect_files(files: list[str], label: str, sort_col: str, ascending: bool = True, head: int = 5, tail: int = 5):
    if not files:
        print(f"[Lỗi] Không tìm thấy tệp Parquet nào cho {label}.")
        return
    size_mb = sum(os.path.getsize(f) for f in files) / (1024 * 1024)
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    if sort_col in df.columns and not df.empty:
        df = df.sort_values(sort_col, ascending=ascending).reset_index(drop=True)
    inspect_dataframe(df, f"{label} ({len(files)} files)", os.path.dirname(files[0]), size_mb, head=head, tail=tail)


def print_summary():
    stock_files = sorted(glob.glob(os.path.join(STOCK_DIR, "year=*", "month=*", "*.parquet")))
    if not stock_files:
        stock_files = sorted(glob.glob(os.path.join(STOCK_DIR, "*.parquet")))

    news_files = sorted(glob.glob(os.path.join(NEWS_DIR, "year=*", "month=*", "*.parquet")))
    if not news_files:
        legacy_news = os.path.join(NEWS_DIR, "historical_news.parquet")
        if os.path.exists(legacy_news):
            news_files = [legacy_news]

    stock_rows = sum(len(pd.read_parquet(f, columns=["timestamp"])) for f in stock_files) if stock_files else 0
    stock_mb = sum(os.path.getsize(f) for f in stock_files) / (1024 * 1024) if stock_files else 0.0
    news_rows = sum(len(pd.read_parquet(f, columns=["article_id"])) for f in news_files) if news_files else 0
    news_mb = sum(os.path.getsize(f) for f in news_files) / (1024 * 1024) if news_files else 0.0

    stock_months = sorted({os.path.basename(os.path.dirname(os.path.dirname(f))).replace("year=", "") + "-" + os.path.basename(os.path.dirname(f)).replace("month=", "") for f in stock_files if "month=" in f})
    news_months = sorted({os.path.basename(os.path.dirname(os.path.dirname(f))).replace("year=", "") + "-" + os.path.basename(os.path.dirname(f)).replace("month=", "") for f in news_files if "month=" in f})

    print("=================================================================")
    print("          TỔNG QUAN BRONZE DATA STORE (THEO THÁNG)               ")
    print("=================================================================")
    print(f" 1. Stock ({STOCK_DIR}): {len(stock_files)} files | {stock_rows:,} dòng ({stock_mb:.2f} MB)")
    if stock_months:
        print(f"    Phân vùng tháng : {stock_months[0]} -> {stock_months[-1]} ({len(stock_months)} tháng)")
    print(f" 2. News  ({NEWS_DIR}): {len(news_files)} files | {news_rows:,} dòng ({news_mb:.2f} MB)")
    if news_months:
        print(f"    Phân vùng tháng : {news_months[0]} -> {news_months[-1]} ({len(news_months)} tháng)")
    print("=================================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Đọc và xem nhanh nội dung tệp Parquet trong Bronze Store")
    parser.add_argument("--symbol", type=str, default=None, help="Mã cổ phiếu (ví dụ: FPT, VNM, HPG)")
    parser.add_argument("--type", type=str, default="1m", choices=["1m", "1D"], help="Loại nến: 1m hoặc 1D (mặc định 1m)")
    parser.add_argument("--month", type=str, default=None, help="Lọc theo tháng YYYY-MM (ví dụ: 2026-11)")
    parser.add_argument("--news", action="store_true", help="Xem dữ liệu tin tức CafeF")
    parser.add_argument("--summary", action="store_true", help="Thống kê tổng số file và dòng trong Bronze Store")
    parser.add_argument("--path", type=str, default=None, help="Đường dẫn file Parquet tùy ý")
    parser.add_argument("--rows", type=int, default=5, help="Số dòng xem mẫu ở đầu và cuối (mặc định 5)")
    args = parser.parse_args()

    if args.summary:
        print_summary()
    elif args.path:
        inspect_files([args.path], os.path.basename(args.path), sort_col="timestamp", head=args.rows, tail=args.rows)
    elif args.news:
        files = _find_news_files(month=args.month)
        inspect_files(files, "NEWS BRONZE", sort_col="published_at", ascending=False, head=args.rows, tail=args.rows)
    else:
        sym = (args.symbol or "FPT").upper()
        files = _find_stock_files(sym, interval=args.type, month=args.month)
        inspect_files(files, f"{sym}_{args.type}", sort_col="timestamp", ascending=True, head=args.rows, tail=args.rows)

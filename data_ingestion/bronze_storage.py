import os
import uuid
from datetime import datetime
import pandas as pd

from config import BRONZE_STOCK_DIR, BRONZE_NEWS_DIR, TZ_VN

STOCK_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "symbol", "source"]
NEWS_COLUMNS = ["article_id", "title", "url", "content", "published_at", "crawled_at", "source"]


def extract_year_month(primary_ts: str, fallback_ts: str = "") -> tuple[str, str]:
    """Trích xuất (YYYY, MM) từ chuỗi thời gian để lưu phân vùng Hive year=YYYY/month=MM."""
    for candidate in (primary_ts, fallback_ts):
        if isinstance(candidate, str) and len(candidate) >= 7:
            head = candidate[:7]
            try:
                dt = datetime.strptime(head, "%Y-%m")
                return f"{dt.year:04d}", f"{dt.month:02d}"
            except ValueError:
                pass
    now = datetime.now(TZ_VN)
    return f"{now.year:04d}", f"{now.month:02d}"


def atomic_write_parquet(df: pd.DataFrame, final_path: str):
    """Ghi DataFrame ra file Parquet theo cơ chế atomic (.tmp -> os.replace) tránh hỏng file."""
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    tmp_path = f"{final_path}.{uuid.uuid4().hex[:8]}.tmp"
    df.to_parquet(tmp_path, index=False, compression="snappy")
    os.replace(tmp_path, final_path)


def normalize_stock_df(records_or_df) -> pd.DataFrame:
    """Chuẩn hóa schema 8 cột và khử trùng lặp cho dữ liệu nến cổ phiếu."""
    df = records_or_df.copy() if isinstance(records_or_df, pd.DataFrame) else pd.DataFrame(records_or_df)
    if df.empty:
        return pd.DataFrame(columns=STOCK_COLUMNS)
    for col in STOCK_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[STOCK_COLUMNS].copy()
    df["timestamp"] = df["timestamp"].astype(str)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["source"] = df["source"].fillna("kafka").astype(str)
    for num_col in ("open", "high", "low", "close"):
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").astype("float64")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    return df.drop_duplicates(subset=["symbol", "timestamp"], keep="last")


def normalize_news_df(records_or_df) -> pd.DataFrame:
    """Chuẩn hóa schema 7 cột và khử trùng lặp cho dữ liệu tin tức."""
    df = records_or_df.copy() if isinstance(records_or_df, pd.DataFrame) else pd.DataFrame(records_or_df)
    if df.empty:
        return pd.DataFrame(columns=NEWS_COLUMNS)
    for col in NEWS_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[NEWS_COLUMNS].copy()
    for col in NEWS_COLUMNS:
        df[col] = df[col].fillna("").astype(str)
    return df.drop_duplicates(subset=["article_id"], keep="last")


def merge_stock_records(records_or_df, stock_dir: str = BRONZE_STOCK_DIR) -> tuple[int, int]:
    """
    Gộp dữ liệu nến cổ phiếu vào phân vùng theo tháng:
    `{stock_dir}/year=YYYY/month=MM/{SYMBOL}_1m.parquet`.
    Trả về (tổng dòng trong các phân vùng vừa chạm, số dòng mới thêm ròng).
    """
    df = normalize_stock_df(records_or_df)
    if df.empty:
        return 0, 0

    ym = df["timestamp"].apply(lambda s: extract_year_month(s))
    df["_year"] = ym.apply(lambda t: t[0])
    df["_month"] = ym.apply(lambda t: t[1])

    total_rows = 0
    added_rows = 0

    for (year, month, symbol), group in df.groupby(["_year", "_month", "symbol"]):
        part_dir = os.path.join(stock_dir, f"year={year}", f"month={month}")
        target_file = os.path.join(part_dir, f"{symbol}_1m.parquet")
        new_part = group[STOCK_COLUMNS].reset_index(drop=True)

        if os.path.exists(target_file):
            old_df = pd.read_parquet(target_file)
            old_len = len(old_df)
            combined = pd.concat([old_df, new_part], ignore_index=True)
            combined["timestamp"] = combined["timestamp"].astype(str)
            combined = combined.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
            atomic_write_parquet(combined, target_file)
            total_rows += len(combined)
            added_rows += max(0, len(combined) - old_len)
        else:
            out_df = new_part.sort_values("timestamp").reset_index(drop=True)
            atomic_write_parquet(out_df, target_file)
            total_rows += len(out_df)
            added_rows += len(out_df)

    return total_rows, added_rows


def merge_news_records(records_or_df, news_dir: str = BRONZE_NEWS_DIR) -> tuple[int, int]:
    """
    Gộp dữ liệu tin tức vào phân vùng theo tháng:
    `{news_dir}/year=YYYY/month=MM/news.parquet`.
    Trả về (tổng dòng trong các phân vùng vừa chạm, số bài mới thêm ròng).
    """
    df = normalize_news_df(records_or_df)
    if df.empty:
        return 0, 0

    ym = df.apply(lambda r: extract_year_month(r["published_at"], r["crawled_at"]), axis=1)
    df["_year"] = ym.apply(lambda t: t[0])
    df["_month"] = ym.apply(lambda t: t[1])

    total_rows = 0
    added_rows = 0

    for (year, month), group in df.groupby(["_year", "_month"]):
        part_dir = os.path.join(news_dir, f"year={year}", f"month={month}")
        target_file = os.path.join(part_dir, "news.parquet")
        new_part = group[NEWS_COLUMNS].reset_index(drop=True)

        if os.path.exists(target_file):
            old_df = pd.read_parquet(target_file)
            old_len = len(old_df)
            combined = pd.concat([old_df, new_part], ignore_index=True)
            combined["article_id"] = combined["article_id"].astype(str)
            combined = combined.drop_duplicates(subset=["article_id"], keep="last")
            combined = combined.sort_values("published_at", ascending=False).reset_index(drop=True)
            atomic_write_parquet(combined, target_file)
            total_rows += len(combined)
            added_rows += max(0, len(combined) - old_len)
        else:
            out_df = new_part.sort_values("published_at", ascending=False).reset_index(drop=True)
            atomic_write_parquet(out_df, target_file)
            total_rows += len(out_df)
            added_rows += len(out_df)

    return total_rows, added_rows

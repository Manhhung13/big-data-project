"""Phát lại MỘT ngày giao dịch thật từ Bronze (nhánh crawl-data) vào thư mục stream_in, đúng data contract
của topic Kafka (stock-price-topic, news-topic). Thay cho auto_collector_1m.py khi không có Kafka / ngoài giờ.

  python -m replay.real_day                     # ngày REPLAY_DATE trong config, ghi tất cả ngay
  python -m replay.real_day --date 2026-11-20   # ngày khác (phải có trong Bronze và sau dữ liệu baseline)
  python -m replay.real_day --pace 15           # ghi 1 lô 5 phút mỗi 15 giây (dùng với streaming.run --mode continuous)

Nến được phát lại NGUYÊN TRẠNG như trong Bronze (kể cả nến trùng nguồn nếu có) để bước làm sạch của stream
xử lý thật. Bản nến đã làm sạch được lưu riêng làm đáp án đối chiếu OHLCV.
"""
import argparse
import datetime as dt
import json
from pathlib import Path

import pandas as pd

from config import PATHS, REPLAY_DATE, SOURCE, SOURCE_PRIORITY, SYMBOLS
from simulator.gen_live import write_files


def _slot(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("5min").dt.strftime("%Y-%m-%d %H:%M:%S")


def load_day(date: str):
    files = sorted(Path(PATHS["bronze_stock"]).glob("*_1m.parquet"))
    if not files:
        raise SystemExit(f"Không thấy Bronze ở {PATHS['bronze_stock']}. Clone repo nhóm hoặc đặt BIGDATA_TEAM_REPO.")
    stock = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    stock = stock[stock["symbol"].isin(SYMBOLS) & stock["timestamp"].str.startswith(date)].copy()
    if stock.empty:
        raise SystemExit(f"Bronze không có nến nào ngày {date}.")
    stock["ts"] = pd.to_datetime(stock["timestamp"].str[:19])
    stock = stock.sort_values(["ts", "symbol"]).reset_index(drop=True)

    news = pd.read_parquet(PATHS["bronze_news"])
    news = news[news["published_at"].str.startswith(date)].copy()
    news["ts"] = pd.to_datetime(news["published_at"].str[:19], format="%Y-%m-%dT%H:%M:%S")
    news = news.sort_values("ts").reset_index(drop=True)
    return stock, news


def clean_bars(stock: pd.DataFrame) -> pd.DataFrame:
    """Đáp án: mỗi (mã, phút) một nến, ưu tiên nguồn theo SOURCE_PRIORITY - giống batch/build_baseline.py."""
    s = stock.copy()
    s["minute"] = s["ts"].dt.floor("min")
    s["prio"] = s["source"].map({k: i for i, k in enumerate(SOURCE_PRIORITY)}).fillna(len(SOURCE_PRIORITY))
    s = s.sort_values(["symbol", "minute", "prio", "timestamp"]).drop_duplicates(["symbol", "minute"])
    return pd.DataFrame({
        "symbol": s["symbol"], "timestamp": s["minute"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        "open": s["open"], "high": s["high"], "low": s["low"], "close": s["close"], "volume": s["volume"],
    }).sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def to_batches(stock: pd.DataFrame, news: pd.DataFrame):
    batches, nbatches = {}, {}
    for (slot, r) in zip(_slot(stock["ts"]), stock.itertuples(index=False)):
        msg = {"symbol": r.symbol, "timestamp": r.timestamp, "open": float(r.open), "high": float(r.high),
               "low": float(r.low), "close": float(r.close), "volume": int(r.volume), "source": r.source,
               "ingested_at": r.timestamp}
        batches.setdefault(slot, []).append(json.dumps(msg, ensure_ascii=False))
    for (slot, r) in zip(_slot(news["ts"]), news.itertuples(index=False)):
        msg = {"article_id": r.article_id, "title": r.title, "url": r.url, "content": r.content,
               "published_at": r.published_at, "crawled_at": r.crawled_at, "source": r.source}
        nbatches.setdefault(slot, []).append(json.dumps(msg, ensure_ascii=False))
    return batches, nbatches


def main():
    if SOURCE != "real":
        raise SystemExit("replay.real_day chỉ dùng với BIGDATA_SOURCE=real")
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=REPLAY_DATE)
    ap.add_argument("--pace", type=float, default=0, help="giây giữa hai lô 5 phút (0 = ghi tất cả ngay)")
    args = ap.parse_args()
    dt.date.fromisoformat(args.date)

    stock, news = load_day(args.date)
    clean = clean_bars(stock)
    PATHS["live_bars_clean"].parent.mkdir(parents=True, exist_ok=True)
    clean.to_csv(PATHS["live_bars_clean"], index=False)
    PATHS["live_truth"].write_text(json.dumps({"live_date": args.date, "source": "bronze"}, indent=2), encoding="utf-8")

    batches, nbatches = to_batches(stock, news)
    write_files(batches, nbatches, args.pace)
    print(f"[replay] {args.date}: {len(stock):,} nến ({len(clean):,} sau khi bỏ trùng, {stock.symbol.nunique()} mã) "
          f"trong {len(batches)} file, {len(news)} bài trong {len(nbatches)} file -> {PATHS['stream_in_stock'].parent}")


if __name__ == "__main__":
    main()

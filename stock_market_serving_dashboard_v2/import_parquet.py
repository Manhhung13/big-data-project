"""Adapt local Bronze Parquet to serving views; never invent anomaly flags.

Default is a dry run over the last 30 days of the selected data, not today.
Naive news timestamps are interpreted in --news-timezone (Vietnam by default).
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd
from elasticsearch.helpers import streaming_bulk
from contract import utc_string, price_id, feature_id, news_id, density_id
from es_common import DATASET, PRICE, FEATURES, NEWS, DENSITY, es_client
from init_indexes import ensure_indexes
from serving_sink import upsert_action

DEFAULT_ROOT = Path(__file__).resolve().parent / "big-data-project-crawl-data" / "data"
UNMAPPED = "__UNMAPPED__"


def parse_news_time(value, local_timezone="Asia/Ho_Chi_Minh"):
    value = pd.Timestamp(value)
    if pd.isna(value):
        raise ValueError("missing timestamp")
    if value.tzinfo is None:
        value = value.tz_localize(local_timezone)
    return value.tz_convert("UTC")


def clean_stock(raw):
    frame = raw.copy()
    required = {"symbol", "timestamp", "open", "high", "low", "close", "volume", "source"}
    if required - set(frame):
        raise ValueError(f"Missing stock fields: {sorted(required - set(frame))}")
    # Source quotes must carry their timezone; never silently assume naive UTC.
    times = frame.timestamp.astype(str)
    aware = times.str.contains(r"(?:Z|[+-]\d{2}:?\d{2})$", regex=True)
    frame["timestamp"] = pd.to_datetime(times.where(aware), format="mixed", utc=True, errors="coerce")
    frame["symbol"] = frame.symbol.astype("string").str.strip().str.upper()
    cols = ["open", "high", "low", "close", "volume"]
    for c in cols:
        frame[c] = pd.to_numeric(frame[c], errors="coerce")
    valid = (frame.timestamp.notna() & frame.symbol.notna() & frame.symbol.str.fullmatch(r"[A-Z0-9]{3,10}")
             & np.isfinite(frame[cols]).all(axis=1)
             & frame[["open", "high", "low", "close"]].gt(0).all(axis=1)
             & frame.volume.ge(0)
             & frame.high.ge(frame[["open", "close", "low"]].max(axis=1))
             & frame.low.le(frame[["open", "close", "high"]].min(axis=1)))
    valid = valid.fillna(False)
    frame = frame[valid].sort_values("timestamp", kind="stable")
    duplicates = int(frame.duplicated(["symbol", "timestamp"], keep="last").sum())
    frame = frame.drop_duplicates(["symbol", "timestamp"], keep="last")
    return frame, {"input": len(raw), "invalid": int((~valid).sum()), "duplicates": duplicates,
                   "clean": len(frame)}


def feature_frame(prices):
    """Only observed 5m OHLCV windows. No filling lunch/overnight gaps."""
    bars = prices.set_index("timestamp").resample("5min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    bars = bars.dropna(subset=["close"])
    bars["return"] = bars.close.pct_change(fill_method=None)
    bars.loc[bars.index.to_series().diff().ne(pd.Timedelta(minutes=5)), "return"] = np.nan
    return bars


def article_symbols(title, content, symbols, aliases=None):
    # Case-sensitive ticker matching avoids treating Vietnamese words as tickers.
    text = f"{title}\n{content}"
    matches = {symbol: ("ticker", symbol) for symbol in symbols
               if re.search(r"(?<!\w)" + re.escape(symbol) + r"(?!\w)", text)}
    for symbol, names in (aliases or {}).items():
        if symbol not in symbols:
            continue
        for alias in names:
            if re.search(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", text, flags=re.IGNORECASE):
                matches.setdefault(symbol, ("alias", alias))
    return matches


def clean_news(raw, symbols, local_timezone, aliases=None):
    documents, seen, counts = [], set(), Counter(input=len(raw))
    for row in raw.to_dict("records"):
        try:
            published = parse_news_time(row["published_at"], local_timezone)
            crawled = parse_news_time(row["crawled_at"], local_timezone)
            article = str(row["article_id"]).strip()
            title = row["title"]
            if not article or article in ("None", "nan") or not isinstance(title, str) or not title.strip():
                raise ValueError("missing article ID/title")
        except (KeyError, ValueError, TypeError):
            counts["invalid"] += 1
            continue
        if article in seen:
            counts["duplicates"] += 1
            continue
        seen.add(article)
        content = row.get("content")
        content = content if isinstance(content, str) else ""
        matches = article_symbols(title, content, symbols, aliases)
        counts["mapped_articles" if matches else "unmapped_articles"] += 1
        counts["clean_articles"] += 1
        for symbol, (rule, alias) in (matches or {UNMAPPED: ("unmapped", "")}).items():
            documents.append({"article_id": article, "symbol": symbol, "title": title,
                              "content": content, "source": str(row.get("source", "")),
                              "url": str(row.get("url", "")),
                              "published_at": utc_string(published), "crawled_at": utc_string(crawled),
                              "mapping_rule": rule, "matched_alias": alias})
    return documents, dict(counts)


def action(index, key, document):
    return upsert_action(index, key, document)


def records(paths, articles, start, end, report):
    processed = utc_string(datetime.now(timezone.utc))
    for path in paths:
        frame, stats = clean_stock(pd.read_parquet(path))
        stats["first"] = utc_string(frame.timestamp.min()) if len(frame) else None
        stats["last"] = utc_string(frame.timestamp.max()) if len(frame) else None
        frame = frame[(frame.timestamp >= start) & (frame.timestamp < end)]
        stats["selected"] = len(frame)
        report["stock"][path.name] = stats
        for row in frame.to_dict("records"):
            row["timestamp"] = utc_string(row["timestamp"])
            row.update(origin="batch", processed_at=processed)
            yield action(PRICE, price_id(row["symbol"], row["timestamp"]), row)
        for symbol, group in frame.groupby("symbol"):
            for stamp, row in feature_frame(group).iterrows():
                doc = {"symbol": symbol, "window_start": utc_string(stamp),
                       "window_end": utc_string(stamp + pd.Timedelta(minutes=5)),
                       "return": float(row["return"]) if pd.notna(row["return"]) else None,
                       "close": float(row.close), "volume": float(row.volume),
                       "origin": "batch", "processed_at": processed}
                # Null/missing flags mean not evaluated, not a negative detection.
                yield action(FEATURES, feature_id(symbol, stamp), doc)
    density = Counter()
    for doc in articles:
        at = pd.Timestamp(doc["published_at"])
        if not start <= at < end:
            continue
        doc = dict(doc, origin="batch", processed_at=processed)
        yield action(NEWS, news_id(doc["article_id"], doc["symbol"]), doc)
        if doc["symbol"] != UNMAPPED:
            density[(doc["symbol"], at.floor("5min"))] += 1
    for (symbol, stamp), count in sorted(density.items()):
        doc = {"symbol": symbol, "window_start": utc_string(stamp), "window_minutes": 5,
               "news_count": count, "origin": "batch", "processed_at": processed}
        yield action(DENSITY, density_id(symbol, 5, stamp), doc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--symbols", nargs="+")
    parser.add_argument("--days", type=int, default=30, help="Last N days of source data; 0 = all history")
    parser.add_argument("--start", help="Inclusive Vietnam date or timezone-aware timestamp")
    parser.add_argument("--end", help="Exclusive Vietnam date or timezone-aware timestamp")
    parser.add_argument("--news-timezone", default="Asia/Ho_Chi_Minh")
    parser.add_argument("--aliases", type=Path, help='JSON {"FPT": ["FPT Corporation", ...]}')
    parser.add_argument("--load", action="store_true", help="Write to real Elasticsearch indexes")
    parser.add_argument("--report", type=Path, default=Path("reports/import_report.json"))
    args = parser.parse_args()
    if DATASET != "real":
        parser.error("import_parquet requires ES_DATASET=real")
    if args.days < 0:
        parser.error("--days must be >= 0")
    all_paths = sorted((args.data_root / "bronze/stock").glob("*.parquet"))
    symbols = {p.stem.removesuffix("_1m") for p in all_paths}
    selected = {s.upper() for s in args.symbols} if args.symbols else symbols
    if not selected or selected - symbols:
        parser.error(f"Missing/unknown symbols: {sorted(selected - symbols)} in {args.data_root}")
    paths = [p for p in all_paths if p.stem.removesuffix("_1m") in selected]
    latest = max(pd.to_datetime(pd.read_parquet(p, columns=["timestamp"]).timestamp,
                                format="mixed", utc=True).max() for p in paths)
    end = parse_news_time(args.end) if args.end else (latest.tz_convert("Asia/Ho_Chi_Minh").normalize()
                                                    + pd.Timedelta(days=1)).tz_convert("UTC")
    start = parse_news_time(args.start) if args.start else (
        end - pd.Timedelta(days=args.days) if args.days else pd.Timestamp("1970-01-01", tz="UTC"))
    if start >= end:
        parser.error("--start must precede --end")
    if start != start.floor("5min") or end != end.floor("5min"):
        parser.error("range boundaries must align with 5-minute windows (or use dates)")
    aliases = json.loads(args.aliases.read_text(encoding="utf-8")) if args.aliases else {}
    if not isinstance(aliases, dict) or any(not isinstance(v, list) or
        any(not isinstance(a, str) or not a.strip() for a in v) for v in aliases.values()):
        parser.error("aliases must map symbols to lists of nonempty strings")
    articles, news_stats = clean_news(pd.read_parquet(args.data_root / "bronze/news/historical_news.parquet"),
                                      symbols, args.news_timezone, aliases)
    selected_articles = [a for a in articles if start <= pd.Timestamp(a["published_at"]) < end]
    news_stats.update(
        first_published=min((a["published_at"] for a in articles), default=None),
        last_published=max((a["published_at"] for a in articles), default=None),
        sources=dict(Counter(a["source"] for a in {a["article_id"]: a for a in articles}.values())),
        selected_articles=len({a["article_id"] for a in selected_articles}),
        selected_mapped_articles=len({a["article_id"] for a in selected_articles if a["symbol"] != UNMAPPED}),
        selected_unmapped_articles=len({a["article_id"] for a in selected_articles if a["symbol"] == UNMAPPED}))
    report = {"mode": "load" if args.load else "dry_run", "data_root": str(args.data_root.resolve()),
              "start_inclusive": utc_string(start), "end_exclusive": utc_string(end),
              "news_naive_timezone": args.news_timezone, "news": news_stats, "stock": {},
              "documents": {}, "errors": 0}
    counts = Counter()

    def counted():
        for item in records(paths, articles, start, end, report):
            counts[item["_index"]] += 1
            yield item

    if args.load:
        with es_client() as es:
            ensure_indexes(es)
            for ok, item in streaming_bulk(es, counted(), chunk_size=1000, max_retries=3,
                                           raise_on_error=False, raise_on_exception=False):
                if not ok:
                    report["errors"] += 1
                    if report["errors"] <= 3:
                        print("BULK ERROR", json.dumps(item, ensure_ascii=False)[:1200])
            for index in counts:
                es.indices.refresh(index=index)
    else:
        for _ in counted():
            pass
    report["documents"] = dict(counts)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "stock"}, ensure_ascii=False, indent=2))
    print("Report:", args.report.resolve())
    if report["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

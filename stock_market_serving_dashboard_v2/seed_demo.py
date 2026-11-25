"""Generate reproducible SYNTHETIC Vietnam-equity-like data; NO real quotes.

Demo uses 5-minute OHLCV bars to keep the sample lightweight; the real
pipeline may ingest 1-minute bars and aggregate into 5-minute features.
"""
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
import random
from elasticsearch.helpers import bulk
from es_common import es_client, PRICE, FEATURES, NEWS, DENSITY, EVENTS, ALL_INDEXES, DATASET
from contract import (utc_string, price_id, feature_id, density_id,
                      news_id, classify_event)

VN = ZoneInfo("Asia/Ho_Chi_Minh")
SYMBOLS = ("FPT", "HPG", "VNM")  # Real ticker strings; values are fabricated.


def trading_times(local_day):
    for hr, start_min, end_min in ((9, 0, 60), (10, 0, 60), (11, 0, 30),
                                   (13, 0, 60), (14, 0, 60)):
        for minute in range(start_min, end_min, 5):
            yield datetime.combine(local_day, time(hr, minute), tzinfo=VN)


def records():
    rng = random.Random(20261110)
    today = datetime.now(VN).date()
    days = [today - timedelta(days=n) for n in range(35, -1, -1)]
    days = [d for d in days if d.weekday() < 5]
    processed_at = utc_string(datetime.now(timezone.utc))
    for si, symbol in enumerate(SYMBOLS):
        previous = (105.0, 28.0, 64.0)[si]
        for di, day in enumerate(days):
            daily_bars = []
            daily_news = []
            daily_events = []
            for j, at_local in enumerate(trading_times(day)):
                if at_local > datetime.now(VN):
                    continue
                at_utc = at_local.astimezone(timezone.utc)
                stamp = utc_string(at_utc)
                end_stamp = utc_string(at_utc + timedelta(minutes=5))
                # Force each combination occasionally to exercise dashboard filters.
                is_composite = di % 4 == 0 and j == 25
                is_price_only = di % 5 == 0 and j == 38
                is_volume_only = di % 5 == 0 and j == 31
                is_news_only = di % 8 == 0 and j == 12
                is_news_volume = di % 6 == 0 and j == 10
                is_price_news = di % 7 == 0 and j == 45
                is_price_volume = di % 9 == 0 and j == 20
                price_anomaly = (is_composite or is_price_only or is_price_news or is_price_volume)
                volume_anomaly = (is_composite or is_volume_only or is_news_volume or is_price_volume)
                news_burst = (is_composite or is_news_only or is_news_volume or is_price_news)
                pct = rng.uniform(-0.004, 0.004)
                if price_anomaly:
                    pct = 0.027 if di % 2 == 0 else -0.027
                close = round(max(1.0, previous * (1 + pct)), 4)
                high = round(max(previous, close) * (1 + rng.uniform(0, 0.001)), 4)
                low = round(min(previous, close) * (1 - rng.uniform(0, 0.001)), 4)
                volume = rng.randint(1000, 4500) * (6 if volume_anomaly else 1)
                return_pct = (close - previous) / previous
                rz = round(3.8 if price_anomaly and pct > 0 else (-3.8 if price_anomaly else return_pct / 0.003), 3)
                vz = 4.3 if volume_anomaly else round((volume - 2750) / 1200, 3)
                count = 8 if news_burst else rng.choices([0, 1, 2], weights=[7, 2, 1])[0]
                nz = 5.0 if news_burst else round((count - 0.4) / 0.8, 3)
                bar = {"timestamp": stamp, "symbol": symbol, "open": round(previous, 4),
                       "high": high, "low": low, "close": close, "volume": volume,
                       "source": "SYNTHETIC_DEMO", "origin": "batch", "processed_at": processed_at}
                yield {"_index": PRICE, "_id": price_id(symbol, stamp), "_source": bar}
                feature = {"symbol": symbol, "window_start": stamp, "window_end": end_stamp,
                           "return": round(return_pct, 6), "return_zscore": rz,
                           "volume_zscore": vz, "price_anomaly": price_anomaly,
                           "volume_anomaly": volume_anomaly, "close": close, "volume": volume,
                           "origin": "batch", "processed_at": processed_at}
                yield {"_index": FEATURES, "_id": feature_id(symbol, stamp), "_source": feature}
                density = {"symbol": symbol, "window_start": stamp, "window_minutes": 5,
                           "news_count": count, "news_zscore": nz,
                           "news_burst": news_burst, "origin": "batch",
                           "processed_at": processed_at}
                yield {"_index": DENSITY, "_id": density_id(symbol, 5, stamp),
                       "_source": density}
                article_ids = []
                for k in range(count):
                    published = utc_string(at_utc + timedelta(seconds=20 + k * 10))
                    article_id = f"DEMO-{symbol}-{stamp}-{k}"
                    article_ids.append(article_id)
                    item = {"article_id": article_id, "symbol": symbol,
                            "published_at": published, "crawled_at": published,
                            "processed_at": processed_at, "source": "SYNTHETIC_DEMO",
                            "title": f"[FAKE] Demo news #{k+1} about {symbol}",
                            "content": "Synthetic example. Not a genuine news article.",
                            "url": ""}
                    yield {"_index": NEWS, "_id": news_id(article_id, symbol),
                           "_source": item}
                daily_bars.append((at_utc, previous, close, count))
                daily_news.append(count)
                if price_anomaly or volume_anomaly or news_burst:
                    daily_events.append((j, stamp, price_anomaly, volume_anomaly, news_burst,
                                         rz, vz, nz, article_ids))
                previous = close
            # Historical features are computed after the day ends (where available).
            for j, stamp, p, v, n, rz, vz, nz, article_ids in daily_events:
                if j >= len(daily_bars):
                    continue
                # Use *clock time*, not row offset (lunch breaks create gaps).
                moment = daily_bars[j][0]
                by_time = {t: c for t, _o, c, _n in daily_bars}
                close_at = daily_bars[j][2]
                before = by_time.get(moment - timedelta(minutes=30))
                after30 = by_time.get(moment + timedelta(minutes=30))
                after60 = by_time.get(moment + timedelta(hours=1))
                count_before = sum(c for t, _o, _close, c in daily_bars
                                   if moment - timedelta(minutes=30) <= t < moment)
                count_after = sum(c for t, _o, _close, c in daily_bars
                                  if moment < t <= moment + timedelta(minutes=30))
                event_id = f"{symbol}_{stamp}"
                ev = {"event_id": event_id, "symbol": symbol, "event_time": stamp,
                      "window_start": stamp, "price_anomaly": p,
                      "volume_anomaly": v, "news_burst": n,
                      "event_type": classify_event(p, v, n), "status": "COMPLETED",
                      "origin": "batch", "return_zscore": rz,
                      "volume_zscore": vz, "news_zscore": nz,
                      "related_article_ids": article_ids,
                      "return_before_30m": round(close_at / before - 1, 6) if before is not None else None,
                      "return_after_30m": round(after30 / close_at - 1, 6) if after30 else None,
                      "return_after_1h": round(after60 / close_at - 1, 6) if after60 else None,
                      "news_count_before_30m": count_before,
                      "news_count_after_30m": count_after,
                      "processed_at": processed_at}
                yield {"_index": EVENTS, "_id": event_id, "_source": ev}


def main():
    if DATASET != "demo":
        raise SystemExit("Set ES_DATASET=demo before generating synthetic data.")
    es = es_client()
    if not es.ping():
        raise SystemExit("Elasticsearch not available. Run 'docker compose up -d' first.")
    success, errors = bulk(es, records(), chunk_size=500, raise_on_error=False)
    for ix in ALL_INDEXES:
        es.indices.refresh(index=ix)
        print(ix, es.count(index=ix)["count"])
    print(f"Processed {success} synthetic docs; errors={len(errors)}")
    if errors:
        raise SystemExit(f"First error: {errors[0]}")


if __name__ == "__main__":
    main()

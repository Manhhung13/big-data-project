"""Simulate an incoming/updated 5-minute window every 15 sec.

This is NOT Kafka or actual exchange data. Bars are upserted with stable IDs,
so updates to the same time bucket do not get counted as duplicate volume.
"""
import random
import time
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from es_common import es_client, PRICE, FEATURES, DENSITY, NEWS, EVENTS, DATASET
from contract import utc_string, price_id, feature_id, density_id, news_id, classify_event


def main():
    if DATASET != "demo":
        raise SystemExit("Set ES_DATASET=demo before simulating quotes.")
    es = es_client()
    if not es.ping():
        raise SystemExit("Elasticsearch not available")
    symbol = "FPT"
    rng = random.Random()
    close = 105.0
    active_window = None
    previous_close = close
    aggregate_volume = 0
    article_counter = 0
    session_id = uuid4().hex
    window_high = window_low = close
    print("Synthetic 5m live-updates for FPT. Ctrl+C to stop.")
    try:
        while True:
            now = datetime.now(timezone.utc)
            w = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
            stamp, processed_at = utc_string(w), utc_string(now)
            if stamp != active_window:
                active_window, previous_close, aggregate_volume = stamp, close, 0
                window_high = window_low = close
            close = round(max(1.0, close * (1 + rng.uniform(-0.004, 0.004))), 4)
            window_high, window_low = max(window_high, close), min(window_low, close)
            aggregate_volume += rng.randint(200, 700)
            count = 1 if article_counter % 3 == 0 else 0
            market = {"symbol": symbol, "timestamp": stamp,
                      "open": previous_close, "high": window_high,
                      "low": window_low, "close": close,
                      "volume": aggregate_volume, "source": "SYNTHETIC_STREAM",
                      "origin": "speed", "processed_at": processed_at}
            # 'wait_for' only for classroom demo; avoid per-record refresh in production.
            es.index(index=PRICE, id=price_id(symbol, stamp), document=market,
                     refresh="wait_for")
            feature = {"symbol": symbol, "window_start": stamp,
                       "window_end": utc_string(w + timedelta(minutes=5)),
                       "return": close / previous_close - 1,
                       "return_zscore": 0.0, "volume_zscore": 0.0,
                       "price_anomaly": False, "volume_anomaly": False,
                       "close": close, "volume": aggregate_volume,
                       "origin": "speed", "processed_at": processed_at}
            es.index(index=FEATURES, id=feature_id(symbol, stamp),
                     document=feature, refresh="wait_for")
            # Simulated cumulative count within current 5-minute window.
            dens_id = density_id(symbol, 5, stamp)
            prev = es.options(ignore_status=404).get(index=DENSITY, id=dens_id)
            if prev.get("found") and prev["_source"].get("origin") == "speed":
                running_count = prev["_source"]["news_count"]
            else:
                running_count = 0
            running_count += count
            news_z = round((running_count - 0.4) / 0.8, 3)
            is_burst = news_z >= 3
            density = {"symbol": symbol, "window_start": stamp,
                       "window_minutes": 5, "news_count": running_count,
                       "news_zscore": news_z, "news_burst": is_burst,
                       "origin": "speed", "processed_at": processed_at}
            es.index(index=DENSITY, id=dens_id, document=density,
                     refresh="wait_for")
            if count:
                art_id = f"SYN-LIVE-{session_id}-{article_counter}"
                article = {"article_id": art_id, "symbol": symbol,
                           "published_at": processed_at, "crawled_at": processed_at,
                           "processed_at": processed_at,
                           "source": "SYNTHETIC_STREAM", "title": f"[FAKE] Live {symbol} article {article_counter}",
                           "content": "Not a genuine news article", "url": ""}
                es.index(index=NEWS, id=news_id(art_id, symbol), document=article,
                         refresh="wait_for")
            if is_burst:
                event_id = f"{symbol}_{stamp}"
                event = {"event_id": event_id, "symbol": symbol,
                         "event_time": stamp, "window_start": stamp,
                         "event_type": classify_event(False, False, True),
                         "price_anomaly": False, "volume_anomaly": False, "news_burst": True,
                         "return_zscore": 0.0, "volume_zscore": 0.0,
                         "news_zscore": news_z, "status": "OPEN", "origin": "speed",
                         "related_article_ids": [], "processed_at": processed_at}
                es.index(index=EVENTS, id=event_id, document=event,
                         refresh="wait_for")
            print(f"{processed_at}: {symbol} close={close} volume_in_window={aggregate_volume}")
            article_counter += 1
            time.sleep(15)
    except KeyboardInterrupt:
        print("Simulation stopped")


if __name__ == "__main__":
    main()

"""Clearly synthetic end-to-end Kafka envelope fixtures (NOT real market data or real news)."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

VN = ZoneInfo('Asia/Ho_Chi_Minh')
DAYS = ['2026-10-01', '2026-10-02', '2026-10-05', '2026-10-06', '2026-10-07']


def _envelope(topic, index, payload, dt):
    return {'topic': topic, 'partition': 0, 'offset': index, 'kafka_timestamp': dt,
            'ingested_at': dt, 'ingest_date': dt.date(), 'message_key': payload.get('symbol') or str(index),
            'headers': {}, 'raw_payload': json.dumps(payload, ensure_ascii=False).encode('utf-8'),
            'raw_html_ref': None, 'schema_version': '1.0', 'decode_status': 'RAW'}


def _noise(day_index, minute, salt, spread):
    """Deterministic, well-mixed pseudo-noise in [-spread, spread] (no RNG: reruns are identical)."""
    digest = hashlib.sha256(f'{day_index}:{minute}:{salt}'.encode()).digest()
    return int.from_bytes(digest[:4], 'big') % (2 * spread + 1) - spread


def synthetic_rows():
    stock, news = [], []
    i = 0
    for day_index, d in enumerate(DAYS):
        day = date.fromisoformat(d)
        for salt, symbol in enumerate(['FPT', 'HPG']):
            base = 100000 if symbol == 'FPT' else 30000
            minute = 0
            for start_h, start_m, end_h, end_m in [(9, 0, 11, 30), (13, 0, 14, 45)]:
                t = datetime.combine(day, time(start_h, start_m), tzinfo=VN)
                stop = datetime.combine(day, time(end_h, end_m), tzinfo=VN)
                while t < stop:
                    close = base + minute * 10 + _noise(day_index, minute, salt, 30) * 10
                    volume = 100 + _noise(day_index, minute, salt + 7, 20)
                    # A conspicuous, synthetic price/volume move on Oct 7 at 10:30.
                    if d == '2026-10-07' and symbol == 'FPT' and t.hour == 10 and t.minute >= 30:
                        close += 10000
                        volume = 5000
                    p = {'schema_version': '1.0', 'symbol': symbol, 'source': 'provider_a',
                         'source_price_unit': 'VND', 'bar_start': t.astimezone(timezone.utc).isoformat(),
                         'open': close, 'high': close + 40, 'low': close - 40, 'close': close, 'volume': volume}
                    stock.append(_envelope('stock-price-topic', i, p, t.astimezone(timezone.utc)))
                    i += 1
                    t += timedelta(minutes=1)
                    minute += 1
    items = [
        ('2026-10-01', '10:21', 'FPT announces a demo filing', 'FPT HPG are fictional mentions in this fixture.'),
        ('2026-10-02', '10:21', 'HPG opens synthetic facility', 'FPT HPG are fictional mentions in this fixture.'),
        ('2026-10-05', '10:21', 'FPT HPG publish unrelated test report', 'FPT HPG are fictional mentions in this fixture.'),
        ('2026-10-06', '10:22', 'FPT demo historical report', '<p>FPT <b>demo</b> report</p><script>x()</script>'),
        ('2026-10-07', '10:31', 'FPT unusual demo news one', 'FPT HPG are fictional mentions in this fixture.'),
        ('2026-10-07', '10:32', 'FPT unusual demo news two', 'FPT HPG are fictional mentions in this fixture.'),
        ('2026-10-07', '10:33', 'FPT unusual demo news three', 'FPT HPG are fictional mentions in this fixture.'),
        # Syndicated copy of "news three" under another URL: one story, counted once in density.
        ('2026-10-07', '10:34', 'FPT unusual demo news three', 'FPT HPG are fictional mentions in this fixture.'),
    ]
    for j, (d, minute, title, body) in enumerate(items):
        dt = datetime.fromisoformat(d + 'T' + minute + ':00').replace(tzinfo=VN).astimezone(timezone.utc)
        host = 'example.org' if j < 7 else 'mirror.example.net'
        payload = {'schema_version': '1.0', 'url': f'https://{host}/demo/{j}?utm_source=test',
                   'title': title, 'content_text': body, 'source': 'demo_news',
                   'published_at': dt.isoformat(), 'crawled_at': (dt + timedelta(seconds=30)).isoformat()}
        news.append(_envelope('news-topic', j, payload, dt))
    return stock, news


def seed_demo(spark, cfg):
    from stock_batch.schemas import get
    from stock_batch.storage.catalog import Transaction
    tx = Transaction(spark, cfg, operation='seed-demo')
    tx.begin(operation='seed-demo', synthetic=True)
    try:
        stock, news = synthetic_rows()
        tx.write('bronze_stock', spark.createDataFrame(stock, get('bronze_stock')))
        tx.write('bronze_news', spark.createDataFrame(news, get('bronze_news')))
        tx.commit()
        return tx.run_id, len(stock), len(news)
    except BaseException as e:
        tx.fail(e)
        raise

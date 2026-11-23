"""B2 V5.1: Quarantine scope covers new Bronze dates and old stale dates."""
import json
from datetime import date, datetime, timezone
from stock_batch.jobs.pipeline import execute
from stock_batch.schemas import conform, get
from stock_batch.storage.catalog import Transaction


def _bronze(spark, articles):
    now = datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc)
    rows = [('news-topic', 0, i, now, now, date(2026, 10, 1), None,
             {}, json.dumps(item).encode(), None, '1.0', 'RAW')
            for i, item in enumerate(articles)]
    return spark.createDataFrame(rows, get('bronze_news'))


def _article(url, title='FPT and HPG agree'):
    return {'url': url, 'title': title, 'content_text': 'FPT HPG',
            'source': 'press', 'published_at': '2026-10-01T03:25:00Z',
            'crawled_at': '2026-10-01T03:27:00Z'}


def _q(spark, dataset, reason, key, day):
    return conform(spark.createDataFrame([{'dataset': dataset,
        'reason_code': reason, 'source_record_key': key,
        'raw_payload': b'previous', 'record_date': day}]), 'quarantine')


def test_new_news_partition_does_not_discard_old_stock_partition(spark, cfg):
    article = _article('https://example.org/article?utm_source=a')
    duplicate = _article('https://example.org/article?utm_source=b')
    invalid = _article('https://example.org/invalid', title='')
    seed = Transaction(spark, cfg)
    seed.write('bronze_news', _bronze(spark, [article, duplicate, invalid]))
    seed.write('quarantine', _q(spark, 'stock', 'INVALID_OHLC', 'stock:0:9',
                                date(2026, 9, 30)))
    seed.commit()

    execute(spark, cfg, '2026-10-01', '2026-10-07', job='B2')
    published = Transaction(spark, cfg)
    assert set(published.catalog['datasets']['quarantine']) == {'2026-09-30', '2026-10-01'}
    kinds = {(r.dataset, r.reason_code) for r in published.read('quarantine').collect()}
    assert kinds == {('stock', 'INVALID_OHLC'), ('news', 'DUPLICATE_LOSER'), ('news', 'EMPTY_TITLE')}

    execute(spark, cfg, '2026-10-01', '2026-10-07', job='B2')
    assert Transaction(spark, cfg).read('quarantine').count() == 3


def test_stale_news_partition_is_removed_while_stock_retained(spark, cfg):
    seed = Transaction(spark, cfg)
    seed.write('bronze_news', _bronze(spark, [_article('https://example.org/new')]))
    old_stock = _q(spark, 'stock', 'INVALID_OHLC', 'stock:0:8', date(2026, 9, 30))
    old_news = _q(spark, 'news', 'EMPTY_TITLE', 'news:0:11', date(2026, 9, 29))
    seed.write('quarantine', old_stock.unionByName(old_news))
    seed.commit()

    execute(spark, cfg, '2026-10-01', '2026-10-07', job='B2')
    published = Transaction(spark, cfg)
    assert set(published.catalog['datasets']['quarantine']) == {'2026-09-30'}
    assert [(r.dataset, r.reason_code) for r in published.read('quarantine').collect()] == [
        ('stock', 'INVALID_OHLC')]

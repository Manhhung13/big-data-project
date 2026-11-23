"""B2 V5 regression: cached classification, zero-reject fast path and history."""
import json
from datetime import date, datetime, timezone

from stock_batch.jobs.pipeline import execute
from stock_batch.schemas import get, conform
from stock_batch.storage.catalog import Transaction, get_run


def _bronze(spark, articles):
    ts = datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc)
    rows = [('news-topic', 0, i, ts, ts, date(2026, 10, 1), None,
             {}, json.dumps(item).encode(), None, '1.0', 'RAW')
            for i, item in enumerate(articles)]
    return spark.createDataFrame(rows, get('bronze_news'))


def _article(url, title='FPT and HPG agree'):
    return {'url': url, 'title': title, 'content_text': 'FPT HPG',
            'source': 'press', 'published_at': '2026-10-01T03:25:00Z',
            'crawled_at': '2026-10-01T03:27:00Z'}


def _q(spark, dataset, code, key, day):
    return conform(spark.createDataFrame([{
        'dataset': dataset, 'reason_code': code, 'source_record_key': key,
        'raw_payload': b'previous', 'record_date': day
    }]), 'quarantine')


def _run(spark, cfg):
    return execute(spark, cfg, '2026-10-01', '2026-10-07', job='B2')


def test_b2_no_reject_does_not_write_quarantine(spark, cfg):
    t = Transaction(spark, cfg)
    t.write('bronze_news', _bronze(spark, [_article('https://example.org/a'),
                                         _article('https://example.org/b')]))
    t.commit()
    rid = _run(spark, cfg)
    current = Transaction(spark, cfg)
    assert current.read('news').count() == 2
    assert current.read('news_stock_mapping').count() >= 2
    assert current.catalog['datasets'].get('quarantine', {}) == {}
    assert get_run(spark, cfg, rid)['quality']['B2']['rejected_rows'] == 0


def test_b2_no_reject_preserves_stock_quarantine_pointer(spark, cfg):
    t = Transaction(spark, cfg)
    t.write('bronze_news', _bronze(spark, [_article('https://example.org/a')]))
    t.write('quarantine', _q(spark, 'stock', 'INVALID_OHLC', 'stock:0:9',
                              date(2026, 9, 30)))
    t.commit()
    original = t.catalog['datasets']['quarantine']['2026-09-30']
    _run(spark, cfg)
    new = Transaction(spark, cfg)
    assert new.catalog['datasets']['quarantine']['2026-09-30'] == original
    assert new.read('quarantine').count() == 1


def test_b2_new_rejects_preserve_stock_and_idempotent(spark, cfg):
    article = _article('https://example.org/dup?utm_source=a')
    duplicate = _article('https://example.org/dup?utm_source=b')
    invalid = _article('https://example.org/invalid', title='')
    t = Transaction(spark, cfg)
    t.write('bronze_news', _bronze(spark, [article, duplicate, invalid]))
    t.write('quarantine', _q(spark, 'stock', 'INVALID_OHLC', 'stock:0:9',
                              date(2026, 9, 30)))
    t.commit()
    rid = _run(spark, cfg)
    current = Transaction(spark, cfg)
    assert current.read('news').count() == 1
    assert current.read('news_stock_mapping').count() == 2
    assert {(r.dataset, r.reason_code) for r in current.read('quarantine').collect()} == {
        ('stock', 'INVALID_OHLC'), ('news', 'DUPLICATE_LOSER'),
        ('news', 'EMPTY_TITLE')}
    assert current.read('quarantine').count() == 3
    assert get_run(spark, cfg, rid)['quality']['B2']['rejected_rows'] == 2
    _run(spark, cfg)
    assert Transaction(spark, cfg).read('quarantine').count() == 3


def test_b2_removes_stale_news_reject_without_losing_stock(spark, cfg):
    t = Transaction(spark, cfg)
    t.write('bronze_news', _bronze(spark, [_article('https://example.org/new')]))
    old_stock = _q(spark, 'stock', 'INVALID_VOLUME', 'stock:0:20',
                   date(2026, 9, 30))
    old_news = _q(spark, 'news', 'EMPTY_TITLE', 'news:0:11',
                  date(2026, 10, 1))
    t.write('quarantine', old_stock.unionByName(old_news))
    t.commit()
    _run(spark, cfg)
    new = Transaction(spark, cfg)
    assert {(r.dataset, r.source_record_key) for r in new.read('quarantine').collect()} == {
        ('stock', 'stock:0:20')}
    assert '2026-10-01' not in new.catalog['datasets']['quarantine']

"""Batch v3 regression tests for S3-S5, S8 and P1-P12 (each reproduced a confirmed v1 defect)."""
import json
import unicodedata
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from stock_batch.jobs.pipeline import UpstreamMissingError, execute
from stock_batch.schemas import conform, empty, get
from stock_batch.storage.catalog import Transaction, get_run


def env(topic, offset, obj=None, ingest=date(2026, 10, 1), raw=None):
    d = datetime(2026, 10, 1, tzinfo=timezone.utc)
    payload = raw if raw is not None else json.dumps(obj, ensure_ascii=False).encode()
    return (topic, 0, offset, d, d, ingest, 'k', {}, payload, None, '1.0', 'RAW')


def bar(t, symbol='FPT', close=100000, vol=100, **kw):
    p = {'schema_version': '1.0', 'symbol': symbol, 'source': 'provider_a', 'source_price_unit': 'VND',
         'bar_start': t.isoformat(), 'open': close, 'high': close + 50, 'low': close - 50, 'close': close, 'volume': vol}
    p.update(kw)
    return p


def article(url, title='FPT news', content='FPT', published='2026-10-01T03:25:00Z'):
    return {'schema_version': '1.0', 'url': url, 'title': title, 'content_text': content, 'source': 'press',
            'published_at': published, 'crawled_at': '2026-10-01T03:27:00Z'}


def seed(spark, cfg, stock=(), news=()):
    tx = Transaction(spark, cfg)
    if stock:
        tx.write('bronze_stock', spark.createDataFrame(list(stock), get('bronze_stock')))
    if news:
        tx.write('bronze_news', spark.createDataFrame(list(news), get('bronze_news')))
    tx.commit()


def minutes(day, n, start=(2, 0)):
    t0 = datetime(day.year, day.month, day.day, *start, tzinfo=timezone.utc)
    return [t0 + timedelta(minutes=i) for i in range(n)]


def custom_symbols(cfg, tmp_path, text):
    p = tmp_path / 'symbols.csv'
    p.write_text('symbol,exchange,company_name,aliases,effective_from,effective_to\n' + text, encoding='utf-8')
    cfg['market']['symbol_file'] = str(p)
    return cfg


# ------------------------------------------------------------------ S3 / S4 shared quarantine
def test_s3_b1_rerun_keeps_news_quarantine_of_same_day(spark, cfg):
    stock = [env('stock-price-topic', i, bar(t)) for i, t in enumerate(minutes(date(2026, 10, 1), 5))]
    news = [env('news-topic', 0, article('https://example.org/x', title=''))]
    seed(spark, cfg, stock, news)
    execute(spark, cfg, '2026-10-01', '2026-10-01', 'B2')
    execute(spark, cfg, '2026-10-01', '2026-10-01', 'B1')
    q = {(r['dataset'], r['reason_code']) for r in Transaction(spark, cfg).read('quarantine').collect()}
    assert ('news', 'EMPTY_TITLE') in q


def test_s4_backfill_reject_is_kept_under_its_trade_date(spark, cfg):
    day = date(2026, 10, 1)
    good = [env('stock-price-topic', i, bar(t), ingest=date(2026, 10, 6)) for i, t in enumerate(minutes(day, 4))]
    bad = env('stock-price-topic', 9, bar(minutes(day, 10)[9], high=1), ingest=date(2026, 10, 6))
    junk = env('stock-price-topic', 10, raw=b'{broken', ingest=date(2026, 10, 1))
    seed(spark, cfg, good + [bad, junk])
    rid = execute(spark, cfg, '2026-10-01', '2026-10-01', 'B1')
    cur = Transaction(spark, cfg)
    rejects = {r['source_record_key']: (r['reason_code'], str(r['record_date'])) for r in cur.read('quarantine').collect()}
    assert rejects == {'stock-price-topic:0:9': ('INVALID_OHLC', '2026-10-01'),
                       'stock-price-topic:0:10': ('INVALID_JSON', '2026-10-01')}
    q = get_run(spark, cfg, rid)['quality']['B1']
    assert (q['raw_rows'], q['accepted_rows'], q['rejected_rows']) == (6, 4, 2)


# ------------------------------------------------------------------ S5 stale partitions
def test_s5_b2_moves_article_without_stale_partition(spark, cfg):
    seed(spark, cfg, news=[env('news-topic', 0, article('https://example.org/a'))])
    execute(spark, cfg, '2026-10-01', '2026-10-02', 'B2')
    t = Transaction(spark, cfg)
    fixed = dict(article('https://example.org/a', published='2026-10-02T03:25:00Z'), crawled_at='2026-10-02T04:00:00Z')
    t.write('bronze_news', t.read('bronze_news').unionByName(
        spark.createDataFrame([env('news-topic', 1, fixed)], get('bronze_news'))))
    t.commit()
    execute(spark, cfg, '2026-10-01', '2026-10-02', 'B2')
    cur = Transaction(spark, cfg)
    assert cur.partitions('news') == ['2026-10-02']
    assert [str(r['publish_date']) for r in cur.read('news').collect()] == ['2026-10-02']


# ------------------------------------------------------------------ P1 / P2 / P3 / P4 / P5
def test_p1_malformed_json_is_invalid_json(spark, cfg):
    from stock_batch.transforms.news import clean_news
    from stock_batch.transforms.stock import clean_stock
    s = spark.createDataFrame([env('stock-price-topic', 0, raw=b'{not json'), env('stock-price-topic', 1, raw=b'[1,2]')],
                              get('bronze_stock'))
    n = spark.createDataFrame([env('news-topic', 0, raw=b'<<garbage')], get('bronze_news'))
    assert {r['reason_code'] for r in clean_stock(s, spark, cfg)[1].collect()} == {'INVALID_JSON'}
    assert clean_news(n, spark, cfg)[2].first()['reason_code'] == 'INVALID_JSON'


def test_p2_symbol_with_two_effective_rows_is_not_duplicated(spark, cfg, tmp_path):
    from stock_batch.transforms.stock import clean_stock
    custom_symbols(cfg, tmp_path, 'FPT,HNX,FPT,FPT,2000-01-01,2026-06-30\nFPT,HOSE,FPT,FPT,2026-07-01,\n')
    s = spark.createDataFrame([env('stock-price-topic', 0, bar(minutes(date(2026, 10, 1), 1)[0]))], get('bronze_stock'))
    ok, rej = clean_stock(s, spark, cfg)
    assert ok.count() == 1 and ok.first()['exchange'] == 'HOSE' and rej.count() == 0
    old = spark.createDataFrame([env('stock-price-topic', 1, bar(datetime(2026, 5, 4, 2, 0, tzinfo=timezone.utc)))],
                                get('bronze_stock'))
    assert clean_stock(old, spark, cfg)[1].first()['reason_code'] == 'OUTSIDE_TRADING_SESSION'  # HNX, no calendar


def test_p3_vietnamese_alias_unicode_case_and_nfd(spark, cfg, tmp_path):
    from stock_batch.transforms.news import clean_news
    custom_symbols(cfg, tmp_path, 'HPG,HOSE,Hoa Phat,HPG|Hòa Phát|A.B (x)*,2000-01-01,\n')
    rows = [env('news-topic', 0, article('https://e.org/1', title='Tập đoàn HÒA PHÁT báo lãi', content='x')),
            env('news-topic', 1, article('https://e.org/2', title=unicodedata.normalize('NFD', 'Hòa Phát mở rộng'), content='x')),
            env('news-topic', 2, article('https://e.org/3', title='Hòa Phátxyz không phải mã', content='x')),
            env('news-topic', 3, article('https://e.org/4', title='Mã a.b (X)* tăng', content='x')),
            env('news-topic', 4, article('https://e.org/5', title='AxB (x)* không khớp', content='x'))]
    n, m, _ = clean_news(spark.createDataFrame(rows, get('bronze_news')), spark, cfg)
    urls = {r['article_id']: r['canonical_url'] for r in n.collect()}
    matched = sorted(urls[r['article_id']] for r in m.collect())
    assert matched == ['https://e.org/1', 'https://e.org/2', 'https://e.org/4']


def test_p4_html_is_cleaned_and_content_html_supported(spark, cfg):
    from stock_batch.transforms.news import clean_news
    a = article('https://example.org/h', content='<p>FPT &amp; HPG</p><script>alert(1)</script>')
    b = dict(article('https://example.org/i'), content_text=None, content_html='<div>FPT <i>ký</i></div>')
    out = {r['canonical_url']: r['content_text'] for r in
           clean_news(spark.createDataFrame([env('news-topic', 0, a), env('news-topic', 1, b)], get('bronze_news')),
                      spark, cfg)[0].collect()}
    assert out == {'https://example.org/h': 'FPT & HPG', 'https://example.org/i': 'FPT ký'}


def test_p5_syndicated_copies_count_once(spark, cfg):
    from stock_batch.transforms.density import news_density
    from stock_batch.transforms.news import clean_news
    a = article('https://site-a.example/x', title='FPT ký hợp đồng', content='<p>FPT ký</p>')
    b = article('https://site-b.example/y', title='FPT  ký hợp đồng', content='FPT ký')
    c = article('https://site-c.example/z', title='FPT báo lãi', content='FPT')
    n, m, _ = clean_news(spark.createDataFrame([env('news-topic', i, x) for i, x in enumerate((a, b, c))],
                                               get('bronze_news')), spark, cfg)
    d = news_density(n, m, spark, cfg, ['2026-10-01']).filter("symbol='FPT' AND window_size='30m'")
    top = d.orderBy(F.desc('eligible_article_count')).first()
    assert (top['news_count'], top['eligible_article_count']) == (2, 3)
    cfg['news']['duplicate_group_policy'] = 'article'
    n2, m2, _ = clean_news(spark.createDataFrame([env('news-topic', i, x) for i, x in enumerate((a, b, c))],
                                                 get('bronze_news')), spark, cfg)
    assert news_density(n2, m2, spark, cfg, ['2026-10-01']).agg(F.max('news_count')).first()[0] == 3


# ------------------------------------------------------------------ P6 / P7 events
def _features(times, returns, completeness=1.0):
    return [{'symbol': 'FPT', 'exchange': 'HOSE', 'trade_date': date(2026, 10, 7), 'session_id': 'morning',
             'time_bucket': (t + timedelta(hours=7)).strftime('%H:%M'), 'window_start': t,
             'window_end': t + timedelta(minutes=5), 'interval': '5m', 'return_simple': r, 'volume_sum': 100,
             'completeness': completeness, 'close': Decimal(100)} for t, r in zip(times, returns)]


def _baselines(times, statuses=None):
    rows = []
    for t in times:
        b = (t + timedelta(hours=7)).strftime('%H:%M')
        for metric, w, mean, std, p99 in [('return_simple', '5m', 0.0, 0.01, 0.05), ('volume_sum', '5m', 100.0, 10.0, 200.0),
                                          ('news_count', '30m', 0.0, 1.0, 5.0)]:
            rows.append({'symbol': 'FPT', 'session_id': 'morning', 'time_bucket': b, 'metric': metric, 'window_size': w,
                         'mean': mean, 'std': std, 'p99': p99, 'sample_count': 30,
                         'baseline_status': (statuses or {}).get(metric, 'READY'), 'baseline_version': 'bl_x',
                         'valid_for_trading_date': date(2026, 10, 7), 'train_end': date(2026, 10, 6)})
    return rows


def _events(spark, cfg, feats, bases, density=None):
    from stock_batch.transforms.events import historical_events
    return historical_events(conform(spark.createDataFrame(feats), 'stock_features'),
                             density if density is not None else empty(spark, 'news_density'),
                             conform(spark.createDataFrame(bases), 'stock_baseline'),
                             empty(spark, 'news'), empty(spark, 'news_stock_mapping'), cfg).collect()


def test_p6_event_keeps_signed_extreme_z(spark, cfg):
    t0 = datetime(2026, 10, 7, 3, 30, tzinfo=timezone.utc)
    ts = [t0, t0 + timedelta(minutes=5)]
    ev = _events(spark, cfg, _features(ts, [-0.5, 0.1]), _baselines(ts))
    assert len(ev) == 1 and ev[0]['z_return'] == pytest.approx(-50.0)


def test_p7_unknown_baseline_is_not_false(spark, cfg):
    t0 = datetime(2026, 10, 7, 3, 30, tzinfo=timezone.utc)
    ev = _events(spark, cfg, _features([t0], [0.5]), _baselines([t0], {'volume_sum': 'SPARSE'}))
    e = ev[0]
    assert e['price_flag'] is True and e['volume_flag'] is None and e['volume_state'] == 'UNKNOWN_INSUFFICIENT_BASELINE'
    assert e['news_state'] == 'UNKNOWN_MISSING_DATA' and e['news_flag'] is None   # no density row
    assert e['status'] == 'PARTIAL_BASELINE' and e['event_type'] == 'PRICE_ONLY' and e['baseline_sample_count'] == 30


def test_events_on_different_days_are_never_merged(spark, cfg):
    days = []
    feats, bases = [], []
    for d in (6, 7):
        t = datetime(2026, 10, d, 3, 30, tzinfo=timezone.utc)
        f = _features([t], [0.5])[0]
        f['trade_date'] = date(2026, 10, d)
        feats.append(f)
        for b in _baselines([t]):
            b['valid_for_trading_date'], b['train_end'] = date(2026, 10, d), date(2026, 10, d - 1)
            bases.append(b)
    ev = sorted(_events(spark, cfg, feats, bases), key=lambda e: e['event_date'])
    assert [(str(e['event_date']), e['event_time'].day, e['event_end'].day) for e in ev] == [
        ('2026-10-06', 6, 6), ('2026-10-07', 7, 7)]


def test_float_noise_std_is_zero_std_not_ready(spark, cfg):
    from stock_batch.transforms.baseline import build_baselines
    rows = []
    for d in (1, 2, 5):
        t = datetime(2026, 10, d, 3, 30, tzinfo=timezone.utc)
        rows.append({'symbol': 'FPT', 'exchange': 'HOSE', 'trade_date': date(2026, 10, d), 'session_id': 'morning',
                     'time_bucket': '10:30', 'window_start': t, 'window_end': t + timedelta(minutes=5),
                     'interval': '5m', 'volume_sum': 100, 'return_simple': 0.1 + 0.2, 'completeness': 1.0})
    b = build_baselines(conform(spark.createDataFrame(rows), 'stock_features'), empty(spark, 'news_density'),
                        spark, cfg, ['2026-10-06'])
    assert {r['baseline_status'] for r in b.collect()} == {'ZERO_STD'}


# ------------------------------------------------------------------ P8 / P9 event study
def _full_day(spark, cfg, day, override=None):
    from stock_batch.reference import trading_grid
    g = trading_grid(spark, cfg, [day]).filter("symbol = 'FPT'")
    g = (g.withColumn('trade_date', F.col('event_date')).withColumn('interval', F.lit('5m'))
         .withColumn('close', (F.lit(100) + F.col('window_idx')).cast('decimal(20,4)'))
         .withColumn('completeness', F.lit(1.0)))
    if override:
        g = override(g)
    return g


def _density_for(spark, cfg, days):
    from stock_batch.transforms.density import news_density
    return news_density(empty(spark, 'news'), empty(spark, 'news_stock_mapping'), spark, cfg, days)


def _study(spark, cfg, feats, events, days):
    from stock_batch.transforms.study import event_study
    ev = conform(spark.createDataFrame(events), 'historical_events')
    return event_study(ev, conform(feats, 'stock_features'), _density_for(spark, cfg, days), spark, cfg, days=days)


def test_p8_intraday_horizon_does_not_cross_session(spark, cfg):
    days = ['2026-10-06', '2026-10-07']
    feats = _full_day(spark, cfg, days[0]).unionByName(_full_day(spark, cfg, days[1]))
    evs = [{'event_id': n, 'symbol': 'FPT', 'event_time': datetime(2026, 10, 6, h, m, tzinfo=timezone.utc),
            'event_date': date(2026, 10, 6), 'event_type': 'PRICE_ONLY'}
           for n, (h, m) in {'lunch': (4, 20), 'close': (7, 40), 'inside': (3, 0)}.items()]
    out = {(r['event_id'], r['horizon']): r for r in _study(spark, cfg, feats, evs, days).collect()}
    for e in ('lunch', 'close'):
        r = out[(e, '30m')]
        assert r['is_complete'] is False and r['missing_reason'] == 'HORIZON_CROSSES_SESSION' and r['return_after'] is None
    r = out[('inside', '30m')]
    assert r['is_complete'] is True and r['news_count_after'] == 0 and r['return_after'] is not None
    assert out[('close', '1tradingday')]['is_complete'] is True           # next trading day close


def test_p9_before_bar_must_be_complete_and_flag_never_null(spark, cfg):
    days = ['2026-10-07']
    before = datetime(2026, 10, 7, 3, 0, tzinfo=timezone.utc)      # T-30m for an event at 10:30 VN
    feats = _full_day(spark, cfg, days[0], lambda g: g.withColumn(
        'completeness', F.when(F.col('window_end') == F.lit(before), 0.6).otherwise(1.0)))
    anchor_missing = datetime(2026, 10, 7, 6, 30, tzinfo=timezone.utc)
    feats = feats.filter(F.col('window_end') != F.lit(anchor_missing))
    evs = [{'event_id': 'b', 'symbol': 'FPT', 'event_time': datetime(2026, 10, 7, 3, 30, tzinfo=timezone.utc),
            'event_date': date(2026, 10, 7), 'event_type': 'PRICE_ONLY'},
           {'event_id': 'a', 'symbol': 'FPT', 'event_time': anchor_missing, 'event_date': date(2026, 10, 7),
            'event_type': 'PRICE_ONLY'}]
    out = {(r['event_id'], r['horizon']): r for r in _study(spark, cfg, feats, evs, days).collect()}
    r = out[('b', '30m')]
    assert r['return_before'] is None and r['is_complete'] is False and r['missing_reason'] == 'MISSING_HISTORY_BARS'
    assert r['return_after'] is not None                                  # after-path is still valid
    assert all(out[('a', h)]['is_complete'] is False for h in ('30m', '1h', '1tradingday'))
    assert {out[('a', h)]['missing_reason'] for h in ('30m', '1h', '1tradingday')} == {'INCOMPLETE_EVENT_BAR'}
    assert all(v['is_complete'] is not None for v in out.values())


# ------------------------------------------------------------------ P10 / P11 / S8
def test_p10_b1_reconciles_raw_in_scope(spark, cfg):
    from stock_batch.fixtures import seed_demo
    seed_demo(spark, cfg)
    rid = execute(spark, cfg, '2026-10-01', '2026-10-01', 'B1')
    q = get_run(spark, cfg, rid)['quality']
    assert q['B1']['raw_rows'] == q['B1']['accepted_rows'] + q['B1']['rejected_rows'] == 510
    assert {g['gate'] for g in get_run(spark, cfg, rid)['quality']['gates']['failed']} == set()


def test_p11_upstream_missing_and_fail_on_empty(spark, cfg):
    with pytest.raises(UpstreamMissingError):
        execute(spark, cfg, '2026-10-01', '2026-10-01', 'B3')
    assert Transaction(spark, cfg).original['run_id'] is None            # nothing published
    seed(spark, cfg, stock=[env('stock-price-topic', 0, bar(minutes(date(2026, 10, 1), 1)[0]))])
    execute(spark, cfg, '2026-10-01', '2026-10-01', 'B1')
    cfg['batch']['fail_on_empty'] = True
    with pytest.raises(UpstreamMissingError, match='no partitions'):
        execute(spark, cfg, '2026-10-02', '2026-10-02', 'B3')


def test_s8_baseline_only_for_trading_days_and_removes_legacy_weekend(spark, cfg):
    from stock_batch.fixtures import seed_demo
    seed_demo(spark, cfg)
    execute(spark, cfg, '2026-10-01', '2026-10-07', 'all')
    t = Transaction(spark, cfg)                                         # plant a legacy weekend partition
    legacy = t.read('stock_baseline', partitions=['2026-10-02']).withColumn('baseline_date', F.lit(date(2026, 10, 4)))
    t.write('stock_baseline', legacy)
    t.commit()
    execute(spark, cfg, '2026-10-01', '2026-10-07', 'B5')
    assert Transaction(spark, cfg).partitions('stock_baseline') == ['2026-10-02', '2026-10-05', '2026-10-06', '2026-10-07']


# ------------------------------------------------------------------ AC06 suspension
def test_ac06_suspended_symbol_day_is_not_interpolated(spark, cfg):
    """FPT trades on 10-01 and 10-05 but is suspended on 10-02 (a trading day): no bars, no fake prices."""
    rows, i = [], 0
    for day in (date(2026, 10, 1), date(2026, 10, 5)):
        for t in minutes(day, 10):                              # 09:00-09:09 VN
            rows.append(env('stock-price-topic', i, bar(t, close=100000 + i)))
            i += 1
    seed(spark, cfg, rows)
    t = Transaction(spark, cfg)
    t.write('bronze_news', empty(spark, 'bronze_news'))         # published, but no news at all
    t.commit()
    execute(spark, cfg, '2026-10-01', '2026-10-05', 'all')
    cur = Transaction(spark, cfg)
    feats = cur.read('stock_features').filter("symbol = 'FPT'")
    assert sorted({str(r[0]) for r in feats.select('trade_date').distinct().collect()}) == ['2026-10-01', '2026-10-05']
    first = feats.filter("trade_date = DATE'2026-10-05'").orderBy('window_start').first()
    assert first['return_simple'] is None and first['return_missing_reason'] == 'START_OF_SESSION_OR_GAP'
    assert feats.filter('completeness <= 0 OR completeness > 1').count() == 0
    # The trading grid still materialises zero-news windows for the suspended day (FR-06).
    dens = cur.read('news_density', partitions=['2026-10-02']).filter("symbol = 'FPT'")
    assert dens.count() > 0 and dens.filter('news_count != 0').count() == 0
    assert cur.read('stock_price', partitions=['2026-10-02']).count() == 0


# ------------------------------------------------------------------ ADR-19 stale downstream
def test_adr19_changed_history_flags_stale_downstream(spark, cfg_factory):
    from stock_batch.fixtures import seed_demo
    cfg = cfg_factory('adr19')
    seed_demo(spark, cfg)
    execute(spark, cfg, '2026-10-01', '2026-10-07', 'all')
    same = execute(spark, cfg, '2026-10-02', '2026-10-02', 'all')
    assert (get_run(spark, cfg, same)['metadata'].get('stale_downstream') or {}) == {}   # identical rerun
    # A late correction of one 2026-10-02 bar (newer Kafka record of the same source wins B1).
    t = Transaction(spark, cfg)
    late = datetime(2026, 10, 3, tzinfo=timezone.utc)
    p = bar(datetime(2026, 10, 2, 3, 0, tzinfo=timezone.utc), close=150000)
    row = ('stock-price-topic', 0, 999999, late, late, date(2026, 10, 3), 'FPT', {}, json.dumps(p).encode(), None, '1.0', 'RAW')
    t.write('bronze_stock', spark.createDataFrame([row], get('bronze_stock')))
    t.commit()
    rid = execute(spark, cfg, '2026-10-02', '2026-10-02', 'all')
    m = get_run(spark, cfg, rid)
    stale = m['metadata']['stale_downstream']
    assert stale['stock_baseline'] == ['2026-10-05', '2026-10-06', '2026-10-07']
    assert any('re-run' in w for w in m['warnings'])


# ------------------------------------------------------------------ P12 publish policy
def test_p12_publish_accepts_zero_std_with_legacy_require_ready(spark, cfg_factory):
    from stock_batch.fixtures import seed_demo
    from stock_batch.serving import PublishError, publish_baseline
    cfg = cfg_factory('p12')
    seed_demo(spark, cfg)
    execute(spark, cfg, '2026-10-01', '2026-10-07', 'all')
    b = Transaction(spark, cfg).read('stock_baseline', partitions=['2026-10-07'])
    version = b.first()['baseline_version']
    statuses = {r[0] for r in b.select('baseline_status').distinct().collect()}
    assert 'ZERO_STD' in statuses                                        # quiet news cohorts
    cfg['baseline'].pop('publish_min_usable_ratio')
    cfg['baseline']['publish_require_ready'] = True
    from stock_batch.config import merge_defaults
    merge_defaults(cfg)
    assert publish_baseline(spark, cfg, version)['status'] == 'PUBLISHED'
    sparse_version = Transaction(spark, cfg).read('stock_baseline', partitions=['2026-10-02']).first()['baseline_version']
    with pytest.raises(PublishError, match='usable'):
        publish_baseline(spark, cfg, sparse_version, allow_older=True)    # 1 prior day < min_samples=2

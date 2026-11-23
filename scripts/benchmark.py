"""Synthetic benchmark (FR-14 / NFR-07, P1): N symbols x D trading days generated *by Spark*.

    python3 scripts/benchmark.py --symbols 30 --days 60 --root file:///tmp/stockbatch-bench

Writes its own reference calendar/symbol files next to the lake (never the repo's demo references),
seeds Bronze through the catalog, runs B1-B7 for the full range and then one incremental day, and
prints a JSON report (per-job seconds and rows from the run manifest, files/bytes from
storage-report). Data is SYNTHETIC: timings say nothing about real-market data quality.
"""
import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse


def trading_days(n, start=date(2026, 6, 1)):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def write_reference(folder, days, symbols):
    folder.mkdir(parents=True, exist_ok=True)
    cal = ['exchange,trading_date,session_id,session_start,session_end,is_trading_day,calendar_version']
    for d in days:
        cal += [f'HOSE,{d},morning,09:00,11:30,true,bench', f'HOSE,{d},afternoon,13:00,14:45,true,bench']
    (folder / 'calendar.csv').write_text('\n'.join(cal) + '\n', encoding='utf-8')
    sym = ['symbol,exchange,company_name,aliases,effective_from,effective_to']
    sym += [f'S{i:03d},HOSE,Company {i:03d},S{i:03d}|Company {i:03d},2000-01-01,' for i in range(symbols)]
    (folder / 'symbols.csv').write_text('\n'.join(sym) + '\n', encoding='utf-8')


def generate(spark, days, n_symbols, news_per_day):
    from pyspark.sql import functions as F
    from stock_batch.schemas import conform
    d = spark.createDataFrame([(i, x) for i, x in enumerate(days)], 'day_idx int, trade_date string')
    s = spark.range(n_symbols).select(F.col('id').cast('int').alias('sym_idx')) \
        .withColumn('symbol', F.format_string('S%03d', 'sym_idx')) \
        .withColumn('base', (F.lit(10000) + F.col('sym_idx') * 1000).cast('double'))
    m = spark.range(255).select(F.col('id').cast('int').alias('m'))
    g = d.crossJoin(s).crossJoin(m)
    local = F.when(F.col('m') < 150, F.expr("to_timestamp(concat(trade_date, ' 09:00')) + make_interval(0,0,0,0,0,m,0)")) \
             .otherwise(F.expr("to_timestamp(concat(trade_date, ' 13:00')) + make_interval(0,0,0,0,0,m - 150,0)"))
    h = F.xxhash64('sym_idx', 'day_idx', 'm')
    spike = (F.col('sym_idx') % 7 == 0) & (F.col('day_idx') == len(days) - 1) & F.col('m').between(90, 95)
    g = (g.withColumn('bar_utc', F.to_utc_timestamp(local, 'Asia/Ho_Chi_Minh'))
         .withColumn('close', F.round(F.col('base') * (1 + (F.pmod(h, 2001) - 1000) / 100000.0) *
                                      F.when(spike, 1.05).otherwise(1.0), 0))
         .withColumn('volume', (F.lit(100) + F.pmod(F.xxhash64('m', 'day_idx', 'sym_idx'), 900)) *
                     F.when(spike, 50).otherwise(1)))
    payload = F.to_json(F.struct(F.lit('1.0').alias('schema_version'), 'symbol', F.lit('provider_a').alias('source'),
                                 F.lit('VND').alias('source_price_unit'),
                                 F.date_format('bar_utc', "yyyy-MM-dd'T'HH:mm:ss'Z'").alias('bar_start'),
                                 F.col('close').alias('open'), (F.col('close') + 10).alias('high'),
                                 (F.col('close') - 10).alias('low'), 'close', F.col('volume').cast('long').alias('volume')))
    stock = g.select(F.lit('stock-price-topic').alias('topic'), (F.col('sym_idx') % 4).alias('partition'),
                     (F.col('sym_idx').cast('long') * 10_000_000 + F.col('day_idx') * 1000 + F.col('m')).alias('offset'),
                     F.col('bar_utc').alias('kafka_timestamp'), F.col('bar_utc').alias('ingested_at'),
                     F.to_date('trade_date').alias('ingest_date'), F.col('symbol').alias('message_key'),
                     payload.cast('binary').alias('raw_payload'), F.lit('1.0').alias('schema_version'),
                     F.lit('RAW').alias('decode_status'))
    k = spark.range(news_per_day).select(F.col('id').cast('int').alias('k'))
    n = d.crossJoin(s).crossJoin(k).withColumn(
        'pub', F.to_utc_timestamp(F.expr("to_timestamp(concat(trade_date, ' 08:30')) + make_interval(0,0,0,0,0,k * 47,0)"),
                                  'Asia/Ho_Chi_Minh'))
    npay = F.to_json(F.struct(F.lit('1.0').alias('schema_version'),
                              F.format_string('https://bench.example/%s/%s/%d', 'symbol', 'trade_date', 'k').alias('url'),
                              F.format_string('%s update %d on %s', 'symbol', 'k', 'trade_date').alias('title'),
                              F.format_string('<p>Company %03d (%s) synthetic body %d</p>', 'sym_idx', 'symbol', 'k').alias('content_text'),
                              F.lit('bench_news').alias('source'),
                              F.date_format('pub', "yyyy-MM-dd'T'HH:mm:ss'Z'").alias('published_at'),
                              F.date_format(F.expr('pub + INTERVAL 1 MINUTE'), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias('crawled_at')))
    news = n.select(F.lit('news-topic').alias('topic'), (F.col('sym_idx') % 2).alias('partition'),
                    (F.col('sym_idx').cast('long') * 10_000_000 + F.col('day_idx') * 100 + F.col('k')).alias('offset'),
                    F.col('pub').alias('kafka_timestamp'), F.col('pub').alias('ingested_at'),
                    F.to_date('trade_date').alias('ingest_date'), npay.cast('binary').alias('raw_payload'),
                    F.lit('1.0').alias('schema_version'), F.lit('RAW').alias('decode_status'))
    return conform(stock, 'bronze_stock'), conform(news, 'bronze_news')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--symbols', type=int, default=30)
    ap.add_argument('--days', type=int, default=60)
    ap.add_argument('--news-per-day', type=int, default=3)
    ap.add_argument('--root', default='file:///tmp/stockbatch-bench')
    ap.add_argument('--master', default='local[4]')
    ap.add_argument('--shuffle-partitions', type=int, default=8)
    ap.add_argument('--driver-memory', default='2g')
    a = ap.parse_args()
    if not a.root.startswith('file:'):
        raise SystemExit('benchmark writes a throw-away lake; use a file:// root')
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master(a.master).appName('stockbatch-benchmark')
             .config('spark.driver.memory', a.driver_memory).config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', str(a.shuffle_partitions))
             .config('spark.locality.wait', '0s' if a.master.startswith('local') else '3s').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    from stock_batch.config import load_config
    from stock_batch.jobs.pipeline import ORDER, execute
    from stock_batch.storage.catalog import Transaction, get_run
    from stock_batch.storage.maintenance import storage_report

    days = trading_days(a.days)
    ref = Path(urlparse(a.root).path) / '_reference'
    write_reference(ref, days, a.symbols)
    cfg = load_config('dev', root=a.root)
    cfg['market'].update(calendar_file=str(ref / 'calendar.csv'), symbol_file=str(ref / 'symbols.csv'))
    cfg['spark']['shuffle_partitions'] = a.shuffle_partitions
    report = {'symbols': a.symbols, 'trading_days': a.days, 'first_day': days[0], 'last_day': days[-1],
              'master': a.master, 'shuffle_partitions': a.shuffle_partitions, 'driver_memory': a.driver_memory}

    t0 = time.time()
    stock, news = generate(spark, days, a.symbols, a.news_per_day)
    tx = Transaction(spark, cfg, operation='benchmark-seed')
    tx.begin(operation='benchmark-seed', synthetic=True)
    report['bronze_stock_rows'] = tx.write('bronze_stock', stock)
    report['bronze_news_rows'] = tx.write('bronze_news', news)
    tx.commit()
    report['seed_seconds'] = round(time.time() - t0, 1)

    for label, (start, end) in {'full_range': (days[0], days[-1]), 'incremental_last_day': (days[-1], days[-1])}.items():
        t0 = time.time()
        rid = execute(spark, cfg, start, end, 'all')
        m = get_run(spark, cfg, rid)
        report[label] = {'wall_seconds': round(time.time() - t0, 1),
                         'jobs': {j: {'seconds': m['jobs'][j]['seconds'], 'rows': m['jobs'][j]['outputs']} for j in ORDER},
                         'gates': m['quality']['gates']['status'], 'gates_checked': m['quality']['gates']['checked']}
    sr = storage_report(spark, cfg)
    report['storage'] = {'totals': sr['totals'],
                         'per_dataset': {k: {x: v[x] for x in ('partitions', 'files', 'bytes', 'max_files_per_partition')}
                                         for k, v in sr['datasets'].items() if v['partitions']}}
    spark.stop()
    print('BENCHMARK_REPORT=' + json.dumps(report))
    return 0


if __name__ == '__main__':
    sys.exit(main())

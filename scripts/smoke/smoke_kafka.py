"""Kafka -> Bronze smoke test against a real broker (isolated stack only).

Produces examples/KAFKA_PAYLOADS.jsonl plus malformed payloads (with headers) through Spark's Kafka
sink, ingests twice, produces more, rolls back and re-ingests. Asserts: no duplicated offsets,
offsets stored in the catalog commit, bad records captured, Bronze decodes through B1/B2.
Topics must exist (verify_all.ps1 creates them with 2 partitions).
"""
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

CHECKS = []


def check(name, ok, detail=None):
    CHECKS.append({'check': name, 'status': 'PASS' if ok else 'FAIL', 'detail': detail})
    print(f"[SMOKE-KAFKA] {'PASS' if ok else 'FAIL'} {name} {detail if detail is not None else ''}", flush=True)


def produce(spark, brokers, records):
    schema = T.StructType([T.StructField('topic', T.StringType()), T.StructField('key', T.StringType()),
                           T.StructField('value', T.StringType()),
                           T.StructField('headers', T.ArrayType(T.StructType([
                               T.StructField('key', T.StringType()), T.StructField('value', T.BinaryType())])))])
    df = spark.createDataFrame(records, schema)
    (df.select('topic', F.col('key').cast('binary').alias('key'), F.col('value').cast('binary').alias('value'), 'headers')
     .write.format('kafka').option('kafka.bootstrap.servers', brokers).save())


def main():
    hdfs, brokers = os.environ.get('HDFS_ROOT', 'hdfs://namenode:8020'), os.environ.get('KAFKA_BROKERS', 'kafka:9092')
    if not hdfs.startswith('hdfs://namenode') or not brokers.startswith('kafka:'):
        raise SystemExit('refusing to run outside the isolated verify stack')
    root = f"{hdfs}/stockbatch-kafka-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    spark = (SparkSession.builder.master('local[2]').appName('stockbatch-smoke-kafka')
             .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
             .config('spark.locality.wait', '0s').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    from stock_batch.config import load_config, repository_root
    from stock_batch.ingest.kafka import ingest_kafka
    from stock_batch.jobs.pipeline import execute
    from stock_batch.lake import LakeReader
    from stock_batch.storage.catalog import get_run, rollback

    cfg = load_config('test', root=root)
    examples = [json.loads(l) for l in (repository_root() / 'examples/KAFKA_PAYLOADS.jsonl').read_text().splitlines() if l.strip()]
    hdr = lambda i: [('producer', b'smoke'), ('seq', str(i).encode())]
    batch1 = [(e['topic'], e['key'], json.dumps(e['value']), hdr(i)) for i, e in enumerate(examples)]
    batch1 += [('stock-price-topic', 'BAD', '{not json', hdr(90)),
               ('news-topic', 'BAD', json.dumps({'url': 'https://example.org/no-version'}), hdr(91))]
    produce(spark, brokers, batch1)

    run1 = ingest_kafka(spark, cfg, brokers)
    r = LakeReader(spark, cfg)
    stock, news, bad = r.read('bronze_stock'), r.read('bronze_news'), r.read('bronze_bad_records')
    offsets1 = r.kafka_offsets()
    check('first_ingest_counts', (stock.count(), news.count(), bad.count()) == (2, 2, 2),
          {'stock': stock.count(), 'news': news.count(), 'bad': bad.count()})
    check('offsets_in_catalog', sum(int(v) for v in offsets1.get('stock-price-topic', {}).values()) == 2 and
          sum(int(v) for v in offsets1.get('news-topic', {}).values()) == 2, offsets1)
    h = stock.filter("message_key = 'FPT'").first()['headers']
    check('headers_kept_as_map', h == {'producer': 'smoke', 'seq': '0'}, h)

    run2 = ingest_kafka(spark, cfg, brokers)
    q2 = get_run(spark, cfg, run2)['quality']
    check('second_ingest_reads_nothing_new', LakeReader(spark, cfg).read('bronze_stock').count() == 2 and
          'bronze_stock' not in q2 and LakeReader(spark, cfg).kafka_offsets() == offsets1, q2)

    produce(spark, brokers, [('stock-price-topic', 'HPG', json.dumps(dict(examples[0]['value'], symbol='HPG')), hdr(5))])
    run3 = ingest_kafka(spark, cfg, brokers)
    q3 = get_run(spark, cfg, run3)['quality']['bronze_stock']
    check('incremental_ingest_only_new', q3['records_read'] == 1 and q3['new_records'] == 1, q3)

    rollback(spark, cfg, run1)                       # offsets go back with Bronze
    check('rollback_restores_offsets', LakeReader(spark, cfg).kafka_offsets() == offsets1)
    run4 = ingest_kafka(spark, cfg, brokers)
    r = LakeReader(spark, cfg)
    keys = r.read('bronze_stock').groupBy('topic', 'partition', 'offset').count()
    check('replay_after_rollback_no_duplicate_offsets', keys.filter('count > 1').count() == 0 and keys.count() == 3,
          {'stock_rows': keys.count(), 'run': run4})

    # Undecodable rows are filed under their ingest date (today, Asia/Ho_Chi_Minh), so B1's scope
    # runs from the bar date to today.
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).date().isoformat()
    execute(spark, cfg, '2026-10-02', today, 'B1')
    execute(spark, cfg, '2026-10-02', '2026-10-02', 'B2')
    r = LakeReader(spark, cfg)
    sp, nw = r.read('stock_price').count(), r.read('news').count()
    rejected = {x['reason_code'] for x in r.read('quarantine').collect()}
    check('bronze_decodes_through_b1_b2', sp == 2 and nw == 1 and {'INVALID_JSON', 'EMPTY_TITLE'} <= rejected,
          {'stock_price': sp, 'news': nw, 'quarantine': sorted(rejected)})
    spark.stop()
    failed = [c for c in CHECKS if c['status'] != 'PASS']
    print('SMOKE_KAFKA_SUMMARY=' + json.dumps({'root': root, 'passed': len(CHECKS) - len(failed),
                                              'failed': len(failed), 'checks': CHECKS}, default=str))
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())

"""S11: Kafka -> Bronze is incremental, keeps headers, never duplicates offsets and stores offsets in
the catalog commit. Broker-free: ``ingest_frames`` receives Kafka-shaped DataFrames."""
import json
from datetime import datetime, timezone

from pyspark.sql import types as T

from stock_batch.ingest.kafka import ingest_frames
from stock_batch.storage.catalog import Transaction, get_run, rollback

KAFKA_SCHEMA = T.StructType([
    T.StructField('topic', T.StringType()), T.StructField('partition', T.IntegerType()),
    T.StructField('offset', T.LongType()), T.StructField('timestamp', T.TimestampType()),
    T.StructField('key', T.BinaryType()), T.StructField('value', T.BinaryType()),
    T.StructField('headers', T.ArrayType(T.StructType([T.StructField('key', T.StringType()),
                                                       T.StructField('value', T.BinaryType())])))])


def _records(spark, topic, offsets, partition=0, bad=()):
    ts = datetime(2026, 10, 2, 3, 30, tzinfo=timezone.utc)
    rows = []
    for o in offsets:
        if o in bad:
            value = b'{not json' if o % 2 else json.dumps({'symbol': 'FPT'}).encode()
        else:
            value = json.dumps({'schema_version': '1.0', 'symbol': 'FPT', 'offset_hint': o}).encode()
        headers = [('source', b'provider_a'), ('trace', f't{o}'.encode()), ('source', b'dup-ignored')]
        rows.append((topic, partition, o, ts, b'FPT', value, headers))
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def _ingest(spark, cfg, frames):
    tx = Transaction(spark, cfg, operation='ingest-kafka')
    tx.begin(operation='ingest-kafka')
    ingest_frames(spark, cfg, tx, frames)
    tx.commit()
    return tx


def test_s11_incremental_exactly_once_with_headers_and_offsets(spark, cfg):
    first = _ingest(spark, cfg, {'bronze_stock': _records(spark, 'stock-price-topic', range(0, 5), bad={3, 4})})
    cur = Transaction(spark, cfg)
    bronze = cur.read('bronze_stock')
    assert bronze.count() == 5
    row = bronze.filter('offset = 1').first()
    assert row['headers'] == {'source': 'provider_a', 'trace': 't1'}        # first header wins, map kept
    assert row['decode_status'] == 'OK' and row['ingest_date'] is not None
    assert {r['offset']: r['reason_code'] for r in cur.read('bronze_bad_records').collect()} == {
        3: 'INVALID_JSON', 4: 'MISSING_SCHEMA_VERSION'}
    assert cur.original['kafka_offsets'] == {'stock-price-topic': {'0': 5}}

    # Replay overlapping range 3..7 (at-least-once delivery): only 5,6,7 are new.
    second = _ingest(spark, cfg, {'bronze_stock': _records(spark, 'stock-price-topic', range(3, 8))})
    cur = Transaction(spark, cfg)
    offs = sorted(r['offset'] for r in cur.read('bronze_stock').collect())
    assert offs == list(range(8))
    q = get_run(spark, cfg, second.run_id)['quality']['bronze_stock']
    assert (q['records_read'], q['new_records'], q['duplicates_skipped']) == (5, 3, 2)
    assert cur.original['kafka_offsets'] == {'stock-price-topic': {'0': 8}}
    assert cur.read('bronze_bad_records').count() == 2                     # bad offsets not re-added

    # Offsets travel with the data: rollback restores both.
    rollback(spark, cfg, first.run_id)
    cur = Transaction(spark, cfg)
    assert cur.read('bronze_stock').count() == 5
    assert cur.original['kafka_offsets'] == {'stock-price-topic': {'0': 5}}


def test_s11_only_touched_partitions_are_rewritten(spark, cfg):
    from datetime import date
    from stock_batch.schemas import get
    t = Transaction(spark, cfg)
    old = datetime(2026, 9, 30, tzinfo=timezone.utc)
    t.write('bronze_stock', spark.createDataFrame([('stock-price-topic', 0, 100, old, old, date(2026, 9, 30), 'FPT', {},
                                                    b'{}', None, '1.0', 'RAW')], get('bronze_stock')))
    t.commit()
    before = Transaction(spark, cfg).original['datasets']['bronze_stock']['2026-09-30']
    _ingest(spark, cfg, {'bronze_stock': _records(spark, 'stock-price-topic', range(0, 3), partition=1)})
    cur = Transaction(spark, cfg)
    assert cur.original['datasets']['bronze_stock']['2026-09-30'] == before
    assert cur.read('bronze_stock').count() == 4
    assert cur.original['kafka_offsets']['stock-price-topic'] == {'1': 3}

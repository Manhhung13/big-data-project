"""Storage hotfix regression tests: run with Spark fixture in tests/integration/conftest.py."""
from datetime import date, datetime, timezone

from stock_batch.schemas import get, empty
from stock_batch.storage.catalog import Transaction


def _bronze(spark, items):
    # items are (day, offset); day may be None for null partition verification.
    time = datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc)
    rows = [(
        'stock-price-topic', 0, int(offset), time, time, day,
        'FPT', {}, b'{}', None, '1.0', 'RAW'
    ) for day, offset in items]
    return spark.createDataFrame(rows, schema=get('bronze_stock'))


def _isolation(cfg):
    root = cfg['project']['data_root']
    assert root.startswith('file:'), f'Tests must NEVER write to shared HDFS: {root}'


def test_hotfix_snapshot_multi_partition_and_scoped_replacement(spark, cfg):
    _isolation(cfg)
    d1, d2 = date(2026, 10, 1), date(2026, 10, 2)
    first = Transaction(spark, cfg)
    first.write('bronze_stock', _bronze(spark, [(d1, 10), (d2, 20)]))
    first.commit()
    assert Transaction(spark, cfg).read('bronze_stock').count() == 2

    second = Transaction(spark, cfg)
    second.write('bronze_stock', _bronze(spark, [(d1, 99)]),
                 only_partitions=['2026-10-01'])
    second.commit()
    values = {(str(r['ingest_date']), r['offset']) for r in
              Transaction(spark, cfg).read('bronze_stock').collect()}
    assert values == {('2026-10-01', 99), ('2026-10-02', 20)}
    for entry in Transaction(spark, cfg).original['datasets']['bronze_stock'].values():
        assert '/ingest_date=' in entry

    third = Transaction(spark, cfg)
    third.write('bronze_stock', empty(spark, 'bronze_stock'),
                only_partitions=['2026-10-01'])
    third.commit()
    final = Transaction(spark, cfg).read('bronze_stock').collect()
    assert len(final) == 1
    assert final[0]['offset'] == 20


def test_hotfix_null_partition_round_trip(spark, cfg):
    _isolation(cfg)
    tx = Transaction(spark, cfg)
    tx.write('bronze_stock', _bronze(spark, [(None, 42)]))
    tx.commit()
    row = Transaction(spark, cfg).read('bronze_stock').first()
    assert row['offset'] == 42
    assert row['ingest_date'] is None
    assert '__NULL__' in Transaction(spark, cfg).original['datasets']['bronze_stock']

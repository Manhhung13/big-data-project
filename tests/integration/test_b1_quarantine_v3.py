"""Regression test for B1 complete daily sample (510 valid, no rejected rows)."""
from stock_batch.fixtures import synthetic_rows
from stock_batch.schemas import get, empty
from stock_batch.storage.catalog import Transaction
from stock_batch.transforms.stock import clean_stock
from pyspark.sql import functions as F


def test_b1_quarantine_v3_full_demo_and_empty_partition(spark, cfg):
    assert cfg['project']['data_root'].startswith('file:'), 'Refusing to write to shared HDFS'
    stock_rows, _ = synthetic_rows()
    bronze = spark.createDataFrame(stock_rows, schema=get('bronze_stock'))
    accepted, rejected, classified = clean_stock(bronze, spark, cfg, cache_classified=True)
    try:
        dates = ['2026-10-01']
        accepted = accepted.filter(F.col('trade_date').cast('string').isin(*dates))
        rejected = rejected.filter(F.col('record_date').cast('string').isin(*dates))
        tx = Transaction(spark, cfg)
        tx.write('stock_price', accepted, only_partitions=dates)
        n_bad = rejected.count()
        assert n_bad == 0
        tx.write('quarantine', empty(spark, 'quarantine'), only_partitions=dates)
        tx.commit()
    finally:
        classified.unpersist(blocking=False)
    reader = Transaction(spark, cfg)
    assert reader.read('stock_price').count() == 510
    assert reader.read('quarantine').count() == 0
    assert reader.original['datasets']['quarantine'] == {}

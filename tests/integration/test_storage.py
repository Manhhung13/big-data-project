"""AC14/AC15: local immutable Parquet catalogs, real Spark only."""
from datetime import datetime,date,timezone
from stock_batch.storage.catalog import Transaction,FileSystem,rollback
from stock_batch.schemas import get


def _one(spark,offset=10):
    stamp=datetime(2026,10,1,3,30,tzinfo=timezone.utc)
    return spark.createDataFrame([('stock-price-topic',0,offset,stamp,stamp,date(2026,10,1),
         'FPT',{},b'{}',None,'1.0','RAW')],get('bronze_stock'))


def test_ac14_repeat_same_partition_has_one_logical_key(spark,cfg):
    tx=Transaction(spark,cfg)
    tx.write('bronze_stock',_one(spark))
    tx.commit()
    a=Transaction(spark,cfg)
    assert a.read('bronze_stock').count()==1
    a.write('bronze_stock',_one(spark))
    a.commit()
    b=Transaction(spark,cfg)
    assert b.read('bronze_stock').count()==1
    assert a.run_id!=tx.run_id


def test_ac15_failed_staging_does_not_modify_current(spark,cfg):
    a=Transaction(spark,cfg)
    a.write('bronze_stock',_one(spark))
    a.commit()
    b=Transaction(spark,cfg)
    b.write('bronze_stock',_one(spark,offset=999))
    b.fail(RuntimeError('simulated mid-Gold failure'))
    current=Transaction(spark,cfg)
    assert current.original['run_id']==a.run_id
    assert current.read('bronze_stock').first()['offset']==10


def test_rollback_successful_snapshot(spark,cfg):
    a=Transaction(spark,cfg);a.write('bronze_stock',_one(spark));a.commit()
    b=Transaction(spark,cfg);b.write('bronze_stock',_one(spark,offset=99));b.commit()
    assert Transaction(spark,cfg).read('bronze_stock').first()['offset']==99
    rollback(spark,cfg,a.run_id)
    assert Transaction(spark,cfg).read('bronze_stock').first()['offset']==10

"""AC16 optional: requires shared Hive Metastore and HIVE_METASTORE_URIS."""
import os
import pytest

@pytest.mark.skipif(not os.environ.get('HIVE_METASTORE_URIS'),reason='external Hive Metastore not configured')
def test_ac16_hive_external_reads_published_data(spark,cfg):
    from stock_batch.cli import register_hive
    from stock_batch.storage.catalog import Transaction
    if not Transaction(spark,cfg).original.get('datasets'):
        pytest.skip('seed/publish data before Hive integration')
    registered=register_hive(spark,cfg,'stock_batch_ci')
    assert registered['registered']
    for x in registered['registered']:
        spark.sql(f"SELECT * FROM stock_batch_ci.current_{x['dataset']} LIMIT 1").collect()

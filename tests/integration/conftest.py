import os
import pytest

@pytest.fixture(scope='session')
def spark():
    from pyspark.sql import SparkSession
    os.environ['SPARK_LOCAL_IP']='127.0.0.1'
    s=(SparkSession.builder.master('local[2]').appName('stock-batch-integration')
       .config('spark.sql.session.timeZone','UTC')
       .config('spark.sql.shuffle.partitions','2').config('spark.locality.wait','0s').getOrCreate())
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()


def _isolated_config(path):
    from stock_batch.config import load_config

    expected_root = path.as_uri()
    config = load_config("test", root=expected_root)

    assert config["project"]["data_root"] == expected_root, (
        "Integration test must use isolated temporary storage. "
        "Refusing to run against shared HDFS."
    )
    assert expected_root.startswith('file:'), 'tests may only write to local temporary storage'

    return config


@pytest.fixture
def cfg(tmp_path):
    return _isolated_config(tmp_path)


@pytest.fixture(scope='session')
def cfg_factory(tmp_path_factory):
    """Create further isolated lakes (same guard as ``cfg``), e.g. for module-scoped fixtures."""
    def make(name='lake'):
        return _isolated_config(tmp_path_factory.mktemp(name))
    return make

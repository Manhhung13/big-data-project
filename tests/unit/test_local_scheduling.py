"""Guard against the Spark local-mode delay-scheduling hang (no JVM: SparkContext is stubbed)."""
from types import SimpleNamespace

import pytest

from stock_batch.jobs.pipeline import _is_zero_duration, check_local_scheduling


def fake_spark(master, **conf):
    return SimpleNamespace(sparkContext=SimpleNamespace(master=master, getConf=lambda: conf))


@pytest.mark.parametrize('value,zero', [('0', True), ('0s', True), ('0ms', True), (' 0.0 S ', True),
                                        ('3s', False), ('100ms', False), ('1', False), ('abc', False)])
def test_is_zero_duration(value, zero):
    assert _is_zero_duration(value) is zero


def test_local_mode_with_default_locality_wait_is_refused():
    with pytest.raises(ValueError, match='spark.locality.wait=0s'):
        check_local_scheduling(fake_spark('local[2]'))


def test_local_mode_with_zero_locality_wait_passes():
    check_local_scheduling(fake_spark('local[*]', **{'spark.locality.wait': '0s'}))


def test_per_level_override_is_checked():
    spark = fake_spark('local[1]', **{'spark.locality.wait': '0s', 'spark.locality.wait.node': '3s'})
    with pytest.raises(ValueError, match='spark.locality.wait.node'):
        check_local_scheduling(spark)


def test_cluster_mode_keeps_delay_scheduling():
    check_local_scheduling(fake_spark('spark://master:7077'))
    check_local_scheduling(fake_spark('yarn', **{'spark.locality.wait': '3s'}))

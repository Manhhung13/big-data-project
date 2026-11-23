"""CLI wiring (in-process dispatch, shared SparkSession) and S6 compaction provenance."""
import glob
from urllib.parse import urlparse

import pytest
from pyspark.sql import functions as F

from stock_batch.cli import build_parser, dispatch
from stock_batch.fixtures import seed_demo
from stock_batch.lake import LakeReader


@pytest.fixture(scope='module')
def lake(spark, cfg_factory):
    cfg = cfg_factory('cli')
    seed_demo(spark, cfg)
    return cfg


def cli(spark, cfg, *argv):
    return dispatch(spark, cfg, build_parser().parse_args(['--env', 'test', *argv]))


def test_cli_end_to_end_commands(spark, lake, tmp_path):
    cfg = lake
    run = cli(spark, cfg, 'run', '--job', 'all', '--start', '2026-10-01', '--end', '2026-10-07')
    assert run['status'] == 'SUCCESS' and run['generation'] == 2
    assert cli(spark, cfg, 'verify', '--run-id', run['run_id'])['status'] == 'OK'
    assert cli(spark, cfg, 'show-current')['generation'] == 2
    assert cli(spark, cfg, 'list-runs', '--limit', '5')[0]['run_id'] == run['run_id']
    rep = cli(spark, cfg, 'storage-report')
    assert rep['datasets']['stock_features']['max_files_per_partition'] == 1
    assert cli(spark, cfg, 'gc')['mode'] == 'DRY_RUN'
    assert cli(spark, cfg, 'show-baseline') == {'status': 'NO_BASELINE'}
    version = LakeReader(spark, cfg).read('stock_baseline', '2026-10-07', '2026-10-07').first()['baseline_version']
    assert cli(spark, cfg, 'publish-baseline', '--version', version)['baseline_version'] == version
    assert cli(spark, cfg, 'show-baseline')['baseline_version'] == version
    out = cli(spark, cfg, 'export', '--dataset', 'event_analysis', '--format', 'parquet', '--out', (tmp_path / 'ea').as_uri())
    assert out['rows'] > 0 and out['id_fields'] == ['event_id', 'horizon', 'analysis_version']
    again = cli(spark, cfg, 'run', '--from-job', 'B5', '--start', '2026-10-07', '--end', '2026-10-07')
    from stock_batch.storage.catalog import get_run
    assert list(get_run(spark, cfg, again['run_id'])['jobs']) == ['B5', 'B6', 'B7']
    back = cli(spark, cfg, 'rollback', '--run-id', run['run_id'])
    assert back['status'] == 'ROLLED_BACK' and LakeReader(spark, cfg).generation == back['generation']


def test_s6_compact_preserves_provenance_and_fingerprints(spark, lake):
    cfg = lake
    reader = LakeReader(spark, cfg)
    if 'stock_features' not in reader.datasets():
        cli(spark, cfg, 'run', '--job', 'all', '--start', '2026-10-01', '--end', '2026-10-07')
        reader = LakeReader(spark, cfg)
    cols = ['symbol', 'window_start', 'run_id', 'processing_timestamp', 'config_hash', 'code_version', 'source_snapshot']
    before = sorted(tuple(r) for r in reader.read('stock_features').select(*cols).collect())
    res = cli(spark, cfg, 'compact', '--dataset', 'stock_features', '--start', '2026-10-01', '--end', '2026-10-07',
              '--files', '2')
    after_reader = LakeReader(spark, cfg)
    after = sorted(tuple(r) for r in after_reader.read('stock_features').select(*cols).collect())
    assert after == before                                         # provenance untouched (S6)
    assert after_reader.stats('stock_features') == reader.stats('stock_features')
    assert after_reader.run_id == res['run_id'] != reader.run_id
    for path in after_reader.catalog['datasets']['stock_features'].values():
        assert 1 <= len(glob.glob(urlparse(path).path + '/*.parquet')) <= 2

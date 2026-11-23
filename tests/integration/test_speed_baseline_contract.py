"""F1: the published baseline export must conform to contracts/gold/speed_baseline_rows.schema.json.

Validates the real Parquet at ``export_path`` against the enums/properties of the JSON schema
(no jsonschema dependency: the image only ships pytest + PyYAML).
"""
import json

from pyspark.sql import functions as F

from stock_batch.config import repository_root
from stock_batch.fixtures import seed_demo
from stock_batch.jobs.pipeline import execute
from stock_batch.lake import LakeReader
from stock_batch.serving import publish_baseline


def _schema():
    return json.loads((repository_root() / 'contracts/gold/speed_baseline_rows.schema.json').read_text(encoding='utf-8'))


def test_published_baseline_export_matches_row_contract(spark, cfg_factory):
    cfg = cfg_factory('speed_contract')
    seed_demo(spark, cfg)
    execute(spark, cfg, '2026-10-01', '2026-10-07', 'all')
    reader = LakeReader(spark, cfg)
    version = reader.read('stock_baseline', '2026-10-07', '2026-10-07').first()['baseline_version']
    pointer = publish_baseline(spark, cfg, version)
    rows = spark.read.parquet(pointer['export_path'])
    schema = _schema()
    props = schema['properties']

    assert schema.get('additionalProperties') is False
    assert set(rows.columns) <= set(props), sorted(set(rows.columns) - set(props))
    assert set(schema['required']) <= set(rows.columns)

    pairs = {(r['metric'], r['window_size']) for r in rows.select('metric', 'window_size').distinct().collect()}
    assert {m for m, _ in pairs} <= set(props['metric']['enum'])
    bad_windows = {w for _, w in pairs if w not in props['window_size']['enum']}
    assert not bad_windows, f'window_size values not in the contract enum: {sorted(bad_windows)}'
    statuses = {r[0] for r in rows.select('baseline_status').distinct().collect()}
    assert statuses <= set(props['baseline_status']['enum'])
    sessions = {r[0] for r in rows.select('session_id').distinct().collect()}
    assert sessions <= set(props['session_id']['enum'])
    # Documented grain: price/volume only at 5m; news_count at every configured window.
    assert {w for m, w in pairs if m != 'news_count'} == {'5m'}
    assert {w for m, w in pairs if m == 'news_count'} == {'5m', '30m', '1h'}
    assert rows.filter(F.col('sample_count') < 1).count() == 0
    assert rows.count() == pointer['row_count']

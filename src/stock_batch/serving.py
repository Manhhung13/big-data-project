"""Integration points for downstream modules: baseline publication (Speed Layer, FR-11) and
generation-pinned exports (Serving / Elasticsearch). Read-only with respect to Silver/Gold.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

from stock_batch.storage.catalog import Transaction, code_version, system_paths, utcnow
from stock_batch.storage.fs import dumps

SPEED_COLUMNS = ['baseline_version', 'baseline_date', 'valid_for_trading_date', 'train_start', 'train_end',
                 'symbol', 'exchange', 'session_id', 'time_bucket', 'metric', 'window_size', 'sample_count',
                 'mean', 'std', 'median', 'p95', 'p99', 'baseline_status', 'fallback_level', 'formula_version',
                 'source_snapshot', 'run_id', 'config_hash']
USABLE = ('READY', 'ZERO_STD')
_VERSION_DATE = re.compile(r'_(\d{4}-\d{2}-\d{2})_')

# Elasticsearch document identity per dataset (stable across reruns: business keys only).
EXPORT_KEYS = {
    'historical_events': ['event_id'],
    'event_analysis': ['event_id', 'horizon', 'analysis_version'],
    'news': ['article_id'],
    'news_stock_mapping': ['article_id', 'symbol', 'mapping_version'],
    'stock_features': ['symbol', 'window_start', 'interval'],
    'news_density': ['symbol', 'window_end', 'window_size'],
    'stock_baseline': ['baseline_version', 'symbol', 'session_id', 'time_bucket', 'metric', 'window_size'],
    'stock_price': ['symbol', 'bar_start'],
}
RESTRICTED_COLUMNS = {'news': ['content_text']}   # BRD 11.1: no full text unless licensed


class PublishError(ValueError):
    pass


def baseline_pointer_path(root):
    return f"{system_paths(root)['baselines']}/current.json"


def read_published_baseline(spark, cfg):
    from stock_batch.storage.fs import FileSystem
    root = cfg['project']['data_root'].rstrip('/')
    return FileSystem(spark, root).read_json(baseline_pointer_path(root))


def publish_baseline(spark, cfg, version, allow_older=False):
    """Validate one baseline_version, export it as immutable Parquet and swap the pointer atomically."""
    from pyspark.sql import functions as F
    tx = Transaction(spark, cfg, operation='publish-baseline')
    m = _VERSION_DATE.search(version)
    parts = [m.group(1)] if m and m.group(1) in tx.partitions('stock_baseline') else None
    b = tx.read('stock_baseline', partitions=parts).filter(F.col('baseline_version') == version)
    agg = b.agg(F.count(F.lit(1)).alias('rows'),
                F.sum(F.when(F.col('train_end') >= F.col('valid_for_trading_date'), 1).otherwise(0)).alias('leak'),
                F.collect_set(F.col('valid_for_trading_date').cast('string')).alias('dates'),
                F.max('train_end').cast('string').alias('train_end')).first()
    if not agg['rows']:
        raise PublishError(f'baseline_version not found in the current catalog: {version}')
    if agg['leak']:
        raise PublishError('unsafe baseline cutoff: train_end >= valid_for_trading_date')
    if len(agg['dates']) != 1:
        raise PublishError('baseline_version must target exactly one trading day')
    valid_date = agg['dates'][0]
    status_counts = {r['baseline_status']: int(r['count']) for r in b.groupBy('baseline_status').count().collect()}
    usable = sum(n for s, n in status_counts.items() if s in USABLE) / agg['rows']
    min_ratio = float(cfg['baseline'].get('publish_min_usable_ratio', 0.8))
    if usable < min_ratio:
        raise PublishError(f'only {usable:.1%} of cohorts are usable (READY/ZERO_STD); policy requires '
                           f'>= {min_ratio:.0%} (baseline.publish_min_usable_ratio). status={status_counts}')
    root = tx.root
    current = tx.fs.read_json(baseline_pointer_path(root))
    if current and current.get('valid_for_trading_date', '') > valid_date and not allow_older:
        raise PublishError(f"currently published baseline is for {current['valid_for_trading_date']}; refusing "
                           f'to move back to {valid_date} without --allow-older')

    export = f"{system_paths(root)['baselines']}/versions/{version}"
    if not tx.fs.exists(export):
        staging = f"{system_paths(root)['baselines']}/_staging/{version}_{tx.run_id}"
        (b.select(*SPEED_COLUMNS).repartition(1).sortWithinPartitions('symbol', 'metric', 'window_size',
                                                                       'session_id', 'time_bucket')
         .write.mode('errorifexists').option('compression', cfg['spark']['compression']).parquet(staging))
        try:
            tx.fs.rename_directory(staging, export)        # atomic publish of the immutable export
        except FileExistsError:
            tx.fs.delete(staging)                          # a concurrent publisher won; same content
    exported_rows = spark.read.parquet(export).count()
    if exported_rows != agg['rows']:
        raise PublishError(f'export {export} has {exported_rows} rows, catalog has {agg["rows"]}')
    payload = {'baseline_version': version, 'valid_for_trading_date': valid_date, 'train_end': agg['train_end'],
               'row_count': int(agg['rows']), 'status_counts': status_counts, 'usable_ratio': round(usable, 6),
               'catalog_generation': tx.base_generation, 'source_run_id': tx.original.get('run_id'),
               'catalog_path': f"{system_paths(root)['log']}/{tx.base_generation:020d}.json",
               'dataset_partitions': {k: v for k, v in tx.original['datasets'].get('stock_baseline', {}).items()
                                      if k == valid_date},
               'export_path': export, 'export_format': 'parquet', 'schema_version': cfg['project']['schema_version'],
               'code_version': code_version(), 'published_at': utcnow(), 'status': 'PUBLISHED',
               'previous_version': (current or {}).get('baseline_version')}
    history = f"{system_paths(root)['baselines']}/history/{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}_{version}.json"
    tx.fs.create_exclusive(history, dumps(payload))
    tx.fs.replace(baseline_pointer_path(root), dumps(payload))
    return payload


def export_dataset(spark, cfg, dataset, start=None, end=None, fmt='jsonl', out=None, generation=None,
                   include_content=False):
    """Export a pinned catalog generation for Serving/Elasticsearch with a stable ``_id``."""
    from pyspark.sql import functions as F
    from stock_batch.jobs.pipeline import date_list
    if dataset not in EXPORT_KEYS:
        raise ValueError(f'export supports {sorted(EXPORT_KEYS)}')
    if fmt not in ('jsonl', 'parquet'):
        raise ValueError('format must be jsonl or parquet')
    tx = Transaction(spark, cfg, operation='export', generation=generation)
    keys = tx.partitions(dataset)
    if start or end:
        wanted = set(date_list(start or end, end or start))
        keys = [k for k in keys if k in wanted]
    df = tx.read(dataset, partitions=keys)
    dropped = [] if include_content else [c for c in RESTRICTED_COLUMNS.get(dataset, []) if c in df.columns]
    df = df.drop(*dropped)
    key_cols = EXPORT_KEYS[dataset]
    doc_id = (F.col(key_cols[0]).cast('string') if len(key_cols) == 1 else
              F.substring(F.sha2(F.concat_ws('|', *[F.col(c).cast('string') for c in key_cols]), 256), 1, 40))
    df = df.withColumn('_id', doc_id).withColumn('_catalog_generation', F.lit(tx.base_generation))
    label = f"{start or 'all'}_{end or 'all'}"
    target = out or f'{tx.root}/_system/exports/{dataset}/generation={tx.base_generation}/{label}'
    writer = df.repartition(1).sortWithinPartitions('_id').write.mode('overwrite')
    if fmt == 'jsonl':
        writer.option('timestampFormat', "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").option('ignoreNullFields', 'false').json(target)
    else:
        writer.option('compression', cfg['spark']['compression']).parquet(target)
    manifest = {'dataset': dataset, 'catalog_generation': tx.base_generation, 'source_run_id': tx.original.get('run_id'),
                'partitions': keys, 'rows': spark.read.format('json' if fmt == 'jsonl' else 'parquet').load(target).count(),
                'format': fmt, 'id_fields': key_cols, 'dropped_columns': dropped, 'path': target,
                'exported_at': utcnow()}
    tx.fs.write_json(f'{target}/_export_manifest.json', manifest, overwrite=True)
    return manifest

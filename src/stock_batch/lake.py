"""Read-only access to the lake for other modules (Speed Layer, Serving, notebooks).

``LakeReader`` pins ONE catalog generation at construction; every read resolves immutable paths
from that generation, so a concurrent batch commit can never mix two snapshots in one query.

    reader = LakeReader(spark, data_root='hdfs://namenode:8020/data')
    events = reader.read('historical_events', start='2026-10-01', end='2026-10-07')
    baseline = reader.published_baseline()          # pointer dict or None
    rows = reader.baseline_rows()                   # DataFrame of the published version
"""
from __future__ import annotations

from datetime import date, timedelta

from stock_batch.schemas import empty, get
from stock_batch.storage.catalog import PARTITIONS, load_catalog, system_paths
from stock_batch.storage.fs import FileSystem


class LakeReader:
    def __init__(self, spark, cfg=None, *, data_root=None, generation=None):
        if cfg is None and data_root is None:
            raise ValueError('pass cfg or data_root')
        self.spark = spark
        self.root = (data_root or cfg['project']['data_root']).rstrip('/')
        self.fs = FileSystem(spark, self.root)
        self.catalog = load_catalog(self.fs, self.root, generation=generation)
        self.generation = int(self.catalog['generation'])
        self.run_id = self.catalog.get('run_id')

    def datasets(self):
        return sorted(k for k, v in self.catalog['datasets'].items() if v)

    def partitions(self, name):
        return sorted(self.catalog['datasets'].get(name, {}))

    def stats(self, name):
        return dict(self.catalog.get('stats', {}).get(name, {}))

    def read(self, name, start=None, end=None, partitions=None):
        """DataFrame of ``name`` at the pinned generation, optionally limited to a date range."""
        if name not in PARTITIONS:
            raise ValueError(f'unknown dataset {name}')
        parts = self.catalog['datasets'].get(name, {})
        keys = sorted(parts) if partitions is None else [k for k in partitions if k in parts]
        if start or end:
            lo, hi = date.fromisoformat(start or end), date.fromisoformat(end or start)
            wanted = {(lo + timedelta(days=i)).isoformat() for i in range((hi - lo).days + 1)}
            keys = [k for k in keys if k in wanted]
        if not keys:
            return empty(self.spark, name)
        return self.spark.read.schema(get(name)).parquet(*[parts[k] for k in keys])

    def kafka_offsets(self):
        return dict(self.catalog.get('kafka_offsets', {}))

    def published_baseline(self):
        """The Speed-Layer baseline pointer (contracts/gold/speed_baseline.schema.json) or None."""
        return self.fs.read_json(f"{system_paths(self.root)['baselines']}/current.json")

    def baseline_rows(self, version=None):
        """Rows of the published (or a given exported) baseline version; never the live Gold table."""
        if version is None:
            pointer = self.published_baseline()
            if not pointer:
                raise LookupError('no baseline has been published (Speed Layer must use NO_BASELINE)')
            path = pointer['export_path']
        else:
            path = f"{system_paths(self.root)['baselines']}/versions/{version}"
            if not self.fs.exists(path):
                raise LookupError(f'baseline version {version} was never published')
        return self.spark.read.parquet(path)

"""Lake catalog: immutable partition snapshots published through an append-only commit log.

Layout under ``data_root``::

    <area>/_versions/run_id=<run>/write=<n>/<partition_col>=<value>/part-*.parquet   (immutable)
    _system/catalog/log/<generation:020d>.json   one immutable file per commit (source of truth)
    _system/catalog/current.json                 copy of the newest commit (for legacy readers)
    _system/runs/run_id=<run>/manifest.json      RUNNING -> COMMITTING -> SUCCESS | FAILED

A transaction remembers the generation it started from (``base_generation``) and commits by
creating ``log/<base+1>.json`` with an exclusive atomic create (compare-and-swap). If another
writer committed first, the create fails and :class:`CommitConflictError` is raised instead of
silently discarding the other writer's work (ADR-13). Readers resolve the newest log entry, so a
missing or stale ``current.json`` never makes the lake look empty. A legacy ``current.json``
without a log is adopted as generation 0.

Every commit stores, per dataset partition, the row count and an order-independent business
fingerprint computed by reading the materialised Parquet back; ``verify`` recomputes them.
"""
from __future__ import annotations

import copy
import os
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from time import monotonic

from stock_batch.schemas import conform, empty, get
from stock_batch.storage.fs import FileSystem, dumps  # noqa: F401  (FileSystem is public API)

PARTITIONS = {
    'bronze_stock': 'ingest_date', 'bronze_news': 'ingest_date', 'bronze_bad_records': 'ingest_date',
    'stock_price': 'trade_date', 'news': 'publish_date', 'news_stock_mapping': 'publish_date',
    'stock_features': 'trade_date', 'news_density': 'event_date',
    'stock_baseline': 'baseline_date', 'historical_events': 'event_date',
    'event_analysis': 'event_date', 'quarantine': 'record_date',
}
AREAS = {
    'bronze_stock': 'bronze/stock', 'bronze_news': 'bronze/news',
    'bronze_bad_records': 'bronze/bad_records',
    'stock_price': 'silver/stock_price', 'news': 'silver/news',
    'news_stock_mapping': 'silver/news_stock_mapping',
    'stock_features': 'gold/stock_features', 'news_density': 'gold/news_density',
    'stock_baseline': 'gold/stock_baseline', 'historical_events': 'gold/historical_events',
    'event_analysis': 'gold/event_analysis', 'quarantine': '_system/quarantine',
}
# Physical sort inside each partition file: stable files, better compression and min/max pruning.
SORT_KEYS = {
    'bronze_stock': ['topic', 'partition', 'offset'], 'bronze_news': ['topic', 'partition', 'offset'],
    'bronze_bad_records': ['topic', 'partition', 'offset'],
    'stock_price': ['symbol', 'bar_start'], 'news': ['article_id'],
    'news_stock_mapping': ['article_id', 'symbol'], 'stock_features': ['symbol', 'window_start'],
    'news_density': ['symbol', 'window_size', 'window_end'],
    'stock_baseline': ['symbol', 'metric', 'window_size', 'session_id', 'time_bucket'],
    'historical_events': ['symbol', 'event_time'], 'event_analysis': ['event_id', 'horizon'],
    'quarantine': ['dataset', 'reason_code', 'source_record_key'],
}
GOLD = {'stock_features', 'news_density', 'stock_baseline', 'historical_events', 'event_analysis'}
# Columns that legitimately differ between identical reruns; excluded from fingerprints.
NON_BUSINESS = {'run_id', 'code_version', 'config_hash', 'processing_timestamp',
                'source_snapshot', 'processed_at', 'ingested_at'}
HELPER = '__lake_partition_key'
NULL_PARTITION = '__NULL__'
FORMAT_VERSION = 2
_LOG_NAME = re.compile(r'^(\d{20})\.json$')


class CatalogError(RuntimeError):
    pass


class CommitConflictError(CatalogError):
    """Another writer published a generation after this transaction started."""


class CatalogCorruptError(CatalogError):
    """Successful runs exist but no catalog generation can be resolved."""


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def partition_key(value):
    if value is None:
        return NULL_PARTITION
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value)


def code_version():
    from stock_batch import __version__
    return os.environ.get('GIT_COMMIT') or __version__


# --------------------------------------------------------------------------- fingerprints
def row_hash(schema):
    """Row hash over business columns: xxhash64 of canonical JSON (doubles rounded, maps sorted)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, FloatType, MapType
    cols = []
    for field in schema.fields:
        if field.name in NON_BUSINESS or field.name == HELPER:
            continue
        c = F.col(field.name)
        if isinstance(field.dataType, (DoubleType, FloatType)):
            c = F.round(c, 9)
        elif isinstance(field.dataType, MapType):
            c = F.array_sort(F.map_entries(c))
        cols.append(c.alias(field.name))
    return F.xxhash64(F.to_json(F.struct(*cols)))


def partition_stats(df, part_col):
    """{partition_key: {'rows': n, 'fingerprint': str}} from ONE aggregation (sum of row hashes)."""
    from pyspark.sql import functions as F
    h = row_hash(df.schema).cast('decimal(38,0)')
    rows = (df.groupBy(F.col(part_col).cast('string').alias('_p'))
              .agg(F.count(F.lit(1)).alias('rows'), F.sum(h).cast('string').alias('fp'))
              .collect())
    return {(r['_p'] if r['_p'] is not None else NULL_PARTITION):
            {'rows': int(r['rows']), 'fingerprint': r['fp'] or '0'} for r in rows}


# --------------------------------------------------------------------------- catalog log
def system_paths(root):
    return {'log': f'{root}/_system/catalog/log', 'pointer': f'{root}/_system/catalog/current.json',
            'runs': f'{root}/_system/runs', 'quality': f'{root}/_system/quality_reports',
            'baselines': f'{root}/_system/baselines'}


def log_path(root, generation):
    return f"{system_paths(root)['log']}/{int(generation):020d}.json"


def manifest_path(root, run_id):
    return f"{system_paths(root)['runs']}/run_id={run_id}/manifest.json"


def list_generations(fs, root):
    return sorted(int(m.group(1)) for n in fs.list_names(system_paths(root)['log'])
                  for m in [_LOG_NAME.match(n)] if m)


def empty_catalog():
    return {'format_version': FORMAT_VERSION, 'generation': 0, 'run_id': None,
            'datasets': {}, 'stats': {}, 'kafka_offsets': {}}


def _normalise(entry):
    entry = dict(entry)
    entry.setdefault('format_version', 1)
    entry.setdefault('generation', 0)
    entry.setdefault('run_id', None)
    entry.setdefault('datasets', {})
    entry.setdefault('stats', {})
    entry.setdefault('kafka_offsets', {})
    return entry


def load_catalog(fs, root, generation=None, strict=True):
    """Resolve the newest (or a given) generation; adopt a legacy pointer as generation 0."""
    gens = list_generations(fs, root)
    if generation is not None:
        generation = int(generation)
        if generation in gens:
            return _normalise(fs.read_json(log_path(root, generation)))
        if generation == 0 and not gens:
            legacy = fs.read_json(system_paths(root)['pointer'])
            if legacy is not None:
                return dict(_normalise(legacy), generation=0)
        raise CatalogError(f'catalog generation {generation} does not exist')
    if gens:
        entry = _normalise(fs.read_json(log_path(root, gens[-1])))
        if int(entry['generation']) != gens[-1]:
            raise CatalogCorruptError(f'log entry {gens[-1]} declares generation {entry["generation"]}')
        return entry
    legacy = fs.read_json(system_paths(root)['pointer'])
    if legacy is not None:
        return dict(_normalise(legacy), generation=0)
    if strict and _has_successful_run(fs, root):
        raise CatalogCorruptError(
            'catalog pointer and commit log are missing although successful runs exist; the lake is '
            'NOT empty. Restore it with: stock-batch rollback --run-id <last SUCCESS run id> '
            '(see list-runs).')
    return empty_catalog()


def _has_successful_run(fs, root):
    runs = system_paths(root)['runs']
    for name in fs.list_names(runs):
        m = fs.read_json(f'{runs}/{name}/manifest.json')
        if m and m.get('status') in ('SUCCESS', 'COMMITTING') and m.get('datasets'):
            return True
    return False


# --------------------------------------------------------------------------- transaction
class Transaction:
    """One unit of publication: stage immutable partitions, then commit one catalog generation."""

    def __init__(self, spark, cfg, run_id=None, *, operation='batch', strict=True, generation=None):
        self.spark = spark
        self.cfg = cfg
        self.root = cfg['project']['data_root'].rstrip('/')
        self.fs = FileSystem(spark, self.root)
        self.run_id = run_id or (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' +
                                 uuid.uuid4().hex[:8])
        self.pointer = system_paths(self.root)['pointer']
        self.original = load_catalog(self.fs, self.root, generation=generation, strict=strict)
        self.base_generation = int(self.original['generation'])
        self.catalog = copy.deepcopy(self.original)
        self.operation = operation
        self.status = None
        self.started_at = utcnow()
        self.writes = []
        self.written = {}            # dataset -> set(partition keys) staged or removed by this tx
        self.jobs = {}
        self.metadata = {}
        self.quality = {}
        self.warnings = []
        self._write_attempts = {}
        self._new_paths = []
        self._job = None
        self._published = False

    # ------------------------------------------------------------------ reading
    def partitions(self, name):
        return sorted(self.catalog['datasets'].get(name, {}))

    def has_dataset(self, name):
        return name in self.catalog['datasets']

    def read(self, name, partitions=None):
        """Read the pinned snapshot of ``name``; ``partitions`` prunes to those partition keys."""
        if name not in PARTITIONS:
            raise ValueError(name)
        values = self.catalog['datasets'].get(name, {})
        if partitions is not None:
            wanted = {partition_key(p) for p in partitions}
            values = {k: v for k, v in values.items() if k in wanted}
        if self._job is not None:
            self.jobs[self._job]['_inputs'].setdefault(name, set()).update(values)
        if not values:
            return empty(self.spark, name)
        return self.spark.read.schema(get(name)).parquet(*[values[k] for k in sorted(values)])

    # ------------------------------------------------------------------ writing
    def write(self, name, df, only_partitions=None, output_files=0, *,
              replace_all=False, preserve_provenance=False):
        """Write ``df`` as new immutable partitions staged in this transaction; return row count.

        * default: partitions present in ``df`` replace the same catalog partitions;
        * ``only_partitions``: exactly these logical partitions are replaced (those without rows
          are removed) and rows outside them are an error;
        * ``replace_all``: the dataset becomes exactly the partitions present in ``df``.

        One file per partition (``repartition`` by partition key, bounded by maxRecordsPerFile),
        sorted by the dataset's business key. ``output_files > 1`` spreads a partition over that
        many files by a deterministic hash of the row (compaction).
        """
        from pyspark.sql import functions as F
        if name not in PARTITIONS:
            raise ValueError(name)
        if replace_all and only_partitions is not None:
            raise ValueError('use either only_partitions or replace_all')
        files = int(output_files or 0)
        if files and not 1 <= files <= 256:
            raise ValueError('output_files must be in 1..256')
        if self.status is None:
            self.record('RUNNING')  # a crash from here on leaves a trace (S10)
        part = PARTITIONS[name]
        attempt = self._write_attempts.get(name, 0) + 1
        self._write_attempts[name] = attempt
        df = conform(df, name)
        if name in GOLD and not preserve_provenance:
            from stock_batch.config import config_hash
            snapshot = f'base_generation={self.base_generation};run_id={self.run_id}'
            df = (df.withColumn('run_id', F.lit(self.run_id))
                    .withColumn('code_version', F.lit(code_version()))
                    .withColumn('config_hash', F.lit(config_hash(self.cfg)))
                    .withColumn('processing_timestamp', F.current_timestamp())
                    .withColumn('source_snapshot', F.concat_ws(
                        '|', F.coalesce(F.col('source_snapshot'), F.lit(name)), F.lit(snapshot))))

        write_df = df.withColumn(HELPER, F.coalesce(F.col(part).cast('string'), F.lit(NULL_PARTITION)))
        if files > 1:
            from pyspark.sql.types import MapType
            hashable = [c.name for c in write_df.schema.fields if not isinstance(c.dataType, MapType)]
            write_df = write_df.repartition(F.col(HELPER), F.pmod(F.xxhash64(*hashable), F.lit(files)))
        else:
            write_df = write_df.repartition(F.col(HELPER))
        write_df = write_df.sortWithinPartitions(HELPER, *SORT_KEYS.get(name, []))

        base = f'{self.root}/{AREAS[name]}/_versions/run_id={self.run_id}/write={attempt}'
        started = monotonic()
        print(f'[STORAGE] BEGIN {name} | run={self.run_id} | write={attempt}', flush=True)
        context = self.spark.sparkContext
        context.setJobGroup(f'{self.run_id}:{name}:{attempt}', f'Snapshot write {name}',
                            interruptOnCancel=True)
        try:
            (write_df.write.mode('errorifexists')
                .option('compression', self.cfg['spark']['compression'])
                .option('maxRecordsPerFile', int(self.cfg.get('storage', {}).get('max_records_per_file',
                                                                                  5_000_000)))
                .partitionBy(HELPER).parquet(base))
        finally:
            context.setLocalProperty('spark.jobGroup.id', None)
            context.setLocalProperty('spark.job.description', None)
            context.setLocalProperty('spark.job.interruptOnCancel', None)

        discovered = {}
        for dirname, source_path in self.fs.child_directories(base):
            if not dirname.startswith(HELPER + '='):
                continue
            value = dirname[len(HELPER) + 1:]
            if not value or '/' in value or '\\' in value:
                raise ValueError('invalid written partition key: ' + value)
            discovered[value] = source_path

        scoped = None if only_partitions is None else {partition_key(v) for v in only_partitions}
        if scoped is not None and not set(discovered) <= scoped:
            raise ValueError(f'rows outside only_partitions for {name}: {sorted(set(discovered) - scoped)}')

        paths = {}
        for value, source_path in sorted(discovered.items()):
            target = f'{base}/{part}={value}'
            self.fs.rename_directory(source_path, target)
            paths[value] = target

        # Read the materialised files back once: proves they are readable and yields per-partition
        # row counts plus business fingerprints for the catalog (S12, NFR-01).
        stats = (partition_stats(self.spark.read.schema(get(name)).parquet(*paths.values()), part)
                 if paths else {})
        if set(stats) != set(paths):
            raise CatalogError(f'{name}: read-back partitions {sorted(stats)} != written {sorted(paths)}')
        row_count = sum(s['rows'] for s in stats.values())

        table = self.catalog['datasets'].setdefault(name, {})
        table_stats = self.catalog['stats'].setdefault(name, {})
        if replace_all:
            removed = set(table) - set(paths)
            table.clear()
            table_stats.clear()
        else:
            removed = {v for v in (scoped or ()) if v in table and v not in paths}
            for value in (scoped or ()):
                table.pop(value, None)
                table_stats.pop(value, None)
        table.update(paths)
        table_stats.update(stats)
        self._new_paths.extend(paths.values())
        self.written.setdefault(name, set()).update(set(paths) | removed)
        if self._job is not None:
            outputs = self.jobs[self._job]['outputs']
            outputs[name] = outputs.get(name, 0) + row_count
        self.writes.append({'dataset': name, 'write_attempt': attempt,
                            'partition_count': len(scoped if scoped is not None else paths),
                            'partitions_written': len(paths), 'partitions_removed': sorted(removed),
                            'row_count': row_count, 'seconds': round(monotonic() - started, 2)})
        print(f'[STORAGE] DONE {name} | partitions={len(paths)} | rows={row_count} | '
              f'elapsed={monotonic() - started:.1f}s', flush=True)
        return row_count

    # ------------------------------------------------------------------ jobs / checkpoints
    @contextmanager
    def job(self, name):
        """Track one batch job: inputs, outputs, duration and a resumable catalog delta."""
        before = copy.deepcopy(self.catalog['datasets'])
        rec = {'status': 'RUNNING', 'started_at': utcnow(), '_inputs': {}, 'outputs': {}}
        self.jobs[name] = rec
        self._job = name
        started = monotonic()
        try:
            yield rec
        except BaseException as exc:
            rec['status'] = 'FAILED'
            rec['error'] = f'{type(exc).__name__}: {exc}'[:2000]
            raise
        else:
            rec['status'] = 'SUCCEEDED'
            delta = {}
            for ds, after in self.catalog['datasets'].items():
                prev = before.get(ds)
                changed = {k: v for k, v in after.items() if (prev or {}).get(k) != v}
                removed = sorted(k for k in (prev or {}) if k not in after)
                if changed or removed or prev is None:
                    delta[ds] = {'set': changed, 'removed': removed,
                                 'stats': {k: self.catalog['stats'].get(ds, {}).get(k) for k in changed}}
            rec['delta'] = delta
        finally:
            self._job = None
            rec['seconds'] = round(monotonic() - started, 2)
            rec['inputs'] = {k: ({'partitions': len(v), 'first': min(v), 'last': max(v)} if v
                                 else {'partitions': 0}) for k, v in rec.pop('_inputs').items()}
            self.record('RUNNING')   # checkpoint: lets resume skip completed jobs after a crash

    def apply_delta(self, delta):
        """Re-apply a completed job's catalog delta from a failed run (resume)."""
        for ds, d in delta.items():
            table = self.catalog['datasets'].setdefault(ds, {})
            stats = self.catalog['stats'].setdefault(ds, {})
            for k in d.get('removed', []):
                table.pop(k, None)
                stats.pop(k, None)
            for k, path in d.get('set', {}).items():
                if not self.fs.exists(path):
                    raise CatalogError(f'cannot resume: staged partition is missing: {path}')
                table[k] = path
                if (d.get('stats') or {}).get(k) is not None:
                    stats[k] = d['stats'][k]
                self._new_paths.append(path)
            self.written.setdefault(ds, set()).update(d.get('set', {}))
            self.written[ds].update(d.get('removed', []))

    # ------------------------------------------------------------------ manifests
    def manifest(self, status, error=None):
        from stock_batch.config import config_hash
        return {'run_id': self.run_id, 'status': status, 'error': error, 'operation': self.operation,
                'timestamp_utc': utcnow(), 'started_at': self.started_at,
                'config_hash': config_hash(self.cfg), 'code_version': code_version(),
                'schema_version': self.cfg['project']['schema_version'],
                'base_generation': self.base_generation,
                'generation': self.catalog.get('generation') if status in ('COMMITTING', 'SUCCESS') else None,
                'parent_run_id': self.original.get('run_id'),
                'writes': self.writes, 'jobs': self.jobs, 'metadata': self.metadata,
                'quality': self.quality, 'warnings': self.warnings,
                'datasets': self.catalog['datasets'], 'stats': self.catalog['stats'],
                'kafka_offsets': self.catalog.get('kafka_offsets', {})}

    def record(self, status, error=None):
        self.status = status
        self.fs.write_json(manifest_path(self.root, self.run_id), self.manifest(status, error),
                           overwrite=True)

    def begin(self, **metadata):
        self.metadata.update(metadata)
        self.record('RUNNING')
        return self

    def commit(self):
        """Publish generation ``base+1`` atomically, or raise :class:`CommitConflictError`."""
        missing = [p for p in self._new_paths if not self.fs.exists(p)]
        if missing:
            raise CatalogError(f'staged partitions disappeared before commit: {missing[:3]}')
        generation = self.base_generation + 1
        self.catalog.update({'format_version': FORMAT_VERSION, 'generation': generation,
                             'parent_generation': self.base_generation, 'run_id': self.run_id,
                             'operation': self.operation, 'committed_at': utcnow()})
        self.catalog.setdefault('kafka_offsets', {})
        self.record('COMMITTING')
        if not self.fs.create_exclusive(log_path(self.root, generation), dumps(self.catalog)):
            self.catalog['generation'] = self.base_generation
            raise CommitConflictError(
                f'catalog generation {generation} was published by another writer after this run '
                f'started (base generation {self.base_generation}); nothing from run {self.run_id} '
                'was published. Re-run the job on the new snapshot.')
        self._published = True       # from here on the run IS published, whatever happens next
        try:
            self.fs.replace(self.pointer, dumps(self.catalog))
        except Exception as exc:  # the log entry is authoritative; current.json is a copy
            self.warnings.append(f'current.json refresh failed (log generation {generation} is '
                                 f'authoritative): {exc}')
        self.record('SUCCESS')

    def fail(self, exc):
        if self.status == 'SUCCESS':
            return
        message = f'{type(exc).__name__}: {exc}' if isinstance(exc, BaseException) else str(exc)
        if self._published:
            # The log entry exists, so readers already see this generation: never report FAILED.
            self.warnings.append(f'error after the catalog commit (generation is published): {message}')
            self.record('SUCCESS')
            return
        self.record('FAILED', message)


# --------------------------------------------------------------------------- run helpers
def _root(cfg):
    return cfg['project']['data_root'].rstrip('/')


def get_run(spark, cfg, run_id):
    """Read a run manifest; a COMMITTING manifest is reconciled against the commit log."""
    root = _root(cfg)
    fs = FileSystem(spark, root)
    m = fs.read_json(manifest_path(root, run_id))
    if m and m.get('status') == 'COMMITTING':
        gen = m.get('generation')
        entry = fs.read_json(log_path(root, gen)) if gen is not None else None
        m['status'] = 'SUCCESS' if entry and entry.get('run_id') == run_id else 'FAILED'
        m['reconciled_from'] = 'COMMITTING'
    return m


def list_runs(spark, cfg, limit=20):
    root = _root(cfg)
    fs = FileSystem(spark, root)
    out = []
    for name in reversed(fs.list_names(system_paths(root)['runs'])):
        if not name.startswith('run_id='):
            continue
        m = get_run(spark, cfg, name[len('run_id='):])
        if m:
            row = {k: m.get(k) for k in ('run_id', 'status', 'operation', 'generation',
                                         'base_generation', 'timestamp_utc', 'error')}
            row['jobs'] = {j: v.get('status') for j, v in (m.get('jobs') or {}).items()}
            out.append(row)
        if len(out) >= limit:
            break
    return out


def rollback(spark, cfg, run_id):
    """Publish a NEW generation whose datasets equal those of a previous successful run."""
    target = get_run(spark, cfg, run_id)
    if not target or target.get('status') != 'SUCCESS':
        raise ValueError('not a published successful run: ' + run_id)
    tx = Transaction(spark, cfg, operation='rollback', strict=False)
    missing = [p for parts in target['datasets'].values() for p in parts.values() if not tx.fs.exists(p)]
    if missing:
        raise CatalogError(f'cannot roll back to {run_id}: {len(missing)} partition paths no longer '
                           f'exist (e.g. {missing[0]}); was it garbage-collected?')
    tx.catalog['datasets'] = copy.deepcopy(target['datasets'])
    tx.catalog['stats'] = copy.deepcopy(target.get('stats') or {})
    # Bronze returns to the target state, so must the Kafka offsets committed with it; the next
    # ingest re-reads from there and de-duplicates by (topic, partition, offset).
    tx.catalog['kafka_offsets'] = copy.deepcopy(target.get('kafka_offsets') or {})
    tx.metadata = {'operation': 'rollback', 'target_run_id': run_id,
                   'target_generation': target.get('generation')}
    tx.commit()
    return {'status': 'ROLLED_BACK', 'target_run_id': run_id, 'rollback_run_id': tx.run_id,
            'generation': tx.catalog['generation'], 'datasets': sorted(tx.catalog['datasets']),
            'run_id': run_id}

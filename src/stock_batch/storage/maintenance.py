"""Operational maintenance: verify, storage report, garbage collection and compaction (FR-14)."""
from __future__ import annotations

import time
from urllib.parse import urlparse

from stock_batch.schemas import get
from stock_batch.storage.catalog import (AREAS, PARTITIONS, CatalogError, Transaction, get_run, list_generations,
                                         load_catalog, partition_stats, system_paths)
from stock_batch.storage.fs import FileSystem


def _root(cfg):
    return cfg['project']['data_root'].rstrip('/')


def _norm(path):
    """Compare lake paths independent of scheme/authority (hdfs:///x vs hdfs://nn:8020/x)."""
    return urlparse(path).path.rstrip('/') if '://' in path or path.startswith('file:') else path.rstrip('/')


def resolve_snapshot(spark, cfg, run_id=None, generation=None):
    """Return (label, entry) where entry has datasets/stats; by run manifest or catalog generation."""
    fs = FileSystem(spark, _root(cfg))
    if run_id:
        m = get_run(spark, cfg, run_id)
        if not m:
            raise ValueError('unknown run_id ' + run_id)
        return m, {'datasets': m.get('datasets') or {}, 'stats': m.get('stats') or {},
                   'generation': m.get('generation'), 'run_id': run_id}
    entry = load_catalog(fs, _root(cfg), generation=generation)
    return None, entry


def verify(spark, cfg, run_id=None, generation=None, gates=True):
    """Re-check a snapshot: paths exist, rows + business fingerprints match, gates pass (S12)."""
    from stock_batch.quality import check_dataset
    root = _root(cfg)
    fs = FileSystem(spark, root)
    manifest, entry = resolve_snapshot(spark, cfg, run_id, generation)
    report = {'run_id': run_id or entry.get('run_id'), 'generation': entry.get('generation'),
              'manifest_status': manifest.get('status') if manifest else None,
              'datasets': {}, 'problems': []}
    for name, parts in sorted(entry['datasets'].items()):
        stored = entry['stats'].get(name, {})
        info = {'partitions': len(parts), 'rows': 0, 'missing_paths': [], 'mismatches': [], 'no_stats': 0}
        present = {}
        for key, path in sorted(parts.items()):
            if not fs.exists(path) or not fs.data_files(path):
                info['missing_paths'].append(key)
            else:
                present[key] = path
        if present:
            try:
                df = spark.read.schema(get(name)).parquet(*present.values())
                actual = partition_stats(df, PARTITIONS[name])
                info['rows'] = sum(s['rows'] for s in actual.values())
                for key in present:
                    want, got = stored.get(key), actual.get(key, {'rows': 0, 'fingerprint': '0'})
                    if want is None:
                        info['no_stats'] += 1
                    elif want != got:
                        info['mismatches'].append({'partition': key, 'expected': want, 'actual': got})
                if gates:
                    checks, _ = check_dataset(df, name, cfg)
                    failed = {g: n for g, n in checks if n}
                    if failed:
                        info['gate_failures'] = failed
            except Exception as exc:  # unreadable / corrupted Parquet
                info['read_error'] = f'{type(exc).__name__}: {exc}'[:500]
        for kind in ('missing_paths', 'mismatches', 'gate_failures', 'read_error'):
            if info.get(kind):
                report['problems'].append({'dataset': name, 'kind': kind, 'detail': info[kind]})
        report['datasets'][name] = info
    if 'stock_baseline' in entry['datasets'] and entry['datasets']['stock_baseline']:
        b = spark.read.schema(get('stock_baseline')).parquet(*[p for p in entry['datasets']['stock_baseline'].values()
                                                              if fs.exists(p)])
        report['baseline_leakage_rows'] = b.filter('train_end >= valid_for_trading_date').count()
    gens = list_generations(fs, root)
    pointer = fs.read_json(system_paths(root)['pointer'])
    report['catalog'] = {'newest_generation': gens[-1] if gens else None,
                         'pointer_generation': (pointer or {}).get('generation'),
                         'pointer_in_sync': (not gens) or (pointer or {}).get('generation') == gens[-1]}
    if not report['catalog']['pointer_in_sync']:
        report['warnings'] = ['current.json lags the commit log; readers using LakeReader/Transaction are '
                              'unaffected and the next commit refreshes it']
    report['status'] = 'FAILED' if report['problems'] else 'OK'
    return report


def storage_report(spark, cfg, generation=None):
    """Files, bytes and small files per dataset of the pinned generation, plus unreferenced bytes."""
    root = _root(cfg)
    fs = FileSystem(spark, root)
    entry = load_catalog(fs, root, generation=generation)
    small = int(cfg.get('storage', {}).get('small_file_bytes', 16 * 1024 * 1024))
    out = {'generation': entry.get('generation'), 'small_file_bytes': small, 'datasets': {}}
    for name in sorted(AREAS):
        parts = entry['datasets'].get(name, {})
        files = [(k, size) for k, p in parts.items() for _, size in fs.data_files(p)]
        per_part = {}
        for k, _ in files:
            per_part[k] = per_part.get(k, 0) + 1
        area_bytes = sum(size for _, size in fs.data_files(f'{root}/{AREAS[name]}/_versions'))
        live = sum(size for _, size in files)
        out['datasets'][name] = {'partitions': len(parts), 'files': len(files), 'bytes': live,
                                 'small_files': sum(1 for _, s in files if s < small),
                                 'max_files_per_partition': max(per_part.values()) if per_part else 0,
                                 'unreferenced_bytes': max(area_bytes - live, 0)}
    totals = out['datasets'].values()
    out['totals'] = {k: sum(d[k] for d in totals) for k in ('partitions', 'files', 'bytes', 'small_files',
                                                           'unreferenced_bytes')}
    return out


def _snapshot_paths(datasets):
    return {_norm(p) for parts in (datasets or {}).values() for p in parts.values()}


def gc(spark, cfg, keep_generations=None, min_age_hours=None, keep_success_runs=None, apply=False):
    """Delete unreferenced immutable partition directories. Dry-run unless ``apply``.

    Protected: the newest K catalog generations, SUCCESS runs (all, or the newest
    ``keep_success_runs``) so rollback keeps working, partitions behind published baselines, and
    anything younger than ``min_age_hours`` (in-flight writers).
    """
    root = _root(cfg)
    fs = FileSystem(spark, root)
    scfg = cfg.get('storage', {})
    keep_generations = int(keep_generations or scfg.get('gc_keep_generations', 10))
    min_age_hours = float(scfg.get('gc_min_age_hours', 24) if min_age_hours is None else min_age_hours)
    keep_success_runs = keep_success_runs if keep_success_runs is not None else scfg.get('gc_keep_success_runs')
    protected, reasons = set(), {}
    gens = list_generations(fs, root)
    for g in gens[-keep_generations:]:
        protected |= _snapshot_paths(load_catalog(fs, root, generation=g)['datasets'])
    protected |= _snapshot_paths(load_catalog(fs, root, strict=False)['datasets'])
    reasons['generations'] = gens[-keep_generations:]
    success = []
    for name in fs.list_names(system_paths(root)['runs']):
        m = get_run(spark, cfg, name[len('run_id='):]) if name.startswith('run_id=') else None
        if m and m.get('status') == 'SUCCESS':
            success.append(m)
    success.sort(key=lambda m: m.get('timestamp_utc') or '')
    kept = success if not keep_success_runs else success[-int(keep_success_runs):]
    for m in kept:
        protected |= _snapshot_paths(m.get('datasets'))
    reasons['success_runs_protected'] = len(kept)
    baselines = system_paths(root)['baselines']
    published = [fs.read_json(f'{baselines}/current.json')] + [
        fs.read_json(f'{baselines}/history/{n}') for n in fs.list_names(f'{baselines}/history')]
    for p in published:
        for path in ((p or {}).get('dataset_partitions') or {}).values():
            protected.add(_norm(path))
    now, min_age = time.time(), min_age_hours * 3600
    candidates, total = [], 0
    for name, area in sorted(AREAS.items()):
        versions = f'{root}/{area}/_versions'
        for run_dir, run_path in fs.child_directories(versions):
            for write_dir, write_path in fs.child_directories(run_path):
                for leaf, leaf_path in fs.child_directories(write_path):
                    if _norm(leaf_path) in protected:
                        continue
                    if now - fs.modification_time(leaf_path) < min_age:
                        continue
                    size = sum(s for _, s in fs.data_files(leaf_path))
                    candidates.append({'dataset': name, 'path': leaf_path, 'bytes': size})
                    total += size
    # Staging left behind by failed ingests/publishes (never referenced by any catalog).
    for staging in (f'{root}/_system/ingest_staging', f'{baselines}/_staging'):
        for _, path in fs.child_directories(staging):
            if now - fs.modification_time(path) >= min_age:
                size = sum(s for _, s in fs.data_files(path))
                candidates.append({'dataset': '_staging', 'path': path, 'bytes': size})
                total += size
    if apply:
        for c in candidates:
            fs.delete(c['path'], recursive=True)
    return {'mode': 'APPLY' if apply else 'DRY_RUN', 'candidates': len(candidates), 'bytes': total,
            'paths': [c['path'] for c in candidates[:200]], 'protected_paths': len(protected),
            'policy': {'keep_generations': keep_generations, 'min_age_hours': min_age_hours,
                       'keep_success_runs': keep_success_runs or 'all'}, 'protected_by': reasons}


def compact(spark, cfg, name, start, end, files=1):
    """Rewrite partitions into ``files`` files each, keeping provenance; commit only if the
    business fingerprints before and after are identical (S6)."""
    from stock_batch.jobs.pipeline import date_list
    if name not in PARTITIONS:
        raise ValueError('unsupported dataset ' + name)
    if not 1 <= int(files) <= 256:
        raise ValueError('files per partition outside 1..256')
    dates = date_list(start, end)
    tx = Transaction(spark, cfg, operation='compact')
    tx.begin(operation='compact', dataset=name, start=start, end=end, files=files)
    try:
        keys = [k for k in tx.partitions(name) if k in set(dates)]
        if not keys:
            raise ValueError(f'{name} has no partitions between {start} and {end}')
        source = tx.read(name, partitions=keys)
        before = partition_stats(source, PARTITIONS[name])
        tx.write(name, source, only_partitions=keys, output_files=int(files), preserve_provenance=True)
        after = {k: tx.catalog['stats'][name][k] for k in keys if k in tx.catalog['stats'][name]}
        if before != after:
            raise CatalogError(f'compaction changed business content of {name}: before={before} after={after}')
        tx.quality['compact'] = {'dataset': name, 'partitions': keys, 'fingerprints_equal': True}
        tx.commit()
        return {'run_id': tx.run_id, 'dataset': name, 'partitions': len(keys), 'generation': tx.catalog['generation']}
    except BaseException as e:
        tx.fail(e)
        raise

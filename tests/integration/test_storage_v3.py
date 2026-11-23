"""Storage v3 regression tests: commit log + CAS, manifest lifecycle, layout, verify, gc, rollback."""
import glob
import os
from datetime import date, datetime, timezone
from urllib.parse import urlparse

import pytest

from stock_batch.schemas import get
from stock_batch.storage.catalog import (CatalogCorruptError, CommitConflictError, Transaction, get_run,
                                         list_generations, rollback, system_paths)
from stock_batch.storage.fs import FileSystem


def _bronze(spark, items, topic='stock-price-topic'):
    t = datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc)
    return spark.createDataFrame([(topic, 0, int(o), t, t, d, 'FPT', {}, b'{}', None, '1.0', 'RAW')
                                  for d, o in items], get('bronze_stock'))


def _local(path):
    return urlparse(path).path


def _commit(spark, cfg, items, name='bronze_stock'):
    tx = Transaction(spark, cfg)
    tx.write(name, _bronze(spark, items))
    tx.commit()
    return tx


class _Crash(BaseException):
    """Simulates the driver process dying (not an Exception, so nothing catches it)."""


# ------------------------------------------------------------------ S1 lost update / CAS
def test_s1_concurrent_commit_conflict_keeps_first_writer(spark, cfg):
    a, b = Transaction(spark, cfg), Transaction(spark, cfg)
    a.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), 1)]))
    b.write('bronze_news', _bronze(spark, [(date(2026, 10, 1), 2)]))
    a.commit()
    with pytest.raises(CommitConflictError):
        b.commit()
    b.fail(RuntimeError('conflict'))
    cur = Transaction(spark, cfg)
    assert cur.original['run_id'] == a.run_id and cur.base_generation == 1
    assert cur.read('bronze_stock').count() == 1
    assert 'bronze_news' not in cur.original['datasets']
    assert get_run(spark, cfg, b.run_id)['status'] == 'FAILED'
    assert list_generations(FileSystem(spark, cfg['project']['data_root']), cfg['project']['data_root']) == [1]


# ------------------------------------------------------------------ S2 missing pointer
def test_s2_missing_pointer_does_not_empty_the_lake(spark, cfg):
    a = _commit(spark, cfg, [(date(2026, 10, 1), 1)])
    root = cfg['project']['data_root']
    os.remove(_local(system_paths(root)['pointer']))
    cur = Transaction(spark, cfg)                       # resolved from the commit log
    assert cur.original['run_id'] == a.run_id and cur.read('bronze_stock').count() == 1


def test_s2_missing_pointer_and_log_with_success_runs_is_corrupt(spark, cfg):
    from stock_batch.jobs.pipeline import execute
    a = _commit(spark, cfg, [(date(2026, 10, 1), 1)])
    root = cfg['project']['data_root']
    os.remove(_local(system_paths(root)['pointer']))
    for f in glob.glob(_local(system_paths(root)['log']) + '/*.json'):
        os.remove(f)
    with pytest.raises(CatalogCorruptError, match='rollback'):
        Transaction(spark, cfg)
    with pytest.raises(CatalogCorruptError):
        execute(spark, cfg, '2026-10-01', '2026-10-01', 'B1')   # never publishes an "empty" lake
    rollback(spark, cfg, a.run_id)                              # documented recovery
    assert Transaction(spark, cfg).read('bronze_stock').count() == 1


# ------------------------------------------------------------------ S9 small files / layout
def test_s9_one_sorted_file_per_partition(spark, cfg):
    items = [(date(2026, 10, d), o) for d in (1, 2, 5) for o in (9, 3, 7, 1, 5)]
    tx = Transaction(spark, cfg)
    tx.write('bronze_stock', _bronze(spark, items).repartition(8))
    tx.commit()
    paths = Transaction(spark, cfg).original['datasets']['bronze_stock']
    for key, path in paths.items():
        files = glob.glob(_local(path) + '/*.parquet')
        assert len(files) == 1, (key, files)
        offsets = [r['offset'] for r in spark.read.parquet(path).collect()]
        assert offsets == sorted(offsets)


def test_write_modes_replace_all_and_scoped(spark, cfg):
    _commit(spark, cfg, [(date(2026, 10, 1), 1), (date(2026, 10, 2), 2)])
    tx = Transaction(spark, cfg)
    tx.write('bronze_stock', _bronze(spark, [(date(2026, 10, 5), 3)]), replace_all=True)
    tx.commit()
    assert Transaction(spark, cfg).partitions('bronze_stock') == ['2026-10-05']
    tx = Transaction(spark, cfg)
    with pytest.raises(ValueError, match='outside only_partitions'):
        tx.write('bronze_stock', _bronze(spark, [(date(2026, 10, 6), 4)]), only_partitions=['2026-10-05'])


# ------------------------------------------------------------------ S10 manifest lifecycle
def test_s10_running_manifest_before_commit_and_reconciliation(spark, cfg):
    tx = Transaction(spark, cfg)
    tx.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), 1)]))
    assert get_run(spark, cfg, tx.run_id)['status'] == 'RUNNING'      # a crash now leaves a trace

    # crash after the log entry, before the pointer: the new generation is authoritative
    fs = tx.fs
    real = fs.replace

    def crash_on_pointer(path, text):
        if path.endswith('current.json'):
            raise _Crash()
        return real(path, text)
    fs.replace = crash_on_pointer
    with pytest.raises(_Crash):
        tx.commit()
    m = get_run(spark, cfg, tx.run_id)
    assert m['status'] == 'SUCCESS' and m['reconciled_from'] == 'COMMITTING'
    assert Transaction(spark, cfg).original['run_id'] == tx.run_id


def test_s10_crash_before_log_entry_reconciles_failed(spark, cfg):
    a = _commit(spark, cfg, [(date(2026, 10, 1), 1)])
    tx = Transaction(spark, cfg)
    tx.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), 99)]))
    tx.fs.create_exclusive = lambda *args: (_ for _ in ()).throw(_Crash())
    with pytest.raises(_Crash):
        tx.commit()
    m = get_run(spark, cfg, tx.run_id)
    assert m['status'] == 'FAILED' and m['reconciled_from'] == 'COMMITTING'
    cur = Transaction(spark, cfg)
    assert cur.original['run_id'] == a.run_id and cur.read('bronze_stock').first()['offset'] == 1


# ------------------------------------------------------------------ S12 verify
def test_s12_verify_detects_missing_and_modified_files(spark, cfg):
    from stock_batch.storage.maintenance import verify
    a = _commit(spark, cfg, [(date(2026, 10, 1), 1), (date(2026, 10, 1), 2), (date(2026, 10, 2), 3)])
    assert verify(spark, cfg, run_id=a.run_id)['status'] == 'OK'
    paths = a.catalog['datasets']['bronze_stock']
    # modify: replace the file of 2026-10-02 with different content (same row count)
    victim = glob.glob(_local(paths['2026-10-02']) + '/*.parquet')[0]
    other = _bronze(spark, [(date(2026, 10, 2), 333)])
    tmp = f"{cfg['project']['data_root']}/_tmp_other"
    other.coalesce(1).write.parquet(tmp)
    os.replace(glob.glob(_local(tmp) + '/*.parquet')[0], victim)
    crc = os.path.join(os.path.dirname(victim), '.' + os.path.basename(victim) + '.crc')
    if os.path.exists(crc):
        os.remove(crc)          # a clean rewrite (no stale local checksum) -> fingerprint mismatch
    # delete: remove the only file of 2026-10-01
    os.remove(glob.glob(_local(paths['2026-10-01']) + '/*.parquet')[0])
    rep = verify(spark, cfg, run_id=a.run_id)
    kinds = {(p['dataset'], p['kind']) for p in rep['problems']}
    assert rep['status'] == 'FAILED'
    assert ('bronze_stock', 'missing_paths') in kinds and ('bronze_stock', 'mismatches') in kinds


def test_fingerprints_are_order_independent_and_ignore_provenance(spark, cfg):
    a = _commit(spark, cfg, [(date(2026, 10, 1), o) for o in (1, 2, 3)])
    b = Transaction(spark, cfg)
    b.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), o) for o in (3, 1, 2)]).repartition(3))
    assert b.catalog['stats']['bronze_stock'] == a.catalog['stats']['bronze_stock']


# ------------------------------------------------------------------ rollback / gc / report
def test_rollback_publishes_new_generation_and_restores_offsets(spark, cfg):
    a = Transaction(spark, cfg)
    a.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), 10)]))
    a.catalog['kafka_offsets'] = {'stock-price-topic': {'0': 11}}
    a.commit()
    b = Transaction(spark, cfg)
    b.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), 20)]))
    b.catalog['kafka_offsets'] = {'stock-price-topic': {'0': 21}}
    b.commit()
    res = rollback(spark, cfg, a.run_id)
    cur = Transaction(spark, cfg)
    assert res['generation'] == 3 and cur.base_generation == 3
    assert cur.read('bronze_stock').first()['offset'] == 10
    assert cur.original['kafka_offsets'] == {'stock-price-topic': {'0': 11}}
    assert get_run(spark, cfg, res['rollback_run_id'])['status'] == 'SUCCESS'


def test_gc_dry_run_then_apply_keeps_rollback_targets(spark, cfg):
    from stock_batch.storage.maintenance import gc, storage_report
    a = _commit(spark, cfg, [(date(2026, 10, 1), 1)])
    _commit(spark, cfg, [(date(2026, 10, 1), 2)])
    failed = Transaction(spark, cfg)
    failed.write('bronze_stock', _bronze(spark, [(date(2026, 10, 1), 3)]))
    failed.fail(RuntimeError('boom'))
    orphan_path = failed.catalog['datasets']['bronze_stock']['2026-10-01']
    dry = gc(spark, cfg, min_age_hours=0)
    assert dry['mode'] == 'DRY_RUN' and orphan_path in dry['paths'] and os.path.exists(_local(orphan_path))
    assert storage_report(spark, cfg)['datasets']['bronze_stock']['unreferenced_bytes'] > 0
    done = gc(spark, cfg, min_age_hours=0, apply=True)
    assert done['candidates'] == dry['candidates'] and not os.path.exists(_local(orphan_path))
    rollback(spark, cfg, a.run_id)                           # SUCCESS runs stay restorable
    assert Transaction(spark, cfg).read('bronze_stock').first()['offset'] == 1
    young = gc(spark, cfg, min_age_hours=1000)
    assert young['candidates'] == 0


def test_storage_report_counts_files_and_small_files(spark, cfg):
    from stock_batch.storage.maintenance import storage_report
    _commit(spark, cfg, [(date(2026, 10, d), o) for d in (1, 2) for o in range(3)])
    rep = storage_report(spark, cfg)['datasets']['bronze_stock']
    assert rep['partitions'] == 2 and rep['files'] == 2 and rep['max_files_per_partition'] == 1
    assert rep['small_files'] == 2 and rep['bytes'] > 0

"""HDFS smoke test (isolated stack only): atomic FS primitives via py4j + full lake lifecycle.

Run inside the `spark` service of docker/verify/docker-compose.verify.yml. Writes ONLY below
hdfs://namenode:8020/stockbatch-smoke-<timestamp>. Prints a JSON summary; exit code 1 on failure.
"""
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession

CHECKS = []


def check(name, ok, detail=None):
    CHECKS.append({'check': name, 'status': 'PASS' if ok else 'FAIL', 'detail': detail})
    print(f"[SMOKE-HDFS] {'PASS' if ok else 'FAIL'} {name} {detail if detail is not None else ''}", flush=True)
    return ok


def main():
    hdfs = os.environ.get('HDFS_ROOT', 'hdfs://namenode:8020')
    if not hdfs.startswith('hdfs://namenode'):
        raise SystemExit('refusing to run outside the isolated verify stack')
    root = f"{hdfs}/stockbatch-smoke-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    spark = (SparkSession.builder.master('local[2]').appName('stockbatch-smoke-hdfs')
             .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
             .config('spark.locality.wait', '0s').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    from stock_batch.config import load_config
    from stock_batch.fixtures import seed_demo
    from stock_batch.jobs.pipeline import execute
    from stock_batch.lake import LakeReader
    from stock_batch.serving import export_dataset, publish_baseline
    from stock_batch.storage.catalog import Transaction, get_run, list_generations, rollback
    from stock_batch.storage.fs import FileSystem
    from stock_batch.storage.maintenance import gc, storage_report, verify

    cfg = load_config('test', root=root)
    fs = FileSystem(spark, root)
    check('filesystem_is_hdfs', not fs.is_local, root)

    # --- atomic primitives on the real NameNode (py4j) ---
    target = f'{root}/_smoke/exclusive.json'
    first, second = fs.create_exclusive(target, 'one'), fs.create_exclusive(target, 'two')
    check('create_exclusive_first_wins', first is True and second is False and fs.read_text(target) == 'one')
    results = []
    contended = f'{root}/_smoke/contended.json'
    threads = [threading.Thread(target=lambda i=i: results.append(fs.create_exclusive(contended, str(i)))) for i in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    check('create_exclusive_single_winner_under_contention', results.count(True) == 1, results)
    pointer = f'{root}/_smoke/pointer.json'
    fs.replace(pointer, 'v1')
    fs.replace(pointer, 'v2')
    check('atomic_replace_overwrite', fs.read_text(pointer) == 'v2' and
          [n for n in fs.list_names(f'{root}/_smoke') if n.startswith('.tmp')] == [])

    # --- lake lifecycle ---
    t0 = time.time()
    seed_run, nstock, nnews = seed_demo(spark, cfg)
    run_id = execute(spark, cfg, '2026-10-01', '2026-10-07', 'all')
    m = get_run(spark, cfg, run_id)
    check('pipeline_success_gates_pass', m['status'] == 'SUCCESS' and m['quality']['gates']['status'] == 'PASS',
          {'seconds': round(time.time() - t0, 1), 'generation': m['generation']})
    rep = verify(spark, cfg, run_id=run_id)
    check('verify_ok', rep['status'] == 'OK', {k: v['rows'] for k, v in rep['datasets'].items()})
    sr = storage_report(spark, cfg)
    check('one_file_per_partition', all(d['max_files_per_partition'] <= 1 for d in sr['datasets'].values()),
          sr['totals'])
    reader = LakeReader(spark, cfg)
    version = reader.read('stock_baseline', '2026-10-07', '2026-10-07').first()['baseline_version']
    pub = publish_baseline(spark, cfg, version)
    check('publish_baseline', pub['status'] == 'PUBLISHED' and reader.baseline_rows().count() == pub['row_count'],
          {'version': version, 'rows': pub['row_count'], 'status_counts': pub['status_counts']})
    ex = export_dataset(spark, cfg, 'historical_events', '2026-10-01', '2026-10-07', 'jsonl')
    check('export_jsonl', ex['rows'] > 0 and ex['catalog_generation'] == reader.generation, ex['path'])

    # --- crash between commit-log entry and pointer refresh on HDFS ---
    real = FileSystem.replace

    class Crash(BaseException):
        pass

    def crash(self, path, text):
        if path.endswith('/_system/catalog/current.json'):
            raise Crash()
        return real(self, path, text)
    FileSystem.replace = crash
    try:
        execute(spark, cfg, '2026-10-07', '2026-10-07', 'B3')
    except Crash:
        pass
    finally:
        FileSystem.replace = real
    gens = list_generations(fs, root)
    stale = fs.read_json(f'{root}/_system/catalog/current.json')['generation']
    check('crash_after_log_is_published', Transaction(spark, cfg).base_generation == gens[-1] and stale == gens[-2],
          {'log': gens[-3:], 'pointer': stale})

    # --- rollback + gc dry-run ---
    rb = rollback(spark, cfg, run_id)
    check('rollback_new_generation', LakeReader(spark, cfg).generation == rb['generation'] and
          fs.read_json(f'{root}/_system/catalog/current.json')['generation'] == rb['generation'], rb['generation'])
    g = gc(spark, cfg, min_age_hours=0)
    check('gc_dry_run_deletes_nothing', g['mode'] == 'DRY_RUN' and verify(spark, cfg, run_id=run_id)['status'] == 'OK',
          {'candidates': g['candidates'], 'bytes': g['bytes']})
    spark.stop()
    failed = [c for c in CHECKS if c['status'] != 'PASS']
    print('SMOKE_HDFS_SUMMARY=' + json.dumps({'root': root, 'passed': len(CHECKS) - len(failed),
                                             'failed': len(failed), 'checks': CHECKS}, default=str))
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())

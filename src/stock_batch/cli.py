"""Entry point: python -m stock_batch.cli ... / stock-batch ..."""
import argparse
import json
import os
import sys

from stock_batch.config import load_config


def spark_session(cfg, enable_hive=False, embedded_metastore=None):
    from pyspark.sql import SparkSession
    master = os.environ.get('SPARK_MASTER', cfg['spark'].get('master', 'local[2]'))
    builder = (SparkSession.builder.appName('stock-batch-layer').master(master)
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', str(cfg['spark']['shuffle_partitions']))
               .config('spark.sql.sources.partitionOverwriteMode', 'dynamic'))
    if master.startswith('local'):
        builder = builder.config('spark.locality.wait', '0s')   # see pipeline.check_local_scheduling
    if enable_hive:
        uri = os.environ.get('HIVE_METASTORE_URIS')
        if embedded_metastore:
            # Embedded Derby metastore for tests/pilots (single process only).
            builder = (builder.config('spark.sql.catalogImplementation', 'hive')
                       .config('spark.sql.warehouse.dir', f'{embedded_metastore}/warehouse')
                       .config('spark.hadoop.javax.jdo.option.ConnectionURL',
                               f'jdbc:derby:;databaseName={embedded_metastore}/metastore_db;create=true'))
        elif uri:
            builder = builder.config('hive.metastore.uris', uri)
        else:
            raise ValueError('set HIVE_METASTORE_URIS (shared metastore) or pass --embedded-metastore DIR')
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def verify(spark, cfg, run_id=None, generation=None, gates=True):
    from stock_batch.storage.maintenance import verify as _verify
    return _verify(spark, cfg, run_id=run_id, generation=generation, gates=gates)


def publish_baseline(spark, cfg, version, allow_older=False):
    from stock_batch.serving import publish_baseline as _publish
    return _publish(spark, cfg, version, allow_older=allow_older)


def compact(spark, cfg, name, start, end, files=1):
    from stock_batch.storage.maintenance import compact as _compact
    return _compact(spark, cfg, name, start, end, files)


def register_hive(spark, cfg, database='stock_batch'):
    """Persistent EXTERNAL tables per catalog generation with partition LOCATIONs + current_* views.

    Points at the immutable partition paths of the newest generation; never rewrites Parquet.
    """
    from stock_batch.lake import LakeReader
    from stock_batch.schemas import get
    from stock_batch.storage.catalog import PARTITIONS
    if not database.replace('_', '').isalnum():
        raise ValueError('invalid database identifier')
    reader = LakeReader(spark, cfg)
    if not reader.run_id:
        raise ValueError('no published catalog to register')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS `{database}`')
    registered = []
    for name, partitions in sorted(reader.catalog['datasets'].items()):
        if name.startswith('bronze_') or name == 'quarantine' or not partitions:
            continue
        part = PARTITIONS[name]
        cols = ', '.join(f'`{f.name}` {f.dataType.simpleString()}' for f in get(name).fields)
        tbl = f'{name}_g{reader.generation}'
        location = f'{reader.root}/_system/hive_external/{tbl}'
        reader.fs.makedirs(location)
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{database}`.`{tbl}` ({cols}) "
                  f"USING PARQUET PARTITIONED BY (`{part}`) LOCATION '{location}'")
        for value, path in sorted(partitions.items()):
            value = '__HIVE_DEFAULT_PARTITION__' if value == '__NULL__' else value
            spark.sql(f"ALTER TABLE `{database}`.`{tbl}` ADD IF NOT EXISTS "
                      f"PARTITION (`{part}`='{value}') LOCATION '{path}'")
        spark.sql(f'CREATE OR REPLACE VIEW `{database}`.`current_{name}` AS SELECT * FROM `{database}`.`{tbl}`')
        registered.append({'dataset': name, 'snapshot_table': tbl, 'partitions': len(partitions)})
    return {'snapshot_generation': reader.generation, 'snapshot_run_id': reader.run_id, 'registered': registered}


def resume(spark, cfg, failed_run_id):
    """Re-run a FAILED batch with a new run id, reusing checkpointed jobs when still safe."""
    from stock_batch.jobs.pipeline import execute
    from stock_batch.storage.catalog import get_run
    m = get_run(spark, cfg, failed_run_id)
    if not m or m.get('status') != 'FAILED' or m.get('operation', 'batch') != 'batch':
        raise ValueError('run is not a FAILED batch execution')
    sel = m.get('metadata', {})
    if not all(k in sel for k in ('job', 'start', 'end')):
        raise ValueError('failure missing job/date metadata; rerun explicitly')
    return execute(spark, cfg, sel['start'], sel['end'], sel['job'], from_job=sel.get('from_job'),
                   resume_manifest=m)


def build_parser():
    p = argparse.ArgumentParser(prog='stock-batch', description='HDFS Stock Batch Layer B1-B7')
    p.add_argument('--env', choices=['dev', 'test', 'prod'], default='dev')
    p.add_argument('--config', default=None)
    p.add_argument('--data-root', default=None)
    sub = p.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-config')
    sub.add_parser('seed-demo')
    run = sub.add_parser('run')
    run.add_argument('--job', default='all', choices=['all'] + [f'B{i}' for i in range(1, 8)])
    run.add_argument('--from-job', default=None, choices=[f'B{i}' for i in range(1, 8)])
    run.add_argument('--start', required=True)
    run.add_argument('--end', required=True)
    v = sub.add_parser('verify')
    v.add_argument('--run-id', default=None)
    v.add_argument('--generation', type=int, default=None)
    v.add_argument('--no-gates', action='store_true')
    pub = sub.add_parser('publish-baseline')
    pub.add_argument('--version', required=True)
    pub.add_argument('--allow-older', action='store_true')
    sub.add_parser('show-baseline')
    rb = sub.add_parser('rollback')
    rb.add_argument('--run-id', required=True)
    hk = sub.add_parser('ingest-kafka')
    hk.add_argument('--brokers', default='kafka:9092')
    hk.add_argument('--max-records-per-partition', type=int, default=None)
    sub.add_parser('show-current')
    lr = sub.add_parser('list-runs')
    lr.add_argument('--limit', type=int, default=20)
    resume_cmd = sub.add_parser('resume')
    resume_cmd.add_argument('--run-id', required=True)
    cm = sub.add_parser('compact')
    cm.add_argument('--dataset', required=True)
    cm.add_argument('--start', required=True)
    cm.add_argument('--end', required=True)
    cm.add_argument('--files', type=int, default=1)
    sr = sub.add_parser('storage-report')
    sr.add_argument('--generation', type=int, default=None)
    g = sub.add_parser('gc')
    g.add_argument('--keep-generations', type=int, default=None)
    g.add_argument('--min-age-hours', type=float, default=None)
    g.add_argument('--keep-success-runs', type=int, default=None)
    g.add_argument('--apply', action='store_true', help='actually delete (default: dry-run)')
    ex = sub.add_parser('export')
    ex.add_argument('--dataset', required=True)
    ex.add_argument('--start', default=None)
    ex.add_argument('--end', default=None)
    ex.add_argument('--format', choices=['jsonl', 'parquet'], default='jsonl')
    ex.add_argument('--out', default=None)
    ex.add_argument('--generation', type=int, default=None)
    ex.add_argument('--include-content', action='store_true', help='include news content_text (licensed sources only)')
    hv = sub.add_parser('register-hive')
    hv.add_argument('--database', default='stock_batch')
    hv.add_argument('--embedded-metastore', default=None, help='directory for an embedded Derby metastore')
    return p


def dispatch(spark, cfg, args):
    cmd = args.command
    if cmd == 'seed-demo':
        from stock_batch.fixtures import seed_demo
        run_id, nstock, nnews = seed_demo(spark, cfg)
        return {'run_id': run_id, 'bronze_stock': nstock, 'bronze_news': nnews}
    if cmd == 'run':
        from stock_batch.jobs.pipeline import execute
        run_id = execute(spark, cfg, args.start, args.end, args.job, from_job=args.from_job)
        from stock_batch.storage.catalog import get_run
        m = get_run(spark, cfg, run_id)
        return {'status': 'SUCCESS', 'run_id': run_id, 'job': args.job, 'from_job': args.from_job,
                'generation': m.get('generation'), 'warnings': m.get('warnings'),
                'stale_downstream': (m.get('metadata') or {}).get('stale_downstream')}
    if cmd == 'verify':
        return verify(spark, cfg, args.run_id, args.generation, gates=not args.no_gates)
    if cmd == 'publish-baseline':
        return publish_baseline(spark, cfg, args.version, args.allow_older)
    if cmd == 'show-baseline':
        from stock_batch.serving import read_published_baseline
        return read_published_baseline(spark, cfg) or {'status': 'NO_BASELINE'}
    if cmd == 'rollback':
        from stock_batch.storage.catalog import rollback
        return rollback(spark, cfg, args.run_id)
    if cmd == 'ingest-kafka':
        from stock_batch.ingest.kafka import ingest_kafka
        from stock_batch.storage.catalog import get_run
        run_id = ingest_kafka(spark, cfg, args.brokers, args.max_records_per_partition)
        m = get_run(spark, cfg, run_id)
        return {'run_id': run_id, 'quality': m.get('quality'), 'kafka_offsets': m.get('kafka_offsets')}
    if cmd == 'show-current':
        from stock_batch.lake import LakeReader
        return LakeReader(spark, cfg).catalog
    if cmd == 'list-runs':
        from stock_batch.storage.catalog import list_runs
        return list_runs(spark, cfg, args.limit)
    if cmd == 'resume':
        return {'run_id': resume(spark, cfg, args.run_id)}
    if cmd == 'compact':
        return compact(spark, cfg, args.dataset, args.start, args.end, args.files)
    if cmd == 'storage-report':
        from stock_batch.storage.maintenance import storage_report
        return storage_report(spark, cfg, args.generation)
    if cmd == 'gc':
        from stock_batch.storage.maintenance import gc
        return gc(spark, cfg, args.keep_generations, args.min_age_hours, args.keep_success_runs, args.apply)
    if cmd == 'export':
        from stock_batch.serving import export_dataset
        return export_dataset(spark, cfg, args.dataset, args.start, args.end, args.format, args.out,
                              args.generation, args.include_content)
    if cmd == 'register-hive':
        return register_hive(spark, cfg, args.database)
    raise AssertionError(cmd)


def main(argv=None):
    args = build_parser().parse_args(argv)
    cfg = load_config(args.env, args.config, args.data_root)
    if args.command == 'validate-config':
        print(json.dumps({'status': 'VALID', 'environment': cfg['project']['environment'],
                          'data_root': cfg['project']['data_root']}, indent=2))
        return 0
    hive = args.command == 'register-hive'
    spark = spark_session(cfg, enable_hive=hive, embedded_metastore=getattr(args, 'embedded_metastore', None))
    try:
        result = dispatch(spark, cfg, args)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        if isinstance(result, dict) and result.get('status') == 'FAILED':
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == '__main__':
    sys.exit(main())

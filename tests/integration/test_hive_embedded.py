"""AC-16 (P1): EXTERNAL Hive tables over the immutable partitions, embedded Derby metastore.

Each Hive step runs in its own Python/JVM subprocess (one SparkContext per JVM; Derby embedded is
single-process), so the shared test SparkSession is untouched.
"""
import json
import os
import subprocess
import sys
import textwrap

from stock_batch.fixtures import seed_demo
from stock_batch.jobs.pipeline import execute
from stock_batch.lake import LakeReader

QUERY = textwrap.dedent('''
    import json, sys
    from pyspark.sql import SparkSession
    meta = sys.argv[1]
    spark = (SparkSession.builder.master('local[1]').config('spark.sql.session.timeZone', 'UTC')
             .config('spark.locality.wait', '0s')
             .config('spark.sql.catalogImplementation', 'hive')
             .config('spark.sql.warehouse.dir', meta + '/warehouse')
             .config('spark.hadoop.javax.jdo.option.ConnectionURL',
                     'jdbc:derby:;databaseName=' + meta + '/metastore_db;create=true')
             .enableHiveSupport().getOrCreate())
    df = spark.sql("SELECT * FROM stock_batch.current_stock_features WHERE trade_date = DATE'2026-10-07'")
    out = {'rows': df.count(), 'files': df.inputFiles(),
           'plan': df._jdf.queryExecution().executedPlan().toString(),
           'tables': [r.tableName for r in spark.sql('SHOW TABLES IN stock_batch').collect()],
           'columns': [f.name for f in df.schema.fields],
           'type': spark.sql('DESCRIBE TABLE EXTENDED stock_batch.' + sys.argv[2]).filter("col_name = 'Type'").first()[1]}
    print('RESULT=' + json.dumps(out))
    spark.stop()
''')


def _run(args, cwd):
    env = dict(os.environ, SPARK_LOCAL_IP='127.0.0.1')
    return subprocess.run([sys.executable, *args], capture_output=True, text=True, timeout=900, cwd=cwd, env=env)


def test_ac16_hive_external_tables_partition_pruning(spark, cfg_factory, tmp_path):
    cfg = cfg_factory('hive')
    seed_demo(spark, cfg)
    execute(spark, cfg, '2026-10-01', '2026-10-07', 'all')
    reader = LakeReader(spark, cfg)
    expected = reader.read('stock_features', '2026-10-07', '2026-10-07').count()
    meta = str(tmp_path / 'metastore')
    reg = _run(['-m', 'stock_batch.cli', '--env', 'test', '--data-root', cfg['project']['data_root'],
                'register-hive', '--database', 'stock_batch', '--embedded-metastore', meta], tmp_path)
    assert reg.returncode == 0, reg.stderr[-4000:]
    registered = json.loads(reg.stdout[reg.stdout.index('{'):])
    assert registered['snapshot_generation'] == reader.generation
    table = f"stock_features_g{reader.generation}"
    assert any(r['snapshot_table'] == table for r in registered['registered'])

    q = _run(['-c', QUERY, meta, table], tmp_path)
    assert q.returncode == 0, q.stderr[-4000:]
    out = json.loads(next(l for l in q.stdout.splitlines() if l.startswith('RESULT='))[len('RESULT='):])
    assert out['rows'] == expected > 0
    assert out['type'] == 'EXTERNAL'
    assert table in out['tables'] and 'current_stock_features' in out['tables']
    target = reader.catalog['datasets']['stock_features']['2026-10-07'].replace('file://', '')
    assert out['files'] and all(target in f for f in out['files'])          # only the pruned partition
    assert 'PartitionFilters: [isnotnull(trade_date' in out['plan'] and '2026-10-07' in out['plan']
    assert out['columns'][-1] == 'trade_date'

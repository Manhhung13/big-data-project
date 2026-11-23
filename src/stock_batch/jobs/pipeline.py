"""Sequential B1-B7 DAG; no Airflow; one catalog commit on overall success.

Each job runs inside ``tx.job(name)``, which checkpoints its catalog delta into the run manifest so
``resume`` can skip completed jobs. Quality gates run on the partitions written by this run before
the commit; any failure leaves the previous generation current (AC-15).
"""
from datetime import date, timedelta

from pyspark.sql import functions as F

from stock_batch import reference as ref
from stock_batch.quality import run_gates
from stock_batch.storage.catalog import Transaction, partition_key
from stock_batch.transforms.baseline import build_baselines
from stock_batch.transforms.density import news_density
from stock_batch.transforms.events import historical_events
from stock_batch.transforms.features import stock_features
from stock_batch.transforms.news import clean_news
from stock_batch.transforms.stock import clean_stock, parse_stock
from stock_batch.transforms.study import event_study

ORDER = ['B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7']
UPSTREAM = {'B1': ['bronze_stock'], 'B2': ['bronze_news'], 'B3': ['stock_price'],
            'B4': ['news', 'news_stock_mapping'], 'B5': ['stock_features', 'news_density'],
            'B6': ['stock_features', 'news_density', 'stock_baseline'],
            'B7': ['historical_events', 'stock_features']}
OUTPUTS = {'B1': ['stock_price'], 'B2': ['news', 'news_stock_mapping'], 'B3': ['stock_features'],
           'B4': ['news_density'], 'B5': ['stock_baseline'], 'B6': ['historical_events'],
           'B7': ['event_analysis']}


class UpstreamMissingError(ValueError):
    pass


def date_list(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError('--start must be <= --end')
    if (last - first).days > 3660:
        raise ValueError('date range exceeds 10 years')
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def check_session_timezone(spark):
    tz = spark.conf.get('spark.sql.session.timeZone')
    if tz != 'UTC':
        raise ValueError(f'spark.sql.session.timeZone must be UTC (got {tz!r}); timestamps are stored '
                         'in UTC and local dates are derived explicitly in Asia/Ho_Chi_Minh')


LOCALITY_WAIT_KEYS = ('spark.locality.wait', 'spark.locality.wait.process',
                      'spark.locality.wait.node', 'spark.locality.wait.rack')


def _is_zero_duration(value):
    """'0', '0s', '0ms', ' 0.0 s' -> True. Unparseable values count as non-zero."""
    num = str(value).strip().lower().rstrip('abcdefghijklmnopqrstuvwxyz').strip()
    try:
        return float(num) == 0
    except ValueError:
        return False


def check_local_scheduling(spark):
    """Refuse to run in Spark local mode unless delay scheduling is disabled.

    ``LocalSchedulerBackend`` has no periodic ``reviveOffers`` (cluster backends revive every 1s).
    When a stage mixes partitions whose preferred host is the driver (shuffle output, cached or
    checkpointed blocks) with partitions preferring another host (HDFS DataNode), the local task
    finishes in milliseconds, the single revive it triggers falls inside ``spark.locality.wait``,
    and the remaining task is never offered again: the stage hangs forever at ``(n + 0) / N``.
    Reproduced on Spark 3.5.9 with HDFS (bronze_bad_records rewrite during ``ingest-kafka``).
    With one executor, locality is meaningless, so ``spark.locality.wait=0s`` costs nothing.
    """
    sc = spark.sparkContext
    if not sc.master.startswith('local'):
        return
    conf = sc.getConf()
    base = conf.get('spark.locality.wait', '3s')
    bad = {k: conf.get(k, base) for k in LOCALITY_WAIT_KEYS if not _is_zero_duration(conf.get(k, base))}
    if bad:
        raise ValueError(f'Spark local mode needs spark.locality.wait=0s (got {bad}); otherwise stages '
                         'mixing HDFS and driver-local partitions hang (see check_local_scheduling)')


def check_session(spark):
    check_session_timezone(spark)
    check_local_scheduling(spark)


def jobs_to_run(job='all', from_job=None):
    if from_job is not None:
        if from_job not in ORDER:
            raise ValueError('unknown --from-job ' + from_job)
        if job not in ('all', from_job):
            raise ValueError('use either --job or --from-job')
        return ORDER[ORDER.index(from_job):]
    if job == 'all':
        return list(ORDER)
    if job not in ORDER:
        raise ValueError('unknown job ' + job)
    return [job]


def replace_quarantine(tx, label, rejects, scope=None):
    """Replace only the quarantine rows of ``label`` (within ``scope`` record dates); keep the rest.

    Only partitions that hold old rows of ``label`` or receive new rows are rewritten, so other
    datasets' rejects keep their immutable paths (S3).
    """
    new_keys = {partition_key(r[0]) for r in rejects.select('record_date').distinct().collect()}
    existing = tx.partitions('quarantine')
    candidates = existing
    if scope is not None:
        scope = {partition_key(d) for d in scope}
        if not new_keys <= scope:
            raise ValueError(f'{label} rejects outside scope: {sorted(new_keys - scope)}')
        candidates = [k for k in existing if k in scope]
    old_keys = set()
    if candidates:
        old_keys = {partition_key(r[0]) for r in tx.read('quarantine', partitions=candidates)
                    .filter(F.col('dataset') == label).select('record_date').distinct().collect()}
    affected = sorted(new_keys | old_keys)
    if not affected:
        return 0
    retained = (tx.read('quarantine', partitions=[k for k in affected if k in existing])
                .filter(F.col('dataset').isNull() | (F.col('dataset') != label)))
    return tx.write('quarantine', retained.unionByName(rejects), only_partitions=affected)


def _require(tx, cfg, job, partitions=None):
    """Fail if an input dataset was never published; with fail_on_empty also if it has no rows in scope."""
    for name in UPSTREAM[job]:
        if not tx.has_dataset(name):
            raise UpstreamMissingError(f'{job}: upstream dataset {name} has never been published; '
                                       f'run the producing job first (or use --job all / --from-job)')
        if cfg['batch'].get('fail_on_empty') and partitions is not None:
            have = set(tx.partitions(name)) & {partition_key(p) for p in partitions}
            if not have:
                raise UpstreamMissingError(f'{job}: {name} has no partitions in the requested range '
                                           f'(batch.fail_on_empty=true)')


def _check_output(cfg, job, rows):
    if cfg['batch'].get('fail_on_empty') and rows == 0:
        raise UpstreamMissingError(f'{job}: produced 0 rows (batch.fail_on_empty=true)')


def _b1(tx, spark, cfg, dates):
    _require(tx, cfg, 'B1')
    raw_stock = tx.read('bronze_stock')
    stock, rejects, classified = clean_stock(raw_stock, spark, cfg, cache_classified=True, dates=dates)
    try:
        counts = {r['ok']: r['n'] for r in classified.groupBy(F.col('reason_code').isNull().alias('ok'))
                  .agg(F.count(F.lit(1)).alias('n')).collect()}
        rows = tx.write('stock_price', stock, only_partitions=dates)
        replace_quarantine(tx, 'stock', rejects, scope=dates)
        # Independent count of in-scope Bronze records: reconciles raw = accepted + rejected (P10).
        raw_in_scope = parse_stock(raw_stock).filter(F.col('record_date').cast('string').isin(*dates)).count()
        tx.quality['B1'] = {'raw_rows': raw_in_scope, 'accepted_rows': counts.get(True, 0),
                            'rejected_rows': counts.get(False, 0), 'written_rows': rows,
                            'bronze_rows_total': raw_stock.count(), 'scope': 'record_date in requested dates'}
        _check_output(cfg, 'B1', rows)
    finally:
        classified.unpersist(blocking=False)


def _b2(tx, spark, cfg):
    _require(tx, cfg, 'B2')
    raw_news = tx.read('bronze_news')
    news, mapping, rejects, classified = clean_news(raw_news, spark, cfg, cache_classified=True)
    try:
        counts = {r['ok']: r['n'] for r in classified.groupBy(F.col('reason_code').isNull().alias('ok'))
                  .agg(F.count(F.lit(1)).alias('n')).collect()}
        # B2 rebuilds the whole news history, so the datasets are replaced as a whole (S5).
        articles = tx.write('news', news, replace_all=True)
        mapped = tx.write('news_stock_mapping', mapping, replace_all=True)
        replace_quarantine(tx, 'news', rejects)
        tx.quality['B2'] = {'raw_rows': raw_news.count(), 'accepted_articles': counts.get(True, 0),
                            'mapped_rows': mapped, 'rejected_rows': counts.get(False, 0),
                            'written_articles': articles}
        _check_output(cfg, 'B2', articles)
    finally:
        classified.unpersist(blocking=False)


def _b3(tx, spark, cfg, dates):
    _require(tx, cfg, 'B3', dates)
    features = stock_features(tx.read('stock_price', partitions=dates), spark, cfg)
    _check_output(cfg, 'B3', tx.write('stock_features', features, only_partitions=dates))


def _b4(tx, spark, cfg, dates):
    _require(tx, cfg, 'B4')
    days = ref.neighbour_days(dates, before=1)    # clock windows may start the previous day
    density = news_density(tx.read('news', partitions=days), tx.read('news_stock_mapping', partitions=days),
                           spark, cfg, dates)
    _check_output(cfg, 'B4', tx.write('news_density', density, only_partitions=dates))


def _b5(tx, spark, cfg, dates):
    trading = ref.trading_days_in(cfg, dates)
    n = int(cfg['baseline']['train_trading_days'])
    history = sorted({d for day in trading for d in ref.previous_trading_dates(cfg, day, n)})
    _require(tx, cfg, 'B5', history or None)
    baseline = build_baselines(tx.read('stock_features', partitions=history),
                               tx.read('news_density', partitions=history), spark, cfg, trading)
    # Scope = every requested date, so legacy non-trading-day partitions are removed (S8).
    _check_output(cfg, 'B5', tx.write('stock_baseline', baseline, only_partitions=dates))


def _b6(tx, spark, cfg, dates):
    _require(tx, cfg, 'B6', dates)
    around = ref.neighbour_days(dates, before=1, after=1)
    events = historical_events(tx.read('stock_features', partitions=dates), tx.read('news_density', partitions=dates),
                               tx.read('stock_baseline', partitions=dates), tx.read('news', partitions=around),
                               tx.read('news_stock_mapping', partitions=around), cfg)
    tx.write('historical_events', events, only_partitions=dates)


def _b7(tx, spark, cfg, dates):
    _require(tx, cfg, 'B7', dates)
    nxt = {ref.next_trading_date(cfg, d) for d in dates} - {None}
    days = sorted(set(ref.trading_days_in(cfg, dates)) | nxt)
    study = event_study(tx.read('historical_events', partitions=dates), tx.read('stock_features', partitions=days),
                        tx.read('news_density', partitions=dates), spark, cfg, days=days)
    tx.write('event_analysis', study, only_partitions=dates)


def stale_downstream(tx, cfg, before_stats):
    """ADR-19: dates whose B3/B4 fingerprint changed make later baselines/events stale."""
    changed = set()
    for name in ('stock_features', 'news_density'):
        old = before_stats.get(name, {})
        for key in tx.written.get(name, ()):
            new = tx.catalog['stats'].get(name, {}).get(key)
            prev = old.get(key)
            if (prev is None) != (new is None) or (prev and new and prev.get('fingerprint') != new.get('fingerprint')):
                changed.add(key)
    if not changed:
        return {}
    n = int(cfg['baseline']['train_trading_days'])
    dates = ref.trading_dates(cfg)
    affected = set()
    for key in changed:
        later = [d for d in dates if d > key][:n]
        affected.update(later)
    out = {}
    for name in ('stock_baseline', 'historical_events', 'event_analysis'):
        stale = sorted(k for k in tx.partitions(name) if k in affected and k not in tx.written.get(name, set()))
        if name == 'event_analysis':  # 1-trading-day horizon of the previous day reads day D
            prev = [d for d in dates if d < min(changed)][-1:]
            stale = sorted(set(stale) | {k for k in prev if k in tx.partitions(name)
                                         and k not in tx.written.get(name, set())})
        if stale:
            out[name] = stale
    return out


def execute(spark, cfg, start, end, job='all', *, from_job=None, resume_manifest=None):
    check_session(spark)
    dates = date_list(start, end)
    jobs = jobs_to_run(job, from_job)
    tx = Transaction(spark, cfg)
    tx.begin(job=job, from_job=from_job, start=start, end=end, jobs=jobs, requested_partitions=dates)
    skipped = []
    if resume_manifest is not None:
        skipped = _restore_checkpoints(tx, cfg, resume_manifest, jobs)
    before_stats = {k: dict(v) for k, v in tx.original.get('stats', {}).items()}
    runners = {'B1': lambda: _b1(tx, spark, cfg, dates), 'B2': lambda: _b2(tx, spark, cfg),
               'B3': lambda: _b3(tx, spark, cfg, dates), 'B4': lambda: _b4(tx, spark, cfg, dates),
               'B5': lambda: _b5(tx, spark, cfg, dates), 'B6': lambda: _b6(tx, spark, cfg, dates),
               'B7': lambda: _b7(tx, spark, cfg, dates)}
    try:
        for j in jobs:
            if j in skipped:
                continue
            print(f'[PIPELINE] {j} start | run={tx.run_id}', flush=True)
            with tx.job(j):
                runners[j]()
        run_gates(tx, cfg)
        stale = stale_downstream(tx, cfg, before_stats)
        if stale:
            tx.metadata['stale_downstream'] = stale
            tx.warnings.append('downstream partitions computed from changed B3/B4 inputs; re-run them: '
                               + ', '.join(f'{k}={v}' for k, v in stale.items()))
            print('[PIPELINE] WARNING stale downstream: ' + str(stale), flush=True)
        tx.commit()
        return tx.run_id
    except BaseException as exc:
        tx.fail(exc)
        raise


def _restore_checkpoints(tx, cfg, manifest, jobs):
    """Re-apply deltas of jobs completed by a failed run when it is still safe to do so."""
    from stock_batch.config import config_hash
    if manifest.get('config_hash') != config_hash(cfg):
        tx.warnings.append('resume: business config changed; re-running every job')
        return []
    if manifest.get('base_generation') != tx.base_generation:
        tx.warnings.append(f"resume: catalog moved from generation {manifest.get('base_generation')} to "
                           f'{tx.base_generation}; completed jobs are re-run on the new snapshot')
        return []
    skipped = []
    for j in jobs:                       # only a contiguous prefix of completed jobs is reusable
        rec = (manifest.get('jobs') or {}).get(j)
        if not rec or rec.get('status') != 'SUCCEEDED' or 'delta' not in rec:
            break
        tx.apply_delta(rec['delta'])
        tx.jobs[j] = dict(rec, resumed_from=manifest['run_id'])
        if j in (manifest.get('quality') or {}):
            tx.quality[j] = manifest['quality'][j]
        skipped.append(j)
    tx.metadata.update({'resumed_from': manifest['run_id'], 'skipped_jobs': skipped})
    return skipped

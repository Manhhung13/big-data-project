"""End-to-end acceptance on synthetic data (2 symbols x 5 trading days, with real price/volume/news
variation so z-scores are non-NULL): seed -> run all -> verify -> publish -> export, plus AC-14
(determinism), AC-15 (fault injection), commit conflicts, resume and BRD 2.3 lineage."""
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import stock_batch.jobs.pipeline as pipeline
import stock_batch.quality as quality
from stock_batch.config import repository_root
from stock_batch.fixtures import seed_demo
from stock_batch.jobs.pipeline import execute
from stock_batch.lake import LakeReader
from stock_batch.storage.catalog import CommitConflictError, Transaction, get_run, list_runs

START, END = '2026-10-01', '2026-10-07'
GOLD = ['stock_price', 'news', 'news_stock_mapping', 'stock_features', 'news_density', 'stock_baseline',
        'historical_events', 'event_analysis', 'quarantine']


class _Crash(BaseException):
    pass


@pytest.fixture(scope='module')
def lake(spark, cfg_factory):
    cfg = cfg_factory('e2e')
    seed_demo(spark, cfg)
    run_id = execute(spark, cfg, START, END, 'all')
    return cfg, run_id


def _stats(cfg, spark, names=GOLD):
    cat = LakeReader(spark, cfg).catalog
    return {n: cat['stats'].get(n) for n in names}


def test_e2e_pipeline_produces_composite_event_and_passes_gates(spark, lake):
    cfg, run_id = lake
    m = get_run(spark, cfg, run_id)
    assert m['status'] == 'SUCCESS' and m['quality']['gates']['status'] == 'PASS'
    assert [m['jobs'][j]['status'] for j in pipeline.ORDER] == ['SUCCEEDED'] * 7
    report = json.loads(Path(cfg['project']['data_root'].replace('file://', '') +
                             f'/_system/quality_reports/run_id={run_id}/report.json').read_text())
    assert report['status'] == 'PASS' and len(report['gates']) > 30
    ev = LakeReader(spark, cfg).read('historical_events', '2026-10-07', '2026-10-07').filter("symbol='FPT'")
    spike = F.lit('2026-10-07 03:35:00').cast('timestamp')            # 10:35 VN, end of the 10:30 bar
    top = ev.filter((F.col('event_time') <= spike) & (F.col('event_end') >= spike)).first()
    assert top['event_type'] == 'COMPOSITE' and top['status'] == 'COMPLETED'
    assert abs(top['z_return']) > 3 and top['z_volume'] > 3
    # quiet-news cohorts have std = 0 (ZERO_STD): z_news is NULL, the flag comes from the P99 rule
    assert top['news_flag'] is True and top['news_state'] == 'ANOMALY'
    assert len(top['related_article_ids']) == 4                       # incl. the syndicated copy
    news = LakeReader(spark, cfg).read('news')
    assert news.filter(F.col('content_text').contains('<')).count() == 0
    dens = LakeReader(spark, cfg).read('news_density', '2026-10-07', '2026-10-07')
    peak = dens.filter("symbol='FPT' AND window_size='30m'").orderBy(F.desc('eligible_article_count')).first()
    assert (peak['news_count'], peak['eligible_article_count']) == (3, 4)


def test_e2e_verify_publish_export(spark, lake, tmp_path):
    from stock_batch.serving import PublishError, export_dataset, publish_baseline
    from stock_batch.storage.maintenance import verify
    cfg, run_id = lake
    assert verify(spark, cfg, run_id=run_id)['status'] == 'OK'
    assert verify(spark, cfg)['status'] == 'OK'
    reader = LakeReader(spark, cfg)
    version = reader.read('stock_baseline', '2026-10-07', '2026-10-07').first()['baseline_version']
    payload = publish_baseline(spark, cfg, version)
    contract = json.loads((repository_root() / 'contracts/gold/speed_baseline.schema.json').read_text())
    assert set(contract['required']) <= set(payload) and payload['status'] == 'PUBLISHED'
    assert payload['train_end'] < payload['valid_for_trading_date'] == '2026-10-07'
    assert payload['catalog_generation'] == reader.generation
    assert reader.baseline_rows().count() == payload['row_count'] == sum(payload['status_counts'].values())
    older = reader.read('stock_baseline', '2026-10-06', '2026-10-06').first()['baseline_version']
    with pytest.raises(PublishError, match='allow-older'):
        publish_baseline(spark, cfg, older)
    out = tmp_path / 'events'
    man = export_dataset(spark, cfg, 'historical_events', START, END, 'jsonl', out.as_uri())
    docs = [json.loads(l) for f in out.glob('part-*.json') for l in f.read_text().splitlines()]
    assert len(docs) == man['rows'] > 0 and all(d['_id'] == d['event_id'] for d in docs)
    assert man['catalog_generation'] == reader.generation
    n_out = tmp_path / 'news'
    nman = export_dataset(spark, cfg, 'news', fmt='parquet', out=n_out.as_uri())
    assert 'content_text' not in spark.read.parquet(n_out.as_uri()).columns and nman['dropped_columns'] == ['content_text']


def test_ac14_identical_rerun_and_other_root_are_deterministic(spark, lake, cfg_factory):
    cfg, run_id = lake
    before = _stats(cfg, spark)
    execute(spark, cfg, START, END, 'all')
    assert _stats(cfg, spark) == before                                  # same business fingerprints
    other = cfg_factory('e2e_other_root')
    seed_demo(spark, other)
    execute(spark, other, START, END, 'all')
    a, b = LakeReader(spark, cfg), LakeReader(spark, other)
    versions = lambda r: sorted({x[0] for x in r.read('stock_baseline').select('baseline_version').collect()})
    ids = lambda r: sorted(x[0] for x in r.read('historical_events').select('event_id').collect())
    assert versions(a) == versions(b) and ids(a) == ids(b) and len(ids(a)) > 0
    assert _stats(other, spark) == before                                # different data_root, same results


def test_brd23_event_traces_to_bronze_offsets(spark, lake):
    cfg, _ = lake
    r = LakeReader(spark, cfg)
    ev = (r.read('historical_events', '2026-10-07', '2026-10-07').filter("event_type='COMPOSITE'").first())
    base = (r.read('stock_baseline', '2026-10-07', '2026-10-07')
            .filter(F.col('baseline_version') == ev['baseline_version']))
    cohort = {r['sample_count'] for r in base.filter(F.col('symbol') == ev['symbol']).collect()}
    assert ev['baseline_sample_count'] in cohort
    feat = r.read('stock_features', '2026-10-07', '2026-10-07').filter(
        (F.col('symbol') == ev['symbol']) & (F.col('window_end') == F.lit(ev['event_time']))).first()
    assert feat is not None and feat['completeness'] == 1.0
    bars = r.read('stock_price', '2026-10-07', '2026-10-07').filter(
        (F.col('symbol') == ev['symbol']) & (F.col('bar_start') >= F.lit(feat['window_start'])) &
        (F.col('bar_start') < F.lit(feat['window_end'])))
    keys = [x['source_record_key'] for x in bars.collect()]
    assert len(keys) == 5
    offsets = [int(k.split(':')[2]) for k in keys]
    bronze = r.read('bronze_stock').filter(F.col('offset').isin(offsets))
    assert bronze.count() == 5
    payload = json.loads(bronze.orderBy('offset').first()['raw_payload'].decode())
    assert payload['symbol'] == ev['symbol']
    news = r.read('news').filter(F.col('article_id').isin(ev['related_article_ids']))
    assert news.count() == len(ev['related_article_ids'])


# ------------------------------------------------------------------ AC-15 fault injection
INJECT = {'B1': 'clean_stock', 'B2': 'clean_news', 'B3': 'stock_features', 'B4': 'news_density',
          'B5': 'build_baselines', 'B6': 'historical_events', 'B7': 'event_study'}


@pytest.fixture
def seeded(spark, cfg):
    seed_demo(spark, cfg)
    return cfg, Transaction(spark, cfg)


@pytest.mark.parametrize('job', pipeline.ORDER)
def test_ac15_failure_in_each_job_keeps_previous_generation(spark, seeded, monkeypatch, job):
    cfg, before = seeded

    def boom(*a, **k):
        raise RuntimeError(f'injected failure in {job}')
    monkeypatch.setattr(pipeline, INJECT[job], boom)
    with pytest.raises(RuntimeError, match='injected'):
        execute(spark, cfg, '2026-10-07', '2026-10-07', 'all')
    cur = Transaction(spark, cfg)
    assert cur.base_generation == before.base_generation and cur.original['run_id'] == before.original['run_id']
    runs = [r for r in list_runs(spark, cfg) if r['operation'] == 'batch']
    assert runs[0]['status'] == 'FAILED' and runs[0]['jobs'][job] == 'FAILED'


def test_ac15_gate_failure_keeps_previous_generation(spark, seeded, monkeypatch):
    cfg, before = seeded
    real = quality.rules
    monkeypatch.setattr(quality, 'rules', lambda c: dict(real(c), stock_features=[('always_bad', '1 = 1')]))
    with pytest.raises(quality.QualityGateError, match='always_bad'):
        execute(spark, cfg, '2026-10-07', '2026-10-07', 'all')
    assert Transaction(spark, cfg).base_generation == before.base_generation


def test_ac15_crash_between_log_and_pointer(spark, seeded, monkeypatch):
    from stock_batch.storage.fs import FileSystem
    from stock_batch.storage.maintenance import verify
    cfg, before = seeded
    real = FileSystem.replace

    def crash(self, path, text):
        if path.endswith('/_system/catalog/current.json'):
            raise _Crash()
        return real(self, path, text)
    monkeypatch.setattr(FileSystem, 'replace', crash)
    with pytest.raises(_Crash):
        execute(spark, cfg, '2026-10-07', '2026-10-07', 'all')
    monkeypatch.setattr(FileSystem, 'replace', real)
    runs = [r for r in list_runs(spark, cfg) if r['operation'] == 'batch']
    assert runs[0]['status'] == 'SUCCESS'                               # COMMITTING reconciled via log
    cur = Transaction(spark, cfg)
    assert cur.base_generation == before.base_generation + 1 and cur.original['run_id'] == runs[0]['run_id']
    rep = verify(spark, cfg, gates=False)
    assert rep['catalog']['pointer_in_sync'] is False and rep['status'] == 'OK'


def test_concurrent_pipeline_commit_conflict(spark, seeded, monkeypatch):
    cfg, _ = seeded
    real = pipeline.event_study
    inner = {}

    def racing(*a, **k):
        monkeypatch.setattr(pipeline, 'event_study', real)
        inner['run'] = execute(spark, cfg, '2026-10-07', '2026-10-07', 'B1')   # another writer commits
        return real(*a, **k)
    monkeypatch.setattr(pipeline, 'event_study', racing)
    with pytest.raises(CommitConflictError):
        execute(spark, cfg, '2026-10-07', '2026-10-07', 'all')
    cur = Transaction(spark, cfg)
    assert cur.original['run_id'] == inner['run'] and cur.read('stock_price').count() == 510
    assert 'stock_features' not in cur.original['datasets']               # loser published nothing


def test_resume_after_b5_failure_reruns_only_b5_to_b7(spark, seeded, monkeypatch, lake):
    from stock_batch.cli import resume
    cfg, _ = seeded
    monkeypatch.setattr(pipeline, 'build_baselines', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('B5 down')))
    with pytest.raises(RuntimeError):
        execute(spark, cfg, START, END, 'all')
    failed = [r for r in list_runs(spark, cfg) if r['operation'] == 'batch'][0]
    monkeypatch.undo()
    new_run = resume(spark, cfg, failed['run_id'])
    m = get_run(spark, cfg, new_run)
    assert m['status'] == 'SUCCESS' and m['metadata']['skipped_jobs'] == ['B1', 'B2', 'B3', 'B4']
    assert all(m['jobs'][j].get('resumed_from') == failed['run_id'] for j in ('B1', 'B2', 'B3', 'B4'))
    assert all('resumed_from' not in m['jobs'][j] and m['jobs'][j]['status'] == 'SUCCEEDED' for j in ('B5', 'B6', 'B7'))
    assert _stats(cfg, spark) == _stats(lake[0], spark)                  # identical to an uninterrupted run

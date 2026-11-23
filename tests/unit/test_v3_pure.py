"""Pure-Python tests (no Spark/JVM): text cleaning, atomic local FS, catalog resolution, config hash."""
import copy
import json
import threading
import unicodedata

import pytest

from stock_batch.config import business_config, config_hash, load_config, merge_defaults, validate_config
from stock_batch.rules import clean_text, signal_state, stable_id, group_event_types, horizon_target
from stock_batch.storage.catalog import (CatalogCorruptError, CatalogError, empty_catalog, list_generations,
                                         load_catalog, log_path, partition_key, system_paths)
from stock_batch.storage.fs import FileSystem, dumps


# ---------------------------------------------------------------- clean_text (P4, P3)
def test_clean_text_removes_html_scripts_and_entities():
    raw = '<div><p>FPT&nbsp;l&atilde;i <b>l&#7899;n</b></p><script>alert(1)</script><style>p{}</style><!-- x --></div>'
    assert clean_text(raw) == 'FPT lãi lớn'


def test_clean_text_keeps_non_tag_angle_brackets_and_collapses_space():
    assert clean_text('P/E < 10   và\n\tROE > 15') == 'P/E < 10 và ROE > 15'


def test_clean_text_normalises_vietnamese_to_nfc():
    nfd = unicodedata.normalize('NFD', 'Hòa Phát')
    assert nfd != 'Hòa Phát'
    assert clean_text(nfd) == 'Hòa Phát'
    assert unicodedata.is_normalized('NFC', clean_text(nfd))


def test_clean_text_blank_is_none():
    assert clean_text(None) is None
    assert clean_text('  <p> </p> ') is None


def test_stable_id_matches_spark_sha2_prefix_contract():
    import hashlib
    assert stable_id('x') == hashlib.sha256(b'x').hexdigest()[:24]


# ---------------------------------------------------------------- B6 / B7 pure rules (P6, P7, ADR-14)
def test_signal_state_distinguishes_unknown_from_normal():
    assert signal_state(5.0, 'SPARSE', 9.0, 1.0, 0.0, 3.0) == 'UNKNOWN_INSUFFICIENT_BASELINE'
    assert signal_state(None, 'READY', None, 1.0, 0.0, 3.0) == 'UNKNOWN_MISSING_DATA'
    assert signal_state(0.0, 'READY', 0.0, 1.0, 0.0, 3.0) == 'NORMAL'
    assert signal_state(-0.5, 'READY', -50.0, 0.05, 0.0, 3.0) == 'ANOMALY'          # crash is anomalous
    assert signal_state(0, 'ZERO_STD', None, 0, 0, 3.0, min_count=1) == 'NORMAL'   # P99=0 is not a burst
    assert signal_state(3, 'ZERO_STD', None, 0, 0, 3.0, min_count=1) == 'ANOMALY'


def test_classify_baseline_treats_float_noise_as_zero_std():
    from stock_batch.rules import classify_baseline
    assert classify_baseline(5, 4e-18, 2, mean=0.3) == 'ZERO_STD'
    assert classify_baseline(5, 0.003, 2, mean=0.001) == 'READY'
    assert classify_baseline(1, 0.003, 2) == 'SPARSE'


def test_event_type_and_session_horizon_rules():
    assert group_event_types(False, True, True) == 'NEWS_VOLUME'
    assert horizon_target(10, 6, 30) == 16
    assert horizon_target(27, 6, 30) is None       # 30m after 11:15 crosses the lunch break


# ---------------------------------------------------------------- local filesystem atomics
def test_create_exclusive_never_overwrites(tmp_path):
    fs = FileSystem(None, tmp_path.as_uri())
    target = f'{tmp_path.as_uri()}/a/b.json'
    assert fs.create_exclusive(target, 'one') is True
    assert fs.create_exclusive(target, 'two') is False
    assert fs.read_text(target) == 'one'
    assert [n for n in fs.list_names(f'{tmp_path.as_uri()}/a') if n.startswith('.tmp')] == []


def test_create_exclusive_has_exactly_one_winner_under_contention(tmp_path):
    fs = FileSystem(None, tmp_path.as_uri())
    target = f'{tmp_path.as_uri()}/log/00000000000000000001.json'
    results = []
    threads = [threading.Thread(target=lambda i=i: results.append(fs.create_exclusive(target, str(i))))
               for i in range(16)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert results.count(True) == 1


def test_replace_is_atomic_and_overwrites(tmp_path):
    fs = FileSystem(None, tmp_path.as_uri())
    target = f'{tmp_path.as_uri()}/p/current.json'
    fs.replace(target, 'v1')
    fs.replace(target, 'v2')
    assert fs.read_text(target) == 'v2'
    assert fs.list_names(f'{tmp_path.as_uri()}/p') == ['current.json']


# ---------------------------------------------------------------- catalog resolution (S1, S2)
def _write_log(fs, root, gen, **extra):
    fs.create_exclusive(log_path(root, gen), dumps(dict(empty_catalog(), generation=gen, **extra)))


def test_load_catalog_prefers_newest_log_over_stale_pointer(tmp_path):
    root = tmp_path.as_uri()
    fs = FileSystem(None, root)
    _write_log(fs, root, 1, run_id='r1')
    _write_log(fs, root, 2, run_id='r2')
    fs.replace(system_paths(root)['pointer'], dumps({'run_id': 'r1', 'datasets': {}}))
    assert list_generations(fs, root) == [1, 2]
    assert load_catalog(fs, root)['run_id'] == 'r2'
    assert load_catalog(fs, root, generation=1)['run_id'] == 'r1'
    with pytest.raises(CatalogError):
        load_catalog(fs, root, generation=7)


def test_load_catalog_adopts_legacy_pointer_as_generation_zero(tmp_path):
    root = tmp_path.as_uri()
    fs = FileSystem(None, root)
    fs.replace(system_paths(root)['pointer'], dumps({'run_id': 'legacy', 'datasets': {'news': {'2026-10-01': 'x'}}}))
    entry = load_catalog(fs, root)
    assert entry['generation'] == 0 and entry['run_id'] == 'legacy' and entry['stats'] == {}


def test_missing_catalog_with_successful_runs_is_corrupt_not_empty(tmp_path):
    root = tmp_path.as_uri()
    fs = FileSystem(None, root)
    assert load_catalog(fs, root)['datasets'] == {}           # a truly new lake is empty
    fs.replace(f"{system_paths(root)['runs']}/run_id=r1/manifest.json",
               dumps({'run_id': 'r1', 'status': 'SUCCESS', 'datasets': {'news': {'2026-10-01': 'x'}}}))
    with pytest.raises(CatalogCorruptError, match='rollback'):
        load_catalog(fs, root)


def test_partition_key_formats():
    from datetime import date
    assert partition_key(None) == '__NULL__'
    assert partition_key(date(2026, 10, 1)) == '2026-10-01'


# ---------------------------------------------------------------- config (S7, P12)
def test_config_hash_ignores_operational_keys():
    a = load_config('test', root='file:///tmp/a')
    b = load_config('test', root='hdfs://namenode:8020/data')
    b['project']['environment'] = 'prod'
    b['spark']['shuffle_partitions'] = 99
    b['storage']['gc_keep_generations'] = 3
    b['batch']['fail_on_empty'] = True
    b['baseline']['publish_min_usable_ratio'] = 1.0
    assert config_hash(a) == config_hash(b)
    assert 'data_root' not in json.dumps(business_config(a))


def test_config_hash_changes_with_business_rules():
    a = load_config('test')
    b = copy.deepcopy(a)
    b['baseline']['z_threshold'] = 2.5
    assert config_hash(a) != config_hash(b)


def test_legacy_publish_require_ready_maps_to_full_ratio():
    cfg = load_config('test')
    cfg['baseline'].pop('publish_min_usable_ratio')
    cfg['baseline']['publish_require_ready'] = True
    assert merge_defaults(cfg)['baseline']['publish_min_usable_ratio'] == 1.0


def test_validation_runs_after_data_root_override(monkeypatch):
    monkeypatch.setenv('DATA_ROOT', '')
    with pytest.raises(ValueError, match='data_root'):
        load_config('test')


@pytest.mark.parametrize('section,key,value', [
    ('baseline', 'publish_min_usable_ratio', 1.5), ('news', 'duplicate_group_policy', 'fuzzy'),
    ('storage', 'gc_keep_generations', 0), ('storage', 'gc_keep_success_runs', 0)])
def test_new_keys_are_validated(section, key, value):
    cfg = load_config('test')
    cfg[section][key] = value
    with pytest.raises(ValueError):
        validate_config(cfg)


def test_symbol_master_overlap_is_rejected(tmp_path):
    from stock_batch.reference import symbol_rows
    p = tmp_path / 'sym.csv'
    p.write_text('symbol,exchange,company_name,aliases,effective_from,effective_to\n'
                 'FPT,HNX,F,FPT,2000-01-01,2026-08-01\nFPT,HOSE,F,FPT,2026-07-01,\n', encoding='utf-8')
    cfg = load_config('test')
    cfg['market']['symbol_file'] = str(p)
    with pytest.raises(ValueError, match='overlapping'):
        symbol_rows(cfg)

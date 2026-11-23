from pathlib import Path
import copy
import pytest
from stock_batch.config import load_config, validate_config,config_hash,repository_root
from stock_batch.fixtures import synthetic_rows


def test_dev_config():
    cfg=load_config('dev')
    assert cfg['project']['timezone']=='Asia/Ho_Chi_Minh'
    assert cfg['market']['feature_minutes']==5


def test_test_config():
    cfg=load_config('test')
    assert cfg['baseline']['min_samples']==2


def test_config_hash_stable():
    cfg=load_config('dev')
    assert config_hash(cfg)==config_hash(copy.deepcopy(cfg))


def test_config_rejects_invalid_window():
    cfg=load_config('dev')
    cfg['news']['windows_minutes']=[5,45]
    with pytest.raises(ValueError):validate_config(cfg)


def test_config_rejects_invalid_units():
    cfg=load_config('dev');cfg['market']['valid_price_units']['VND']=-10
    with pytest.raises(ValueError):validate_config(cfg)


def test_fixture_is_synthetic_and_has_unique_offsets():
    s,n=synthetic_rows()
    assert len(s)>1000 and len(n)>=5
    assert len({(r['topic'],r['partition'],r['offset']) for r in s})==len(s)
    assert any(b'FPT' in r['raw_payload'] for r in n)


def test_calendar_includes_no_weekends_as_active_days():
    text=(repository_root()/'reference'/'trading_calendar.csv').read_text()
    assert '2026-10-03' not in text and '2026-10-04' not in text

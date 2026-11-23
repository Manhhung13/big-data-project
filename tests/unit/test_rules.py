from datetime import datetime, timedelta, timezone
from stock_batch.rules import (canonical_url,stable_id,zscore,classify_baseline,
    percentile_anomaly, trading_bucket, half_open_instant, group_event_types)


def test_canonical_removes_tracking_preserves_relevant_params():
    a='http://WWW.Example.Org/foo/?utm_source=fb&id=3&b=1#frag'
    b='https://example.org/foo?b=1&id=3'
    assert canonical_url(a)==canonical_url(b)=='https://example.org/foo?b=1&id=3'


def test_rejects_bad_urls():
    assert canonical_url('javascript:alert(1)') is None
    assert canonical_url('') is None


def test_hash_repeatable_and_distinct():
    assert stable_id('a')==stable_id('a')
    assert stable_id('a')!=stable_id('b')
    assert len(stable_id('a'))==24


def test_zscore_and_sparse_unknown():
    assert zscore(3,1,1)==2
    assert zscore(3,1,0) is None
    assert zscore(None,1,2) is None
    assert classify_baseline(19,1,20)=='SPARSE'
    assert classify_baseline(20,0,20)=='ZERO_STD'
    assert classify_baseline(20,2,20)=='READY'


def test_percentile_zero_baseline_not_burst():
    assert percentile_anomaly(0,0,0,min_observed=1) is False
    assert percentile_anomaly(3,0,0,min_observed=1) is True
    assert percentile_anomaly(3,4,1,min_observed=1) is False


def test_half_open_boundaries():
    t=datetime(2026,10,1,3,30,tzinfo=timezone.utc)
    assert half_open_instant(t,t+timedelta(minutes=5),5)
    assert not half_open_instant(t+timedelta(minutes=5),t+timedelta(minutes=5),5)


def test_session_aligned_buckets():
    start=datetime(2026,10,1,2,0,tzinfo=timezone.utc)
    assert trading_bucket(start+timedelta(minutes=8),start)==start+timedelta(minutes=5)
    assert trading_bucket(start-timedelta(minutes=1),start) is None


def test_event_type_rules():
    assert group_event_types(True,True,True)=='COMPOSITE'
    assert group_event_types(True,False,False)=='PRICE_ONLY'
    assert group_event_types(False,False,False)=='NONE'

"""Pure deterministic business rules shared with Spark transformations (no Spark import)."""
from __future__ import annotations

import hashlib
import html
import re
import unicodedata
from datetime import timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING = {'utm_source', 'utm_campaign', 'utm_medium', 'utm_term', 'utm_content', 'fbclid', 'gclid',
            'mc_cid', 'mc_eid'}

_DROP_BLOCKS = re.compile(r'<(script|style|noscript|template|iframe)\b[^>]*>.*?</\1\s*>', re.I | re.S)
_COMMENTS = re.compile(r'<!--.*?-->', re.S)
_TAGS = re.compile(r'</?[A-Za-z][A-Za-z0-9:-]*(?:\s[^<>]*)?/?>')
_SPACE = re.compile(r'\s+')


def clean_text(value):
    """HTML or plain text -> NFC plain text with collapsed whitespace; None/blank -> None.

    Drops script/style/noscript/template/iframe blocks and comments, removes tags, unescapes
    entities and normalises Unicode to NFC so Vietnamese text from different sources (NFC vs NFD)
    compares and matches aliases consistently. A '<' that does not start a tag (``P/E < 10``) is
    kept. Used as the reference implementation for B2 (FR-03); Spark 3.5 has no native Unicode
    normalisation or HTML-entity decoding, which is why B2 wraps this function in one UDF.
    """
    if value is None:
        return None
    text = str(value)
    if '<' in text:
        text = _DROP_BLOCKS.sub(' ', text)
        text = _COMMENTS.sub(' ', text)
        text = _TAGS.sub(' ', text)
    text = unicodedata.normalize('NFC', html.unescape(text))
    text = _SPACE.sub(' ', text).strip()
    return text or None


def canonical_url(url):
    """Lower-cased https URL without www., fragment, trailing slash and tracking parameters."""
    if not url or not url.strip():
        return None
    x = urlsplit(url.strip())
    if x.scheme.lower() not in ('http', 'https') or not x.netloc:
        return None
    host = x.netloc.lower().removeprefix('www.')
    path = x.path.rstrip('/') or '/'
    params = sorted((k, v) for k, v in parse_qsl(x.query, keep_blank_values=True) if k.lower() not in TRACKING)
    return urlunsplit(('https', host, path, urlencode(params), ''))


def stable_id(value, length=24):
    """Same value as Spark ``substring(sha2(value, 256), 1, length)``."""
    return hashlib.sha256(str(value).encode('utf-8')).hexdigest()[:length]


def zscore(value, mean, std):
    return (value - mean) / std if value is not None and mean is not None and std is not None and std > 0 else None


ZERO_STD_RELATIVE_EPS = 1e-9


def classify_baseline(n, std, min_samples, mean=0.0):
    """SPARSE below min_samples; ZERO_STD when std is NULL or float-noise small; else READY."""
    if n < min_samples:
        return 'SPARSE'
    if std is None or std <= ZERO_STD_RELATIVE_EPS * max(abs(mean or 0.0), 1.0):
        return 'ZERO_STD'
    return 'READY'


def percentile_anomaly(value, p99, mean, min_observed=1):
    return (value is not None and p99 is not None and mean is not None and
            value >= min_observed and value > mean and value >= p99)


USABLE_BASELINE = ('READY', 'ZERO_STD')


def signal_state(value, status, z, p99, mean, threshold, min_count=None):
    """NORMAL / ANOMALY / UNKNOWN_* for one metric at one bucket (BRD 8.2; mirrored by B6).

    ``min_count is None`` -> price rule: |z| >= threshold or an upper-tail P99 hit with value > 0.
    Otherwise (volume, news): value >= min_count and (z >= threshold or value >= P99 > mean).
    """
    if status not in USABLE_BASELINE:
        return 'UNKNOWN_INSUFFICIENT_BASELINE'
    if value is None:
        return 'UNKNOWN_MISSING_DATA'
    p_hit = p99 is not None and mean is not None and value >= p99 and value > mean
    if min_count is None:
        hit = (z is not None and abs(z) >= threshold) or (p_hit and value > 0)
    else:
        hit = value >= min_count and ((z is not None and z >= threshold) or p_hit)
    return 'ANOMALY' if hit else 'NORMAL'


def trading_bucket(ts, session_start, minutes=5):
    if not (session_start <= ts):
        return None
    n = int((ts - session_start).total_seconds() // (minutes * 60))
    return session_start + timedelta(minutes=n * minutes)


def half_open_instant(ts, end, width_minutes):
    return end - timedelta(minutes=width_minutes) <= ts < end


def group_event_types(price, volume, news):
    names = [name for yes, name in ((price, 'PRICE'), (volume, 'VOLUME'), (news, 'NEWS')) if yes]
    if len(names) == 3:
        return 'COMPOSITE'
    if not names:
        return 'NONE'
    if names == ['VOLUME', 'NEWS']:
        return 'NEWS_VOLUME'
    return '_'.join(names) + ('_ONLY' if len(names) == 1 else '')


def horizon_target(anchor_idx, steps, session_size):
    """Index reached ``steps`` windows after the anchor inside one session, or None (ADR-14)."""
    target = anchor_idx + steps
    return target if 1 <= target <= session_size else None

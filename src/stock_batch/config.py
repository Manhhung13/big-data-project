"""Configuration loading, defaults, validation and deterministic hashing (no Spark dependency)."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path

import yaml

REQUIRED = {
    'project': ('environment', 'timezone', 'data_root', 'schema_version'),
    'market': ('feature_minutes', 'valid_price_units', 'source_priority', 'calendar_file', 'symbol_file'),
    'news': ('windows_minutes', 'mapping_version', 'trusted_only'),
    'baseline': ('train_trading_days', 'min_samples', 'z_threshold', 'formula_version'),
    'batch': ('event_merge_gap_minutes', 'rule_version', 'analysis_version'),
    'spark': ('shuffle_partitions', 'compression'),
}

# Optional keys and their defaults; older config files keep working.
DEFAULTS = {
    'news': {'min_relevance': 0.0, 'duplicate_group_policy': 'content_hash'},
    'baseline': {'min_news_for_burst': 1, 'min_volume_for_percentile': 1, 'version_prefix': 'bl',
                 'publish_min_usable_ratio': 0.8},
    'batch': {'log_level': 'INFO', 'fail_on_empty': False, 'max_driver_rows': 5000},
    'storage': {'max_records_per_file': 5_000_000, 'small_file_bytes': 16 * 1024 * 1024,
                'gc_keep_generations': 10, 'gc_min_age_hours': 24, 'gc_keep_success_runs': None},
    'kafka': {'stock_topic': 'stock-price-topic', 'news_topic': 'news-topic',
              'starting_offsets': 'earliest'},
}

# Keys that change *where/how* the batch runs but never *what* it computes. They are excluded from
# config_hash so the same data in dev/test/prod produces the same baseline_version (AC-14, NFR-01).
OPERATIONAL = {
    'project': {'environment', 'data_root'},
    'baseline': {'publish_require_ready', 'publish_min_usable_ratio'},
    'batch': {'log_level', 'fail_on_empty', 'max_driver_rows'},
}
OPERATIONAL_SECTIONS = {'spark', 'storage', 'kafka'}
DUPLICATE_POLICIES = ('content_hash', 'article')


def repository_root():
    return Path(__file__).resolve().parents[2]


def merge_defaults(cfg):
    base = cfg.setdefault('baseline', {})
    # Legacy key: `publish_require_ready: true` (no explicit ratio) meant "every cohort usable".
    legacy_all_ready = base.get('publish_require_ready') is True and 'publish_min_usable_ratio' not in base
    for section, values in DEFAULTS.items():
        target = cfg.setdefault(section, {})
        if not isinstance(target, dict):
            raise ValueError(f'section {section} must be a mapping')
        for key, value in values.items():
            target.setdefault(key, copy.deepcopy(value))
    if legacy_all_ready:
        base['publish_min_usable_ratio'] = 1.0
    return cfg


def load_config(env='dev', path=None, root=None):
    path = Path(path) if path else repository_root() / 'configs' / env / 'config.yml'
    cfg = yaml.safe_load(path.read_text(encoding='utf-8'))
    if not isinstance(cfg, dict):
        raise ValueError('configuration must be a mapping')
    merge_defaults(cfg)
    resolved_root = root if root is not None else os.environ.get('DATA_ROOT', cfg['project']['data_root'])
    cfg['project']['data_root'] = (resolved_root or '').rstrip('/')
    validate_config(cfg)
    return cfg


def validate_config(cfg):
    if not isinstance(cfg, dict):
        raise ValueError('configuration must be a mapping')
    for section, keys in REQUIRED.items():
        if section not in cfg or not isinstance(cfg[section], dict):
            raise ValueError(f'missing section: {section}')
        for key in keys:
            if key not in cfg[section]:
                raise ValueError(f'missing {section}.{key}')
    if cfg['project']['timezone'] != 'Asia/Ho_Chi_Minh':
        raise ValueError('expected timezone Asia/Ho_Chi_Minh')
    if int(cfg['market']['feature_minutes']) != 5 or set(cfg['news']['windows_minutes']) != {5, 30, 60}:
        raise ValueError('v1 requires 5m features and 5m/30m/1h news windows')
    if int(cfg['baseline']['train_trading_days']) < 1 or int(cfg['baseline']['min_samples']) < 1:
        raise ValueError('invalid baseline sample policy')
    if float(cfg['baseline']['z_threshold']) <= 0:
        raise ValueError('invalid z threshold')
    units = cfg['market']['valid_price_units']
    if not units or any(float(v) <= 0 for v in units.values()):
        raise ValueError('invalid price unit multiplier')
    if not cfg['market']['source_priority']:
        raise ValueError('market.source_priority must not be empty')
    if cfg['spark']['compression'] not in ('snappy', 'gzip'):
        raise ValueError('unsupported compression')
    if not cfg['project']['data_root']:
        raise ValueError('empty data_root')
    base, news, storage = cfg.get('baseline', {}), cfg.get('news', {}), cfg.get('storage', {})
    ratio = base.get('publish_min_usable_ratio', DEFAULTS['baseline']['publish_min_usable_ratio'])
    if not 0.0 <= float(ratio) <= 1.0:
        raise ValueError('baseline.publish_min_usable_ratio must be in [0, 1]')
    if news.get('duplicate_group_policy', 'content_hash') not in DUPLICATE_POLICIES:
        raise ValueError(f'news.duplicate_group_policy must be one of {DUPLICATE_POLICIES}')
    if float(news.get('min_relevance', 0.0)) < 0:
        raise ValueError('news.min_relevance must be >= 0')
    if int(cfg['batch']['event_merge_gap_minutes']) < 0:
        raise ValueError('batch.event_merge_gap_minutes must be >= 0')
    if int(storage.get('max_records_per_file', 1)) < 1:
        raise ValueError('storage.max_records_per_file must be >= 1')
    if int(storage.get('gc_keep_generations', 1)) < 1:
        raise ValueError('storage.gc_keep_generations must be >= 1')
    if float(storage.get('gc_min_age_hours', 0)) < 0:
        raise ValueError('storage.gc_min_age_hours must be >= 0')
    keep_runs = storage.get('gc_keep_success_runs')
    if keep_runs is not None and int(keep_runs) < 1:
        raise ValueError('storage.gc_keep_success_runs must be null (keep all) or >= 1')
    return True


def business_config(cfg):
    """The subset of the configuration that determines computed results."""
    out = {}
    for section, values in cfg.items():
        if section in OPERATIONAL_SECTIONS or not isinstance(values, dict):
            continue
        skip = OPERATIONAL.get(section, set())
        out[section] = {k: v for k, v in values.items() if k not in skip}
    return out


def _reference_digest(cfg):
    """Content digest of the reference files, so changed calendars/aliases change the hash."""
    digest = {}
    for key in ('calendar_file', 'symbol_file'):
        path = repository_root() / cfg['market'][key]
        digest[key] = hashlib.sha256(path.read_bytes()).hexdigest()[:16] if path.exists() else 'missing'
    return digest


def config_hash(cfg):
    """Business-config hash: excludes data_root, environment, spark/storage/kafka and run knobs."""
    payload = {'config': business_config(cfg), 'reference': _reference_digest(cfg)}
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                     default=str).encode()).hexdigest()[:16]

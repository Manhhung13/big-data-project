"""Pre-publish data-quality gates (BRD 8.1 step 3; NFR-03, NFR-05).

Gates run on the partitions a transaction wrote, before the catalog commit. Each dataset is
checked in ONE aggregation (rule violations, NULL keys and key uniqueness via count vs
count-distinct). Any failure raises :class:`QualityGateError`; the run is recorded FAILED and
readers keep the previous generation. The report is written to
``_system/quality_reports/run_id=<run>/report.json``.
"""
from __future__ import annotations

BUSINESS_KEYS = {
    'bronze_stock': ['topic', 'partition', 'offset'],
    'bronze_news': ['topic', 'partition', 'offset'],
    'bronze_bad_records': ['topic', 'partition', 'offset'],
    'stock_price': ['symbol', 'bar_start'],
    'news': ['article_id'],
    'news_stock_mapping': ['article_id', 'symbol', 'mapping_version'],
    'stock_features': ['symbol', 'window_start', 'interval'],
    'news_density': ['symbol', 'window_end', 'window_size'],
    'stock_baseline': ['baseline_version', 'symbol', 'time_bucket', 'session_id', 'metric', 'window_size'],
    'historical_events': ['event_id', 'rule_version'],
    'event_analysis': ['event_id', 'horizon', 'analysis_version'],
    'quarantine': ['dataset', 'source_record_key'],
}

_OHLC_BAD = ('NOT (open > 0 AND high > 0 AND low > 0 AND close > 0 '
             'AND high >= greatest(open, close, low) AND low <= least(open, close, high))')
_FLAG = "coalesce({0}, false)"
_EXPECTED_TYPE = """CASE
  WHEN coalesce(price_flag,false) AND coalesce(volume_flag,false) AND coalesce(news_flag,false) THEN 'COMPOSITE'
  WHEN coalesce(price_flag,false) AND coalesce(volume_flag,false) THEN 'PRICE_VOLUME'
  WHEN coalesce(price_flag,false) AND coalesce(news_flag,false) THEN 'PRICE_NEWS'
  WHEN coalesce(volume_flag,false) AND coalesce(news_flag,false) THEN 'NEWS_VOLUME'
  WHEN coalesce(price_flag,false) THEN 'PRICE_ONLY'
  WHEN coalesce(volume_flag,false) THEN 'VOLUME_ONLY' ELSE 'NEWS_ONLY' END"""


def rules(cfg):
    """{dataset: [(gate_name, SQL predicate that is TRUE for a violating row)]}."""
    min_samples = int(cfg['baseline']['min_samples'])
    return {
        'stock_price': [
            ('ohlc_valid', _OHLC_BAD),
            ('volume_non_negative', 'volume IS NULL OR volume < 0'),
            ('exchange_session_known', 'exchange IS NULL OR session_id IS NULL OR trade_date IS NULL'),
            ('bar_is_one_minute', 'bar_end IS NULL OR bar_end != bar_start + INTERVAL 1 MINUTE'),
            ('lineage_present', 'source_record_key IS NULL'),
        ],
        'news': [
            ('trusted_time_present', "time_quality = 'TRUSTED' AND published_at IS NULL"),
            ('title_present', 'title IS NULL OR length(title) = 0'),
            ('canonical_url_present', 'canonical_url IS NULL'),
            ('html_removed', r"coalesce(title, '') RLIKE '(?i)<\\s*(script|style|p|div|span|br|a)\\b' OR "
                             r"coalesce(content_text, '') RLIKE '(?i)<\\s*(script|style|p|div|span|br|a)\\b'"),
            ('duplicate_group_present', 'duplicate_group IS NULL'),
        ],
        'news_stock_mapping': [('symbol_present', 'symbol IS NULL OR matched_alias IS NULL')],
        'stock_features': [
            ('ohlc_valid', _OHLC_BAD),
            ('completeness_in_range', 'completeness IS NULL OR completeness <= 0 OR completeness > 1'),
            ('bar_count_le_expected', 'bar_count > expected_bar_count'),
            ('return_null_iff_reason', '(return_simple IS NULL) != (return_missing_reason IS NOT NULL)'),
        ],
        'news_density': [
            ('counts_valid', 'news_count IS NULL OR news_count < 0 OR news_count > eligible_article_count'),
            ('window_size_known', "window_size NOT IN ('5m', '30m', '1h')"),
        ],
        'stock_baseline': [
            ('no_future_leakage', 'train_end IS NULL OR train_end >= valid_for_trading_date'),
            ('status_known', "baseline_status NOT IN ('READY', 'SPARSE', 'ZERO_STD')"),
            ('sparse_iff_below_min_samples', f"(baseline_status = 'SPARSE') != (sample_count < {min_samples})"),
            ('ready_has_positive_std', "baseline_status = 'READY' AND NOT (std > 0)"),
            ('zero_std_has_zero_std', "baseline_status = 'ZERO_STD' AND "
                                      "coalesce(std, 0) > 1e-9 * greatest(abs(coalesce(mean, 0)), 1.0)"),
            ('version_present', 'baseline_version IS NULL'),
        ],
        'historical_events': [
            ('time_order', 'event_end < event_time'),
            ('has_anomaly', 'NOT (' + ' OR '.join(_FLAG.format(f) for f in ('price_flag', 'volume_flag', 'news_flag')) + ')'),
            ('type_matches_flags', f'event_type != {_EXPECTED_TYPE}'),
            ('flag_matches_state', " OR ".join(
                f"(({s}_state = 'ANOMALY') != coalesce({s}_flag, false)) OR "
                f"(({s}_state = 'NORMAL') != coalesce(NOT {s}_flag, false))" for s in ('price', 'volume', 'news'))),
        ],
        'event_analysis': [
            ('is_complete_not_null', 'is_complete IS NULL'),
            ('complete_has_return_after', 'is_complete AND return_after IS NULL'),
            ('complete_has_no_reason', 'is_complete AND missing_reason IS NOT NULL'),
            ('incomplete_has_reason', 'NOT is_complete AND missing_reason IS NULL'),
        ],
        'quarantine': [('reason_present', 'reason_code IS NULL OR dataset IS NULL')],
    }


class QualityGateError(ValueError):
    def __init__(self, failures, report):
        self.failures = failures
        self.report = report
        super().__init__('quality gates failed: ' + '; '.join(
            f"{f['dataset']}.{f['gate']}={f['bad_rows']}" for f in failures))


def _scoped_frame(tx, name, partitions=None):
    keys = tx.written.get(name) if partitions is None else partitions
    present = [p for p in (keys or ()) if p in tx.catalog['datasets'].get(name, {})]
    return (tx.read(name, partitions=present), present) if present else (None, [])


def check_dataset(df, name, cfg):
    """One aggregation -> [(gate, bad_rows)] plus the row count."""
    from pyspark.sql import functions as F
    checks = rules(cfg).get(name, [])
    keys = BUSINESS_KEYS.get(name)
    aggs = [F.count(F.lit(1)).alias('__rows')]
    aggs += [F.sum(F.when(F.expr(expr), 1).otherwise(0)).alias(g) for g, expr in checks]
    if keys:
        aggs.append(F.sum(F.when(F.expr(' OR '.join(f'`{k}` IS NULL' for k in keys)), 1).otherwise(0)).alias('__null_key'))
        aggs.append(F.countDistinct(*[F.col(k) for k in keys]).alias('__distinct'))
    if name == 'news_density':
        aggs += [F.sum(F.when(F.col('window_size') == w, 1).otherwise(0)).alias(f'__w_{w}') for w in ('5m', '30m', '1h')]
    row = df.agg(*aggs).first().asDict()
    out = [(g, int(row[g] or 0)) for g, _ in checks]
    if keys:
        out.append(('business_key_not_null', int(row['__null_key'] or 0)))
        out.append(('business_key_unique', int(row['__rows']) - int(row['__null_key'] or 0) - int(row['__distinct'] or 0)))
    return out, row


def run_gates(tx, cfg, write_report=True):
    from stock_batch.reference import trading_grid
    results, failures = [], []

    def add(dataset, gate, bad, detail=None):
        entry = {'dataset': dataset, 'gate': gate, 'bad_rows': int(bad), 'status': 'FAIL' if bad else 'PASS'}
        if detail is not None:
            entry['detail'] = detail
        results.append(entry)
        if bad:
            failures.append(entry)

    for name in sorted(tx.written):
        df, present = _scoped_frame(tx, name)
        if df is None:
            continue
        checks, row = check_dataset(df, name, cfg)
        for gate, bad in checks:
            add(name, gate, bad)
        if name == 'news_density':
            # Zero-fill: every window size has exactly one row per grid point of the written days.
            expected = trading_grid(tx.spark, cfg, present).count()
            counts = {w: int(row[f'__w_{w}'] or 0) for w in ('5m', '30m', '1h')}
            add(name, 'zero_filled_grid_all_windows', sum(abs(c - expected) for c in counts.values()),
                {'expected_per_window': expected, 'actual': counts})

    for job, q in sorted(tx.quality.items()):
        if isinstance(q, dict) and {'raw_rows', 'rejected_rows'} <= set(q):
            accepted = q.get('accepted_rows', q.get('accepted_articles'))
            if accepted is not None:
                add(job, 'raw_equals_accepted_plus_rejected',
                    abs(int(q['raw_rows']) - int(accepted) - int(q['rejected_rows'])),
                    {'raw': q['raw_rows'], 'accepted': accepted, 'rejected': q['rejected_rows']})

    report = {'run_id': tx.run_id, 'status': 'FAIL' if failures else 'PASS', 'gates': results}
    tx.quality['gates'] = {'status': report['status'], 'checked': len(results), 'failed': failures}
    if write_report:
        try:
            tx.fs.write_json(f'{tx.root}/_system/quality_reports/run_id={tx.run_id}/report.json', report,
                             overwrite=True)
        except Exception as exc:  # persisting the report must never mask the verdict
            tx.warnings.append(f'quality report not written: {exc}')
    if failures:
        raise QualityGateError(failures, report)
    return report

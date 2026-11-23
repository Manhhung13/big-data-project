"""B6 — anomaly scoring with NORMAL / ANOMALY / UNKNOWN states and deterministic grouping (FR-08).

Per bucket and metric the state is ANOMALY, NORMAL, UNKNOWN_INSUFFICIENT_BASELINE (baseline
missing or SPARSE) or UNKNOWN_MISSING_DATA (incomplete bar). A NULL z-score is never read as
"not anomalous" (BRD 8.2): at event level a flag is TRUE if any bucket was ANOMALY, FALSE only if
every bucket was NORMAL, and NULL otherwise. Mirrors ``rules.signal_state``.
"""
from pyspark.sql import Window, functions as F

from stock_batch.schemas import conform

SIGNALS = [('price', 'return_simple', '5m'), ('volume', 'volume_sum', '5m'), ('news', 'news_count', '30m')]
STATE_RANK = {'ANOMALY': 3, 'UNKNOWN_INSUFFICIENT_BASELINE': 2, 'UNKNOWN_MISSING_DATA': 1, 'NORMAL': 0}


def _baseline(df, metric, label, window_size):
    return df.filter((F.col('metric') == metric) & (F.col('window_size') == window_size)).select(
        F.col('symbol').alias(f'{label}_bl_symbol'), F.col('valid_for_trading_date').alias(f'{label}_bl_date'),
        F.col('session_id').alias(f'{label}_bl_session'), F.col('time_bucket').alias(f'{label}_bl_bucket'),
        F.col('mean').alias(f'{label}_mean'), F.col('std').alias(f'{label}_std'),
        F.col('p99').alias(f'{label}_p99'), F.col('baseline_status').alias(f'{label}_status'),
        F.col('baseline_version').alias(f'{label}_version'), F.col('train_end').alias(f'{label}_train_end'),
        F.col('sample_count').alias(f'{label}_samples'))


def _state(label, val, threshold, min_count):
    mean, std, p99 = F.col(f'{label}_mean'), F.col(f'{label}_std'), F.col(f'{label}_p99')
    # z only against a READY cohort: ZERO_STD (incl. float-noise std) never divides by ~0.
    z = F.when((F.col(f'{label}_status') == 'READY') & (std > 0) & val.isNotNull(), (val - mean) / std)
    p_hit = F.coalesce((val >= p99) & (val > mean), F.lit(False))
    if min_count is None:   # price: two-sided z, upper-tail percentile with a positive return
        hit = F.coalesce(F.abs(z) >= threshold, F.lit(False)) | (p_hit & (val > 0))
    else:                   # volume / news: one-sided, with a minimum observed count
        hit = (val >= min_count) & (F.coalesce(z >= threshold, F.lit(False)) | p_hit)
    state = (F.when(~F.coalesce(F.col(f'{label}_status').isin('READY', 'ZERO_STD'), F.lit(False)),
                    'UNKNOWN_INSUFFICIENT_BASELINE')
             .when(val.isNull(), 'UNKNOWN_MISSING_DATA')
             .when(hit, 'ANOMALY').otherwise('NORMAL'))
    return z.cast('double'), state


def historical_events(features, density, baseline, news, mapping, cfg):
    f = features.alias('f')
    # Price/volume use 5m features; the news flag uses the explicit 30m density horizon.
    d = density.filter("window_size = '30m'").select(
        F.col('symbol').alias('d_symbol'), F.col('window_end').alias('d_end'), F.col('news_count'))
    x = f.join(d, (F.col('f.symbol') == F.col('d_symbol')) & (F.col('f.window_end') == F.col('d_end')), 'left')
    for label, metric, wsize in SIGNALS:
        b = _baseline(baseline, metric, label, wsize)
        x = x.join(b, (F.col('f.symbol') == b[f'{label}_bl_symbol']) & (F.col('f.trade_date') == b[f'{label}_bl_date']) &
                   (F.col('f.session_id') == b[f'{label}_bl_session']) & (F.col('f.time_bucket') == b[f'{label}_bl_bucket']) &
                   (b[f'{label}_train_end'] < F.col('f.trade_date')), 'left')
    threshold = float(cfg['baseline']['z_threshold'])
    mins = {'price': None, 'volume': int(cfg['baseline']['min_volume_for_percentile']),
            'news': int(cfg['baseline']['min_news_for_burst'])}
    for label, obs, _ in SIGNALS:
        val = F.when(F.col('completeness') == 1.0, F.col(obs)) if label != 'news' else F.col(obs)
        z, state = _state(label, val.cast('double'), threshold, mins[label])
        x = x.withColumn(f'z_{label}', z).withColumn(f'{label}_state_b', state)

    candidates = x.filter(" OR ".join(f"{label}_state_b = 'ANOMALY'" for label, _, _ in SIGNALS)).select(
        F.col('f.symbol').alias('symbol'), F.col('f.window_end').alias('window_end'),
        F.col('f.trade_date').alias('trade_date'),
        *[F.col(f'{label}_state_b') for label, _, _ in SIGNALS],
        F.col('z_price').alias('z_return'), 'z_volume', 'z_news',
        *[F.col(f'{label}_version') for label, _, _ in SIGNALS],
        *[F.col(f'{label}_samples') for label, _, _ in SIGNALS])
    # Group adjacent anomalous buckets of one symbol and trading day (gap <= config, ADR-09).
    w = Window.partitionBy('symbol', 'trade_date').orderBy('window_end')
    gap = int(cfg['batch']['event_merge_gap_minutes']) * 60
    candidates = (candidates.withColumn('_prev', F.lag('window_end').over(w))
                  .withColumn('_new_group', F.when(F.col('_prev').isNull() |
                              ((F.unix_timestamp('window_end') - F.unix_timestamp('_prev')) > gap), 1).otherwise(0))
                  .withColumn('_group', F.sum('_new_group').over(w.rowsBetween(Window.unboundedPreceding,
                                                                                 Window.currentRow))))
    rank = F.create_map(*[x for k, v in STATE_RANK.items() for x in (F.lit(k), F.lit(v))])
    aggs = [F.min('window_end').alias('event_time'), F.max('window_end').alias('event_end'),
            # Keep the signed z-score with the largest magnitude (a -50 crash is not "-10", P6).
            F.max_by('z_return', F.abs('z_return')).alias('z_return'),
            F.max_by('z_volume', F.abs('z_volume')).alias('z_volume'),
            F.max_by('z_news', F.abs('z_news')).alias('z_news'),
            F.least(*[F.min(f'{label}_version') for label, _, _ in SIGNALS]).alias('baseline_version'),
            F.least(*[F.min(f'{label}_samples') for label, _, _ in SIGNALS]).alias('baseline_sample_count')]
    for label, _, _ in SIGNALS:
        aggs.append(F.max_by(F.col(f'{label}_state_b'), F.element_at(rank, F.col(f'{label}_state_b')))
                    .alias(f'{label}_state'))
    # _group restarts per (symbol, trade_date), so the trading day is part of the event key.
    ev = candidates.groupBy('symbol', 'trade_date', '_group').agg(*aggs).withColumnRenamed('trade_date', 'event_date')
    for label, _, _ in SIGNALS:
        st = F.col(f'{label}_state')
        ev = ev.withColumn(f'{label}_flag', F.when(st == 'ANOMALY', True).when(st == 'NORMAL', False))
    p, v, n = (F.coalesce(F.col(c), F.lit(False)) for c in ('price_flag', 'volume_flag', 'news_flag'))
    ev = ev.withColumn('event_type', F.when(p & v & n, 'COMPOSITE').when(p & v, 'PRICE_VOLUME')
                       .when(p & n, 'PRICE_NEWS').when(v & n, 'NEWS_VOLUME')
                       .when(p, 'PRICE_ONLY').when(v, 'VOLUME_ONLY').otherwise('NEWS_ONLY'))
    unknown = F.lit(False)
    for label, _, _ in SIGNALS:
        unknown = unknown | F.col(f'{label}_state').startswith('UNKNOWN')
    ev = (ev.withColumn('rule_version', F.lit(cfg['batch']['rule_version']))
          # Unchanged v1 formula (session time zone is enforced UTC): ids stay stable across versions.
          .withColumn('event_id', F.sha2(F.concat_ws('|', 'symbol', F.col('event_time').cast('string'),
                                                     F.col('rule_version')), 256))
          .withColumn('status', F.when(unknown, 'PARTIAL_BASELINE').otherwise('COMPLETED'))
          .withColumn('source_snapshot', F.lit('gold:stock_features,news_density,stock_baseline')))
    # Related news within [T-30m, T+30m) is retrospective context only, never evidence at T.
    eligible = (news.filter("time_quality = 'TRUSTED'").join(mapping, 'article_id')
                .select(F.col('symbol').alias('nsymbol'), 'article_id', 'published_at').distinct())
    ev = ev.join(eligible, (ev.symbol == eligible.nsymbol) &
                 (eligible.published_at >= F.expr('event_time - INTERVAL 30 MINUTES')) &
                 (eligible.published_at < F.expr('event_time + INTERVAL 30 MINUTES')), 'left')
    keys = ['event_id', 'symbol', 'event_time', 'event_end', 'event_date', 'event_type', 'status',
            'price_flag', 'volume_flag', 'news_flag', 'price_state', 'volume_state', 'news_state',
            'z_return', 'z_volume', 'z_news', 'baseline_version', 'baseline_sample_count',
            'rule_version', 'source_snapshot']
    ev = ev.groupBy(*keys).agg(F.sort_array(F.collect_set('article_id')).alias('related_article_ids'))
    return conform(ev, 'historical_events')

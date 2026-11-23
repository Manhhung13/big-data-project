"""B7 — event study over the trading grid; never fabricates before/after metrics (FR-09).

ADR-14: the intraday horizons (30m = 6 windows, 1h = 12 windows) are measured *inside the
session of the event*. A horizon that would cross the lunch break or the close is incomplete with
``missing_reason = HORIZON_CROSSES_SESSION``. ``1tradingday`` compares the event close with the
last close of the next trading date of the exchange calendar (not +24h).

A horizon is complete only if the event bar, every bar on the before path ``[T-n, T]`` and every
bar on the after path ``(T, T+n]`` are complete and the news density rows exist.
``is_complete`` is never NULL.
"""
from pyspark.sql import Window, functions as F

from stock_batch.reference import calendar, trading_grid
from stock_batch.schemas import conform


def _timeline(spark, cfg, features, days):
    grid = trading_grid(spark, cfg, days)
    f = features.select(F.col('symbol').alias('_fsym'), F.col('window_end').alias('_fend'),
                        F.col('close').cast('double').alias('close_d'), F.col('completeness').alias('complete'))
    t = (grid.join(f, (grid.symbol == f._fsym) & (grid.window_end == f._fend), 'left')
         .withColumn('ok', F.when(F.col('complete') == 1.0, 1).otherwise(0)))
    w = (Window.partitionBy('symbol', 'event_date', 'session_id').orderBy('window_idx')
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    return t.withColumn('cum_ok', F.sum('ok').over(w)).select(
        'symbol', 'exchange', 'event_date', 'session_id', 'window_end', 'window_idx', 'session_windows',
        'close_d', 'ok', 'cum_ok')


def event_study(events, features, density, spark, cfg, days=None):
    minutes = int(cfg['market']['feature_minutes'])
    timeline = _timeline(spark, cfg, features, days)
    a = timeline.select(*[F.col(c).alias('a_' + c) for c in timeline.columns])
    base = events.join(a, (events.symbol == a.a_symbol) & (events.event_time == a.a_window_end), 'left')
    horizons = spark.createDataFrame([('30m', 30 // minutes, 30), ('1h', 60 // minutes, 60), ('1tradingday', 0, 0)],
                                     'horizon string, n int, horizon_minutes int')
    base = base.crossJoin(F.broadcast(horizons))
    intraday = F.col('horizon') != '1tradingday'

    x = timeline.select(*[F.col(c).alias('x_' + c) for c in ('symbol', 'event_date', 'session_id', 'window_idx',
                                                            'close_d', 'cum_ok')])
    y = timeline.select(*[F.col(c).alias('y_' + c) for c in ('symbol', 'event_date', 'session_id', 'window_idx',
                                                            'close_d', 'cum_ok', 'ok')])
    same_session = lambda p: ((F.col(f'{p}_symbol') == F.col('a_symbol')) &
                              (F.col(f'{p}_event_date') == F.col('a_event_date')) &
                              (F.col(f'{p}_session_id') == F.col('a_session_id')))
    base = base.join(x, intraday & same_session('x') & (F.col('x_window_idx') == F.col('a_window_idx') + F.col('n')), 'left')
    base = base.join(y, intraday & same_session('y') & (F.col('y_window_idx') == F.col('a_window_idx') - F.col('n')), 'left')

    on_grid = F.col('a_window_idx').isNotNull()
    anchor_ok = F.coalesce(F.col('a_ok') == 1, F.lit(False))
    after_cross = F.col('a_window_idx') + F.col('n') > F.col('a_session_windows')
    before_cross = F.col('a_window_idx') - F.col('n') < 1
    after_path = F.coalesce((F.col('x_cum_ok') - F.col('a_cum_ok')) == F.col('n'), F.lit(False))
    before_path = F.coalesce(((F.col('a_cum_ok') - F.col('y_cum_ok')) == F.col('n')) & (F.col('y_ok') == 1), F.lit(False))
    after_ok = intraday & on_grid & anchor_ok & ~F.coalesce(after_cross, F.lit(True)) & after_path
    before_ok = intraday & on_grid & anchor_ok & ~F.coalesce(before_cross, F.lit(True)) & before_path
    base = (base.withColumn('_after_ok', after_ok).withColumn('_before_ok', before_ok)
            .withColumn('return_after', F.when(after_ok, F.col('x_close_d') / F.col('a_close_d') - 1))
            .withColumn('return_before', F.when(before_ok, F.col('a_close_d') / F.col('y_close_d') - 1)))

    # 1 trading day: last close of the next trading date of the exchange.
    cal = calendar(spark, cfg).filter('is_trading_day').select('exchange', 'trading_date').distinct()
    nxt = cal.withColumn('_next', F.lead('trading_date').over(Window.partitionBy('exchange').orderBy('trading_date')))
    nxt = nxt.select(F.col('exchange').alias('_nx_exchange'), F.col('trading_date').alias('_nx_date'), '_next')
    last = (timeline.groupBy('symbol', 'event_date')
            .agg(F.max_by(F.struct('close_d', 'ok'), 'window_end').alias('_last'))
            .select(F.col('symbol').alias('_l_symbol'), F.col('event_date').alias('_l_date'),
                    F.col('_last.close_d').alias('_l_close'), F.col('_last.ok').alias('_l_ok')))
    base = base.join(F.broadcast(nxt), (F.col('a_exchange') == F.col('_nx_exchange')) &
                     (F.col('a_event_date') == F.col('_nx_date')), 'left')
    base = base.join(last, (F.col('a_symbol') == F.col('_l_symbol')) & (F.col('_next') == F.col('_l_date')), 'left')
    day_ok = (~intraday) & on_grid & anchor_ok & F.col('_next').isNotNull() & F.coalesce(F.col('_l_ok') == 1, F.lit(False))
    base = (base.withColumn('_day_ok', F.coalesce(day_ok, F.lit(False)))
            .withColumn('return_after', F.when(F.col('_day_ok'), F.col('_l_close') / F.col('a_close_d') - 1)
                        .otherwise(F.col('return_after'))))

    # News counts in clock windows [T-W, T) and [T, T+W) from B4; intraday horizons only.
    dens = density.filter(F.col('window_size').isin('30m', '1h')).select(
        F.col('symbol').alias('_nsym'), F.col('window_end').alias('_nend'), F.col('window_size').alias('_nw'),
        F.col('news_count').alias('_ncount'))
    nb = dens.select(*[F.col(c).alias(c + '_b') for c in dens.columns])
    na = dens.select(*[F.col(c).alias(c + '_a') for c in dens.columns])
    base = base.join(nb, (base.symbol == nb._nsym_b) & (base.event_time == nb._nend_b) & (base.horizon == nb._nw_b), 'left')
    base = base.join(na, (base.symbol == na._nsym_a) & (base.horizon == na._nw_a) &
                     (F.unix_timestamp(na._nend_a) == F.unix_timestamp(base.event_time) + F.col('horizon_minutes') * 60), 'left')
    base = base.withColumn('news_count_before', F.col('_ncount_b')).withColumn('news_count_after', F.col('_ncount_a'))

    reason = (F.when(~on_grid, 'EVENT_NOT_ON_TRADING_GRID')
              .when(~anchor_ok, 'INCOMPLETE_EVENT_BAR')
              .when(~intraday & F.col('_next').isNull(), 'MISSING_NEXT_TRADING_DAY')
              .when(~intraday & ~F.col('_day_ok'), 'MISSING_NEXT_DAY_CLOSE')
              .when(intraday & (after_cross | before_cross), 'HORIZON_CROSSES_SESSION')
              .when(intraday & ~F.col('_after_ok'), 'MISSING_FUTURE_BARS')
              .when(intraday & ~F.col('_before_ok'), 'MISSING_HISTORY_BARS')
              .when(intraday & (F.col('news_count_before').isNull() | F.col('news_count_after').isNull()),
                    'MISSING_NEWS_DENSITY'))
    base = (base.withColumn('missing_reason', reason)
            .withColumn('is_complete', F.col('missing_reason').isNull())
            .withColumn('volume_z_at_event', F.col('z_volume'))
            .withColumn('cooccurrence_type', F.col('event_type'))
            .withColumn('analysis_version', F.lit(cfg['batch']['analysis_version']))
            .withColumn('source_snapshot', F.lit('gold:historical_events,stock_features,news_density')))
    return conform(base, 'event_analysis')

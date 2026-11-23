"""B5 — strictly as-of statistical baselines, Spark ddof=1 (FR-07).

``baseline_version = <prefix>_<valid date>_<business config hash[:8]>_<content hash[:8]>`` is
content-addressed: an identical rerun on identical inputs yields the identical version (AC-14),
and the same data in another environment (different data_root) yields the same version.
"""
from pyspark.sql import Window, functions as F

from stock_batch.config import config_hash
from stock_batch.reference import calendar
from stock_batch.rules import ZERO_STD_RELATIVE_EPS
from stock_batch.schemas import conform

STAT_COLUMNS = ('eval_date', 'symbol', 'exchange', 'session_id', 'time_bucket', 'metric', 'window_size',
                'sample_count', 'mean', 'std', 'median', 'p95', 'p99', 'train_start', 'train_end')


def metric_history(features, density):
    stock = features.filter('completeness = 1.0').selectExpr(
        'symbol', 'exchange', 'trade_date as obs_date', 'session_id', 'time_bucket',
        "stack(2, 'return_simple', cast(return_simple as double), 'volume_sum', cast(volume_sum as double)) "
        'as (metric, metric_value)')
    stock = stock.withColumn('window_size', F.lit('5m')).filter('metric_value IS NOT NULL')
    exchanges = features.select('symbol', 'exchange').distinct()
    news = (density.select('symbol', F.col('event_date').alias('obs_date'), 'session_id', 'time_bucket',
                           F.lit('news_count').alias('metric'),
                           F.col('news_count').cast('double').alias('metric_value'), 'window_size')
            .join(exchanges, 'symbol', 'inner'))
    return stock.unionByName(news)


def build_baselines(features, density, spark, cfg, eval_dates, version_suffix=None):
    """For each *trading* evaluation date use only the N previous trading dates of the exchange.

    ``version_suffix`` is accepted for backward compatibility and ignored (versions are now
    content-addressed instead of run-addressed).
    """
    history = metric_history(features, density)
    cal = calendar(spark, cfg).filter('is_trading_day').select('exchange', 'trading_date').distinct()
    wanted = [str(d) for d in eval_dates]
    targets = (cal.filter(F.col('trading_date').cast('string').isin(*wanted))
               .withColumnRenamed('trading_date', 'eval_date'))           # trading days only (S8)
    candidates = (targets.join(cal.withColumnRenamed('trading_date', 'train_date'), 'exchange')
                  .filter(F.col('train_date') < F.col('eval_date')))
    w = Window.partitionBy('exchange', 'eval_date').orderBy(F.col('train_date').desc())
    train = candidates.withColumn('_rn', F.row_number().over(w)).filter(
        F.col('_rn') <= int(cfg['baseline']['train_trading_days'])).drop('_rn')
    obs = history.join(F.broadcast(train), (history.exchange == train.exchange) &
                       (history.obs_date == train.train_date), 'inner')
    obs = obs.select('eval_date', *[history[c] for c in history.columns])
    keys = ('eval_date', 'symbol', 'exchange', 'session_id', 'time_bucket', 'metric', 'window_size')
    result = obs.groupBy(*keys).agg(
        F.count('metric_value').cast('long').alias('sample_count'),
        F.avg('metric_value').alias('mean'), F.stddev_samp('metric_value').alias('std'),
        F.expr('percentile_approx(metric_value, 0.5, 10000)').cast('double').alias('median'),
        F.expr('percentile_approx(metric_value, 0.95, 10000)').cast('double').alias('p95'),
        F.expr('percentile_approx(metric_value, 0.99, 10000)').cast('double').alias('p99'),
        F.min('obs_date').alias('train_start'), F.max('obs_date').alias('train_end'))
    min_samples = int(cfg['baseline']['min_samples'])
    # Identical samples can yield std ~1e-18 from floating-point error: that is ZERO_STD, not READY.
    zero_std = F.col('std').isNull() | (F.col('std') <= F.lit(ZERO_STD_RELATIVE_EPS) *
                                        F.greatest(F.abs('mean'), F.lit(1.0)))
    result = result.withColumn(
        'baseline_status', F.when(F.col('sample_count') < min_samples, 'SPARSE')
        .when(zero_std, 'ZERO_STD').otherwise('READY'))
    # Content hash per evaluation date over every cohort statistic (order independent).
    row = F.xxhash64(F.to_json(F.struct(*[F.round(c, 9) if c in ('mean', 'std', 'median', 'p95', 'p99')
                                          else F.col(c) for c in STAT_COLUMNS], F.col('baseline_status'))))
    digest = (result.groupBy('eval_date')
              .agg(F.sum(row.cast('decimal(38,0)')).cast('string').alias('_sum'), F.count(F.lit(1)).alias('_n'))
              .select('eval_date', F.substring(F.sha2(F.concat_ws(':', '_sum', '_n'), 256), 1, 8).alias('_content')))
    prefix, cfg8 = cfg['baseline'].get('version_prefix', 'bl'), config_hash(cfg)[:8]
    result = (result.join(F.broadcast(digest), 'eval_date')
              .withColumn('baseline_date', F.col('eval_date'))
              .withColumn('valid_for_trading_date', F.col('eval_date'))
              .withColumn('baseline_version', F.concat_ws('_', F.lit(prefix), F.date_format('eval_date', 'yyyy-MM-dd'),
                                                          F.lit(cfg8), F.col('_content')))
              .withColumn('fallback_level', F.lit('NONE'))
              .withColumn('formula_version', F.lit(cfg['baseline']['formula_version']))
              .withColumn('source_snapshot', F.lit('gold:stock_features,news_density')))
    return conform(result, 'stock_baseline')

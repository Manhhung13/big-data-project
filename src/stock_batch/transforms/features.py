"""B3 — order-safe 1m OHLCV -> 5m, session-respecting returns (FR-05)."""
from pyspark.sql import Window, functions as F

from stock_batch.reference import calendar
from stock_batch.schemas import conform


def stock_features(stock, spark, cfg):
    c = F.broadcast(calendar(spark, cfg).select(
        F.col('exchange').alias('_exchange'), F.col('trading_date').alias('_date'),
        F.col('session_id').alias('_session'), 'start_utc', 'end_utc'))
    s = stock.join(c, (stock['exchange'] == c['_exchange']) & (stock['trade_date'] == c['_date']) &
                   (stock['session_id'] == c['_session']), 'inner')
    size = int(cfg['market']['feature_minutes'])
    s = s.withColumn('_bucket_idx', F.floor((F.unix_timestamp('bar_start') - F.unix_timestamp('start_utc')) / (size * 60)))
    s = s.withColumn('window_start', F.timestamp_seconds(F.unix_timestamp('start_utc') + F.col('_bucket_idx') * (size * 60)))
    s = s.withColumn('window_end', F.expr(f'window_start + INTERVAL {size} MINUTES'))
    b = (s.groupBy('symbol', 'exchange', 'trade_date', 'session_id', 'window_start', 'window_end', 'end_utc')
         .agg(F.min_by('open', 'bar_start').alias('open'), F.max('high').alias('high'),
              F.min('low').alias('low'), F.max_by('close', 'bar_start').alias('close'),
              F.sum('volume').cast('long').alias('volume_sum'), F.count('*').cast('int').alias('bar_count')))
    b = (b.withColumn('expected_bar_count', F.least(F.lit(size),
              ((F.unix_timestamp('end_utc') - F.unix_timestamp('window_start')) / 60).cast('int')))
         .withColumn('completeness', F.col('bar_count') / F.col('expected_bar_count')))
    w = Window.partitionBy('symbol', 'trade_date', 'session_id').orderBy('window_start')
    b = (b.withColumn('_prev_close', F.lag('close').over(w))
         .withColumn('_prev_end', F.lag('window_end').over(w))
         .withColumn('_prev_complete', F.lag('completeness').over(w)))
    valid = ((F.col('completeness') == 1.0) & (F.col('_prev_complete') == 1.0) &
             (F.col('_prev_end') == F.col('window_start')) & (F.col('_prev_close') > 0))
    b = b.withColumn('return_simple', F.when(valid, (F.col('close') / F.col('_prev_close') - 1).cast('double')))
    b = b.withColumn('return_log', F.when(valid, F.log(F.col('close') / F.col('_prev_close'))))
    b = b.withColumn('return_missing_reason', F.when(valid, F.lit(None).cast('string'))
                     .when(F.col('_prev_close').isNull(), 'START_OF_SESSION_OR_GAP')
                     .otherwise('INCOMPLETE_OR_DISCONTINUOUS'))
    b = (b.withColumn('time_bucket', F.date_format(F.from_utc_timestamp('window_start', 'Asia/Ho_Chi_Minh'), 'HH:mm'))
         .withColumn('interval', F.lit('5m'))
         .withColumn('feature_version', F.lit(cfg['baseline']['formula_version']))
         .withColumn('source_snapshot', F.lit('silver:stock_price')))
    return conform(b, 'stock_features')

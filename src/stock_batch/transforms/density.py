"""B4 — 5m evaluation grid, distinct stories in half-open rolling windows (FR-06).

``news_count`` counts distinct ``duplicate_group`` (a syndicated story counts once);
``eligible_article_count`` counts distinct canonical ``article_id``. Windows without news are
materialised with 0 (zero-fill over the trading grid).
"""
from functools import reduce

from pyspark.sql import functions as F

from stock_batch.reference import trading_grid
from stock_batch.schemas import conform


def window_label(minutes):
    return '1h' if minutes == 60 else f'{minutes}m'


def news_density(news, mapping, spark, cfg, dates=None):
    grid = trading_grid(spark, cfg, dates)
    eligible = news.join(mapping, 'article_id')
    if cfg['news'].get('trusted_only', True):
        eligible = eligible.filter(F.col('time_quality') == 'TRUSTED')
    eligible = (eligible.filter(F.col('relevance') >= float(cfg['news'].get('min_relevance', 0.0)))
                .select(F.col('symbol').alias('n_symbol'), 'article_id', 'duplicate_group', 'published_at')
                .dropDuplicates(['n_symbol', 'article_id']))
    out = []
    for window in sorted(int(w) for w in cfg['news']['windows_minutes']):
        x = (grid.withColumn('window_size', F.lit(window_label(window)))
             .withColumn('window_start', F.expr(f'window_end - INTERVAL {window} MINUTES')))
        x = x.join(eligible, (x.symbol == eligible.n_symbol) &
                   (eligible.published_at >= x.window_start) & (eligible.published_at < x.window_end), 'left')
        x = (x.groupBy('symbol', 'window_start', 'window_end', 'window_size', 'session_id', 'event_date')
             .agg(F.countDistinct('duplicate_group').cast('long').alias('news_count'),
                  F.countDistinct('article_id').cast('long').alias('eligible_article_count')))
        x = (x.withColumn('time_bucket', F.date_format(F.from_utc_timestamp(
                F.expr('window_end - INTERVAL 5 MINUTES'), 'Asia/Ho_Chi_Minh'), 'HH:mm'))
             .withColumn('mapping_version', F.lit(cfg['news']['mapping_version']))
             .withColumn('source_snapshot', F.lit('silver:news,news_stock_mapping')))
        out.append(conform(x, 'news_density'))
    return reduce(lambda a, b: a.unionByName(b), out)

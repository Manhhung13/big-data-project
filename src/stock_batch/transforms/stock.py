"""B1 — deterministic precedence, validation and explicit quarantine (FR-02)."""
from pyspark.sql import Window, functions as F, types as T

from stock_batch.reference import calendar, effective_on, symbols
from stock_batch.schemas import conform

PRICE_JSON = T.StructType([
    T.StructField(k, T.StringType()) for k in
    ('schema_version', 'symbol', 'source', 'source_price_unit', 'bar_start', 'bar_end', 'open', 'high',
     'low', 'close', 'volume', 'source_updated_at')])


def is_json_object(col):
    """True only for a syntactically valid JSON object (from_json alone yields an all-NULL struct)."""
    return col.isNotNull() & F.ltrim(col).startswith('{') & F.get_json_object(col, '$').isNotNull()


def parse_stock(bronze):
    """Decode Kafka payloads; adds ``trade_date`` (VN) and ``record_date`` used for scoping."""
    raw = (bronze.withColumn('json', F.col('raw_payload').cast('string'))
           .withColumn('_valid_json', is_json_object(F.col('json')))
           .withColumn('x', F.when(F.col('_valid_json'), F.from_json('json', PRICE_JSON)))
           .withColumn('symbol', F.upper(F.trim('x.symbol')))
           .withColumn('source', F.col('x.source'))
           .withColumn('source_unit', F.col('x.source_price_unit'))
           .withColumn('bar_start', F.to_timestamp('x.bar_start'))
           .withColumn('volume', F.col('x.volume').cast('long'))
           .withColumn('source_updated_at', F.to_timestamp('x.source_updated_at'))
           .withColumn('source_record_key', F.concat_ws(':', 'topic', 'partition', 'offset'))
           .withColumn('trade_date', F.to_date(F.from_utc_timestamp('bar_start', 'Asia/Ho_Chi_Minh'))))
    # A reject is filed under the trading day it belongs to; undecodable rows fall back to ingest day.
    return raw.withColumn('record_date', F.coalesce('trade_date', 'ingest_date'))


def clean_stock(bronze, spark, cfg, *, cache_classified=False, dates=None):
    """Return ``(accepted, rejects[, classified])``.

    ``dates`` (ISO strings) restricts the work to rows whose ``record_date`` is in scope *before*
    ranking; this is safe because duplicates share ``symbol + bar_start`` and thus the trade date.
    """
    raw = parse_stock(bronze)
    if dates is not None:
        raw = raw.filter(F.col('record_date').cast('string').isin(*dates))
    multipliers = cfg['market']['valid_price_units']
    unit_entries = [item for k, v in multipliers.items() for item in (F.lit(k), F.lit(float(v)))]
    pri_entries = [item for k, v in cfg['market']['source_priority'].items() for item in (F.lit(k), F.lit(int(v)))]
    raw = raw.withColumn('multiplier', F.create_map(*unit_entries)[F.col('source_unit')])
    for key in ('open', 'high', 'low', 'close'):
        raw = raw.withColumn(key, (F.col('x.' + key).cast('decimal(20,4)') * F.col('multiplier')).cast('decimal(20,4)'))
    raw = raw.withColumn('source_rank', F.coalesce(F.create_map(*pri_entries)[F.col('source')], F.lit(999)))

    ref = symbols(spark, cfg)
    known = F.broadcast(ref.select(F.col('symbol').alias('_known_symbol')).distinct())
    # Join by symbol AND effective range: a symbol with an exchange change has two master rows (P2).
    eff = F.broadcast(ref.select(F.col('symbol').alias('_ref_symbol'), 'exchange', 'effective_from', 'effective_to'))
    raw = (raw.join(known, raw.symbol == known._known_symbol, 'left')
              .join(eff, (raw.symbol == eff._ref_symbol) & effective_on(raw.trade_date), 'left'))
    cal = F.broadcast(calendar(spark, cfg).filter('is_trading_day')
                      .select(F.col('exchange').alias('_cal_exchange'), F.col('trading_date').alias('_cal_date'),
                              'session_id', 'start_utc', 'end_utc'))
    raw = raw.join(cal, (raw.exchange == cal._cal_exchange) & (raw.trade_date == cal._cal_date) &
                   (raw.bar_start >= cal.start_utc) & (raw.bar_start < cal.end_utc), 'left')
    valid_price = ((F.col('open') > 0) & (F.col('high') > 0) & (F.col('low') > 0) & (F.col('close') > 0) &
                   (F.col('high') >= F.greatest('open', 'close', 'low')) &
                   (F.col('low') <= F.least('open', 'close', 'high')))
    bar_end = F.to_timestamp('x.bar_end')
    reason = (F.when(~F.col('_valid_json'), 'INVALID_JSON')
              .when(F.col('symbol').isNull() | (F.col('symbol') == ''), 'MISSING_SYMBOL')
              .when(F.col('bar_start').isNull(), 'INVALID_TIMESTAMP')
              .when(F.col('_known_symbol').isNull(), 'UNKNOWN_SYMBOL')
              .when(F.col('exchange').isNull(), 'SYMBOL_NOT_EFFECTIVE')
              .when(F.col('multiplier').isNull(), 'UNKNOWN_PRICE_UNIT')
              .when(bar_end.isNotNull() & (bar_end != F.expr('bar_start + INTERVAL 1 MINUTE')), 'INCONSISTENT_BAR_END')
              .when(F.col('bar_start') != F.date_trunc('minute', 'bar_start'), 'NON_MINUTE_BAR')
              .when(F.col('session_id').isNull(), 'OUTSIDE_TRADING_SESSION')
              .when(~valid_price | F.col('open').isNull(), 'INVALID_OHLC')
              .when(F.col('volume').isNull() | (F.col('volume') < 0), 'INVALID_VOLUME'))
    raw = raw.withColumn('reason_code', reason)
    # One ranking classifies winners and DUPLICATE_LOSER rows; invalid rows sort last so they can
    # never displace the best valid source for a symbol + bar_start (BRD 8.2).
    win = Window.partitionBy('symbol', 'bar_start').orderBy(
        F.when(F.col('reason_code').isNull(), F.lit(0)).otherwise(F.lit(1)),
        F.col('source_rank').asc(),
        F.col('source_updated_at').desc_nulls_last(),
        F.col('kafka_timestamp').desc_nulls_last(),
        F.col('offset').desc(),
        F.col('partition').desc())
    classified = raw.withColumn('_candidate_rank', F.row_number().over(win))
    classified = classified.withColumn(
        'reason_code',
        F.when(F.col('reason_code').isNull() & (F.col('_candidate_rank') > 1), F.lit('DUPLICATE_LOSER'))
         .otherwise(F.col('reason_code')))
    if cache_classified:
        from pyspark import StorageLevel
        classified = classified.persist(StorageLevel.MEMORY_AND_DISK)  # caller unpersists

    out = (classified.where(F.col('reason_code').isNull())
           .withColumn('bar_end', F.expr('bar_start + INTERVAL 1 MINUTE'))
           .withColumn('quality_flag', F.lit('VALID'))
           .withColumn('schema_version', F.lit(cfg['project']['schema_version']))
           .withColumn('processed_at', F.current_timestamp()))
    bad = classified.where(F.col('reason_code').isNotNull()).withColumn('dataset', F.lit('stock'))
    result = (conform(out, 'stock_price'), conform(bad, 'quarantine'))
    return (*result, classified) if cache_classified else result

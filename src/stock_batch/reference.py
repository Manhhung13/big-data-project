"""Versioned (file-backed for the pilot) trading calendar and symbol dictionary.

``trading_dates``/``symbol_rows`` are pure Python (the files are small) and drive partition
pruning on the driver; ``symbols``/``calendar``/``trading_grid`` return Spark DataFrames.
"""
from __future__ import annotations

import csv
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from .config import repository_root


def _path(cfg, key):
    return str(repository_root() / cfg['market'][key])


@lru_cache(maxsize=16)
def _read_csv(path, mtime):
    with open(path, newline='', encoding='utf-8') as fh:
        return tuple(dict(r) for r in csv.DictReader(fh))


def _rows(cfg, key):
    path = _path(cfg, key)
    return _read_csv(path, Path(path).stat().st_mtime)


def symbol_rows(cfg):
    """Validated symbol master rows; overlapping effective ranges of one symbol are an error."""
    rows = _rows(cfg, 'symbol_file')
    by_symbol = {}
    for r in rows:
        start = date.fromisoformat(r['effective_from'])
        end = date.fromisoformat(r['effective_to']) if (r.get('effective_to') or '').strip() else None
        if end is not None and end < start:
            raise ValueError(f"symbol_master: {r['symbol']} effective_to < effective_from")
        by_symbol.setdefault(r['symbol'], []).append((start, end, r['exchange']))
    for symbol, spans in by_symbol.items():
        spans.sort(key=lambda s: s[0])
        for (s1, e1, _), (s2, _, _) in zip(spans, spans[1:]):
            if e1 is None or e1 >= s2:
                raise ValueError(f'symbol_master: overlapping effective ranges for {symbol}')
    return rows


def trading_dates(cfg, exchange=None):
    """Sorted ISO trading dates from the calendar (optionally for one exchange)."""
    out = {r['trading_date'] for r in _rows(cfg, 'calendar_file')
           if r['is_trading_day'].strip().lower() == 'true' and (exchange is None or r['exchange'] == exchange)}
    return sorted(out)


def trading_days_in(cfg, dates):
    wanted = set(dates)
    return [d for d in trading_dates(cfg) if d in wanted]


def previous_trading_dates(cfg, day, n):
    """The ``n`` trading dates strictly before ``day`` (any exchange)."""
    return [d for d in trading_dates(cfg) if d < day][-int(n):] if n > 0 else []


def next_trading_date(cfg, day):
    later = [d for d in trading_dates(cfg) if d > day]
    return later[0] if later else None


def neighbour_days(dates, before=1, after=0):
    """Calendar days around ``dates`` (for clock-time windows that cross midnight)."""
    out = set()
    for d in dates:
        x = date.fromisoformat(d)
        out.update((x + timedelta(days=k)).isoformat() for k in range(-before, after + 1))
    return sorted(out)


def symbols(spark, cfg):
    from pyspark.sql import functions as F
    symbol_rows(cfg)  # validates effective ranges before any join can multiply rows
    df = spark.read.option('header', True).csv(_path(cfg, 'symbol_file'))
    return (df.withColumn('effective_from', F.to_date('effective_from'))
              .withColumn('effective_to', F.to_date('effective_to'))
              .withColumn('aliases_array', F.split(F.coalesce('aliases', F.lit('')), r'\|')))


def effective_on(day_col):
    """Join predicate: the symbol master row is effective on ``day_col``."""
    from pyspark.sql import functions as F
    return ((day_col >= F.col('effective_from')) &
            (F.col('effective_to').isNull() | (day_col <= F.col('effective_to'))))


def calendar(spark, cfg):
    from pyspark.sql import functions as F
    df = spark.read.option('header', True).csv(_path(cfg, 'calendar_file'))
    day = F.col('trading_date')
    start = F.to_timestamp(F.concat_ws(' ', day, F.col('session_start')), 'yyyy-MM-dd HH:mm')
    end = F.to_timestamp(F.concat_ws(' ', day, F.col('session_end')), 'yyyy-MM-dd HH:mm')
    return (df.withColumn('trading_date', F.to_date('trading_date'))
              .withColumn('start_utc', F.to_utc_timestamp(start, 'Asia/Ho_Chi_Minh'))
              .withColumn('end_utc', F.to_utc_timestamp(end, 'Asia/Ho_Chi_Minh'))
              .withColumn('is_trading_day', F.lower(F.trim(F.col('is_trading_day'))) == 'true')
              .select('exchange', 'trading_date', 'session_id', 'start_utc', 'end_utc', 'is_trading_day',
                      'calendar_version'))


def trading_grid(spark, cfg, days=None):
    """One 5m window per exchange / active session / effective symbol, incl. no-news windows.

    Adds ``window_idx`` (1-based position inside the session) and ``session_windows``.
    """
    from pyspark.sql import functions as F
    c = calendar(spark, cfg).filter('is_trading_day')
    if days is not None:
        c = c.filter(F.col('trading_date').cast('string').isin(*[str(d) for d in days]))
    s = symbols(spark, cfg).select('symbol', 'exchange', 'effective_from', 'effective_to')
    joined = c.join(s, 'exchange').where(effective_on(F.col('trading_date')))
    minutes = int(cfg['market']['feature_minutes'])
    seq = F.expr(f'sequence(start_utc + INTERVAL {minutes} MINUTES, end_utc, INTERVAL {minutes} MINUTES)')
    return (joined.withColumn('_ends', seq)
            .withColumn('session_windows', F.size('_ends'))
            .select('symbol', 'exchange', F.col('trading_date').alias('event_date'), 'session_id',
                    'session_windows', F.posexplode('_ends').alias('_pos', 'window_end'))
            .withColumn('window_idx', F.col('_pos') + 1)
            .withColumn('window_start', F.expr(f'window_end - INTERVAL {minutes} MINUTES'))
            .select('symbol', 'exchange', 'event_date', 'session_id', 'window_start', 'window_end',
                    'window_idx', 'session_windows'))

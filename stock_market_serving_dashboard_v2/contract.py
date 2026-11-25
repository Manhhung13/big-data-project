"""Document IDs + field contracts. Input dates must use UTC ISO8601."""
from datetime import datetime, timedelta, timezone


def canonical_time(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return utc_string(value)


def canonical_symbol(symbol):
    if not isinstance(symbol, str):
        raise ValueError("symbol must be a string")
    symbol = symbol.strip().upper()
    if not symbol or "|" in symbol:
        raise ValueError("invalid symbol")
    return symbol

# Same symbol + timestamp in the same logical dataset = same document.
def price_id(symbol, timestamp):
    return f"{canonical_symbol(symbol)}|{canonical_time(timestamp)}"


def feature_id(symbol, window_start):
    return price_id(symbol, window_start)


def density_id(symbol, window_minutes, window_start):
    if type(window_minutes) is not int or window_minutes not in (5, 30, 60):
        raise ValueError("window_minutes must be 5, 30 or 60")
    return f"{canonical_symbol(symbol)}|{int(window_minutes)}m|{canonical_time(window_start)}"


def news_id(article_id, symbol):
    return f"{article_id}|{canonical_symbol(symbol)}"


def event_id(symbol, event_time):
    return f"{canonical_symbol(symbol)}_{canonical_time(event_time)}"


def classify_event(price_anomaly, volume_anomaly, news_burst):
    f = (bool(price_anomaly), bool(volume_anomaly), bool(news_burst))
    labels = {
        (1, 0, 0): "PRICE_ONLY",
        (0, 1, 0): "VOLUME_ONLY",
        (0, 0, 1): "NEWS_ONLY",
        (1, 1, 0): "PRICE_VOLUME",
        (1, 0, 1): "PRICE_NEWS",
        (0, 1, 1): "NEWS_VOLUME",
        (1, 1, 1): "COMPOSITE",
    }
    return labels.get(f, "NONE")


def utc_string(dt):
    """Enforce timezone-aware UTC time; rejects naive local timestamps."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def around_event(event_time: datetime, minutes=30):
    return event_time - timedelta(minutes=minutes), event_time + timedelta(minutes=minutes)

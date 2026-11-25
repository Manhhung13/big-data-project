"""Bounded detail queries and complete Elasticsearch aggregate statistics."""
from datetime import datetime, timedelta

VN = "Asia/Ho_Chi_Minh"


def symbol_time_filter(symbol, field, start, end=None):
    dates = {"gte": start}
    if end is not None:
        dates["lt"] = end
    filters = [{"range": {field: dates}}]
    if symbol:
        filters.append({"term": {"symbol": symbol}})
    return {"bool": {"filter": filters}}


def histogram(field, interval):
    return {"field": field, "fixed_interval": interval, "min_doc_count": 1, "time_zone": VN}


def available_data(es, price_index):
    r = es.search(index=price_index, size=0, aggs={
        "symbols": {"terms": {"field": "symbol", "size": 1000}},
        "first": {"min": {"field": "timestamp"}}, "last": {"max": {"field": "timestamp"}}})
    a = r["aggregations"]
    return [b["key"] for b in a["symbols"]["buckets"]], a["first"].get("value_as_string"), a["last"].get("value_as_string")


def price_series(es, index, symbol, start, interval, end=None):
    r = es.search(index=index, size=0, query=symbol_time_filter(symbol, "timestamp", start, end),
        aggs={"buckets": {"date_histogram": histogram("timestamp", interval), "aggs": {
            "first_open": {"top_metrics": {"metrics": {"field": "open"}, "sort": {"timestamp": "asc"}}},
            "last_close": {"top_metrics": {"metrics": {"field": "close"}, "sort": {"timestamp": "desc"}}},
            "high": {"max": {"field": "high"}}, "low": {"min": {"field": "low"}},
            "total_volume": {"sum": {"field": "volume"}}}}})
    return [{"time": b["key_as_string"], "open": b["first_open"]["top"][0]["metrics"]["open"],
             "close": b["last_close"]["top"][0]["metrics"]["close"], "high": b["high"]["value"],
             "low": b["low"]["value"], "volume": b["total_volume"]["value"]}
            for b in r["aggregations"]["buckets"]["buckets"]]


def news_density_series(es, index, symbol, start, interval, end=None):
    q = symbol_time_filter(symbol, "window_start", start, end)
    q["bool"]["filter"].append({"term": {"window_minutes": 5}})
    r = es.search(index=index, size=0, query=q, aggs={"buckets": {
        "date_histogram": histogram("window_start", interval),
        "aggs": {"count": {"sum": {"field": "news_count"}}}}})
    return [{"time": b["key_as_string"], "news_count": int(b["count"]["value"])}
            for b in r["aggregations"]["buckets"]["buckets"]]


def recent_events(es, index, symbol, start, limit=200, end=None, event_types=None, statuses=None):
    q = symbol_time_filter(symbol, "event_time", start, end)
    if event_types is not None:
        q["bool"]["filter"].append({"terms": {"event_type": event_types}})
    if statuses is not None:
        q["bool"]["filter"].append({"terms": {"status": statuses}})
    r = es.search(index=index, size=limit, query=q, track_total_hits=True,
                  sort=[{"event_time": "desc"}, {"event_id": "asc"}])
    return [h["_source"] for h in r["hits"]["hits"]], r["hits"]["total"]["value"]


def search_articles(es, index, symbol, start, end=None, text="", limit=100):
    q = symbol_time_filter(symbol, "published_at", start, end)
    if text.strip():
        q["bool"]["must"] = [{"multi_match": {"query": text.strip(), "fields": ["title^2", "content"]}}]
    r = es.search(index=index, size=limit, query=q, track_total_hits=True,
                  sort=[{"published_at": "desc"}, {"article_id": "asc"}])
    return [h["_source"] for h in r["hits"]["hits"]], r["hits"]["total"]["value"]


def related_articles(es, index, symbol, event_time, minutes=30, limit=100):
    moment = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    q = symbol_time_filter(symbol, "published_at", (moment - timedelta(minutes=minutes)).isoformat())
    q["bool"]["filter"][0]["range"]["published_at"]["lte"] = (moment + timedelta(minutes=minutes)).isoformat()
    r = es.search(index=index, size=limit, query=q, track_total_hits=True,
                  sort=[{"published_at": "asc"}, {"article_id": "asc"}])
    return [h["_source"] for h in r["hits"]["hits"]], r["hits"]["total"]["value"]


def event_summary(es, index, start, end=None, symbol=None):
    r = es.search(index=index, size=0, track_total_hits=True,
        query=symbol_time_filter(symbol, "event_time", start, end), aggs={
            "by_day": {"date_histogram": histogram("event_time", "1d")},
            "top_symbols": {"terms": {"field": "symbol", "size": 10}},
            "event_types": {"terms": {"field": "event_type", "size": 20}},
            "price_anomalies": {"filter": {"term": {"price_anomaly": True}}},
            "volume_anomalies": {"filter": {"term": {"volume_anomaly": True}}},
            "news_bursts": {"filter": {"term": {"news_burst": True}}},
            "completed": {"filter": {"term": {"status": "COMPLETED"}}, "aggs": {
                "before": {"stats": {"field": "return_before_30m"}},
                "after": {"stats": {"field": "return_after_30m"}}}}})
    return dict(r["aggregations"], total=r["hits"]["total"]["value"])


def news_activity(es, index, start, end=None):
    # Cardinality counts unique articles rather than article-symbol links.
    r = es.search(index=index, size=0,
        query=symbol_time_filter(None, "published_at", start, end), aggs={
            "articles": {"cardinality": {"field": "article_id", "precision_threshold": 40000}},
            "unmapped": {"filter": {"term": {"symbol": "__UNMAPPED__"}}}})
    return r["aggregations"]

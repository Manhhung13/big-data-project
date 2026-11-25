"""Create Serving Layer index mappings once. Do NOT drop production indexes."""
from es_common import es_client, PRICE, FEATURES, NEWS, DENSITY, EVENTS

DT = {"type": "date"}
K = {"type": "keyword"}
D = {"type": "double"}
B = {"type": "boolean"}
I = {"type": "integer"}

MAPPINGS = {
    PRICE: {"properties": {
        "symbol": K, "timestamp": DT, "open": D, "high": D,
        "low": D, "close": D, "volume": D, "source": K,
        "processed_at": DT, "origin": K,
        "source_version": {"type": "long"},
    }},
    FEATURES: {"properties": {
        "symbol": K, "window_start": DT, "window_end": DT,
        "return": D, "return_zscore": D, "volume_zscore": D,
        "price_anomaly": B, "volume_anomaly": B,
        "close": D, "volume": D, "origin": K, "processed_at": DT,
        "source_version": {"type": "long"},
    }},
    NEWS: {"properties": {
        "article_id": K, "symbol": K, "title": {"type": "text", "fields": {"raw": K}},
        "content": {"type": "text"}, "url": {"type": "keyword", "ignore_above": 2048},
        "source": K, "published_at": DT, "crawled_at": DT, "processed_at": DT,
        "mapping_rule": K, "matched_alias": K, "origin": K,
        "source_version": {"type": "long"},
    }},
    DENSITY: {"properties": {
        "symbol": K, "window_start": DT, "window_minutes": I,
        "news_count": I, "news_zscore": D, "news_burst": B,
        "origin": K, "processed_at": DT,
        "source_version": {"type": "long"},
    }},
    EVENTS: {"properties": {
        "event_id": K, "symbol": K, "event_time": DT,
        "window_start": DT, "event_type": K, "status": K, "origin": K,
        "price_anomaly": B, "volume_anomaly": B, "news_burst": B,
        "return_zscore": D, "volume_zscore": D, "news_zscore": D,
        "related_article_ids": K,
        "return_before_30m": D, "return_after_30m": D,
        "return_after_1h": D, "return_after_1d": D,
        "news_count_before_30m": I, "news_count_after_30m": I,
        "processed_at": DT,
        "source_version": {"type": "long"},
    }},
}


def ensure_indexes(es):
    if not es.ping():
        raise SystemExit("Elasticsearch chưa sẵn sàng. Hãy chạy docker compose up -d.")
    for name, mapping in MAPPINGS.items():
        if es.indices.exists(index=name):
            es.indices.put_mapping(index=name, **mapping)
            print("READY", name)
        else:
            es.indices.create(index=name, mappings=mapping,
                              settings={"number_of_shards": 1, "number_of_replicas": 0})
            print("CREATED", name)
    print("Mappings ready; existing documents preserved.")


def main():
    with es_client() as es:
        ensure_indexes(es)


if __name__ == "__main__":
    main()

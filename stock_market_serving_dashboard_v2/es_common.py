"""Shared connection and index naming for the group's Serving Layer."""
import os
from elasticsearch import Elasticsearch

def index_names(dataset="real"):
    """Keep crawled data separate from the original synthetic demo indexes."""
    if dataset not in ("real", "demo"):
        raise ValueError("ES_DATASET must be real or demo")
    prefix = "real-" if dataset == "real" else ""
    return tuple(prefix + name for name in (
        "stock-price-v1", "stock-features-v1", "stock-news-v1",
        "news-density-v1", "market-events-v1"))


DATASET = os.environ.get("ES_DATASET", "real")
PRICE, FEATURES, NEWS, DENSITY, EVENTS = index_names(DATASET)
ALL_INDEXES = (PRICE, FEATURES, NEWS, DENSITY, EVENTS)


def es_client():
    return Elasticsearch(os.environ.get("ES_URL", "http://localhost:9200"),
                         request_timeout=20, max_retries=3, retry_on_timeout=True)

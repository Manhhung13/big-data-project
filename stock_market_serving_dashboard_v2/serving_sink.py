"""JSONL sink for Batch/Speed serving views, with deterministic IDs and ownership.

Spark can call normalize_document() and upsert_action() inside foreachPartition
or foreachBatch. source_version must increase for each logical key and origin.
Final Batch views own a key once written; late Speed views cannot replace them.
"""
import argparse
import json
from pathlib import Path
from elasticsearch.helpers import streaming_bulk
from contract import (canonical_symbol, canonical_time, price_id, feature_id,
                      density_id, news_id, event_id, classify_event)
from es_common import PRICE, FEATURES, NEWS, DENSITY, EVENTS, es_client
from init_indexes import ensure_indexes

INDEXES = dict(price=PRICE, features=FEATURES, news=NEWS, density=DENSITY, events=EVENTS)
SCRIPT = """
if (ctx.op == 'create') { ctx._source = params.doc; }
else {
  def oldVersion = ctx._source.containsKey('source_version') ? ctx._source.source_version : 0;
  if (ctx._source.status == 'COMPLETED' && params.doc.status == 'OPEN') {
    ctx.op = 'noop';
  } else if (ctx._source.origin == 'batch' && params.doc.origin == 'speed') {
    ctx.op = 'noop';
  } else if (ctx._source.origin == params.doc.origin && params.doc.source_version <= oldVersion
             && !(params.doc.source_version == 0 && oldVersion == 0)) {
    ctx.op = 'noop';
  } else {
    ctx._source = params.doc;
  }
}
"""


def upsert_action(index, key, document):
    document = dict(document)
    document.setdefault("source_version", 0)
    return {"_op_type": "update", "_index": index, "_id": key,
            "_retry_on_conflict": 3,
            "scripted_upsert": True, "script": {"lang": "painless", "source": SCRIPT,
                                                "params": {"doc": document}}, "upsert": {}}


def normalize_document(kind, raw):
    doc = dict(raw)
    if doc.get("origin") not in ("batch", "speed"):
        raise ValueError("origin must be batch or speed")
    if type(doc.get("source_version")) is not int or doc["source_version"] < 1:
        raise ValueError("source_version must be a positive increasing integer")
    doc["symbol"] = canonical_symbol(doc["symbol"])
    doc["processed_at"] = canonical_time(doc["processed_at"])
    for field in ("timestamp", "window_start", "window_end", "event_time", "published_at", "crawled_at"):
        if doc.get(field) is not None:
            doc[field] = canonical_time(doc[field])
    if kind == "price":
        for field in ("open", "high", "low", "close", "volume"):
            if type(doc[field]) not in (int, float):
                raise ValueError(f"{field} must be numeric")
        if (min(doc[f] for f in ("open", "high", "low", "close")) <= 0 or doc["volume"] < 0
            or doc["high"] < max(doc["open"], doc["close"], doc["low"])
            or doc["low"] > min(doc["open"], doc["close"], doc["high"])):
            raise ValueError("invalid OHLCV")
        key = price_id(doc["symbol"], doc["timestamp"])
    elif kind == "features":
        key = feature_id(doc["symbol"], doc["window_start"])
    elif kind == "density":
        if type(doc["news_count"]) is not int or doc["news_count"] < 0:
            raise ValueError("news_count must be a nonnegative integer")
        key = density_id(doc["symbol"], doc["window_minutes"], doc["window_start"])
    elif kind == "news":
        if not doc.get("title") or not doc.get("article_id") or not doc.get("published_at"):
            raise ValueError("news requires article_id, title, published_at")
        key = news_id(doc["article_id"], doc["symbol"])
    elif kind == "events":
        flags = [doc.get(f) for f in ("price_anomaly", "volume_anomaly", "news_burst")]
        if any(type(f) is not bool for f in flags) or not any(flags):
            raise ValueError("event requires boolean flags and at least one anomaly")
        event_type = classify_event(*flags)
        if doc.get("event_type", event_type) != event_type:
            raise ValueError("event_type does not match flags")
        doc["event_type"] = event_type
        if doc.get("status") not in ("OPEN", "COMPLETED"):
            raise ValueError("status must be OPEN or COMPLETED")
        if doc["status"] == "OPEN" and any(doc.get(f) is not None for f in (
                "return_after_30m", "return_after_1h", "return_after_1d", "news_count_after_30m")):
            raise ValueError("OPEN event must not claim completed after-event metrics")
        key = event_id(doc["symbol"], doc["event_time"])
        if doc.get("event_id", key) != key:
            raise ValueError(f"event_id must be {key}")
        doc["event_id"] = key
    else:
        raise ValueError(f"Unknown view: {kind}")
    # Reject JSON NaN/Infinity rather than indexing them as apparent measurements.
    json.dumps(doc, allow_nan=False)
    return key, doc


def jsonl_actions(path, kind):
    with path.open(encoding="utf-8-sig") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                key, doc = normalize_document(kind, json.loads(line))
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"{path}:{line_number}: {exc}") from exc
            yield upsert_action(INDEXES[kind], key, doc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=INDEXES)
    parser.add_argument("input", type=Path)
    parser.add_argument("--load", action="store_true")
    args = parser.parse_args()
    validated = sum(1 for _ in jsonl_actions(args.input, args.kind))
    print(f"Validated {validated} documents")
    if not args.load:
        return
    errors = 0
    with es_client() as es:
        ensure_indexes(es)
        for ok, item in streaming_bulk(es, jsonl_actions(args.input, args.kind), max_retries=3,
                                       raise_on_error=False):
            if not ok:
                errors += 1
                if errors <= 3:
                    print("ERROR", item)
        es.indices.refresh(index=INDEXES[args.kind])
    if errors:
        raise SystemExit(f"Failed documents: {errors}")


if __name__ == "__main__":
    main()

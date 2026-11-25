"""Opt-in tests. Only the temporary serving-test-<uuid>-* indexes are removed."""
import os
import unittest
from uuid import uuid4
from elasticsearch.helpers import bulk
from es_common import es_client, PRICE, FEATURES, NEWS, DENSITY, EVENTS
from init_indexes import MAPPINGS
from serving_sink import upsert_action
from queries import price_series, news_density_series, recent_events, event_summary, related_articles


@unittest.skipUnless(os.environ.get("RUN_ES_TESTS") == "1", "Set RUN_ES_TESTS=1 for live Elasticsearch checks")
class ElasticsearchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.es = es_client()
        cls.prefix = "serving-test-" + uuid4().hex + "-"
        cls.names = {name: cls.prefix + str(i) for i, name in enumerate(MAPPINGS)}
        for name, mapping in MAPPINGS.items():
            cls.es.indices.create(index=cls.names[name], mappings=mapping,
                                  settings={"number_of_shards": 1, "number_of_replicas": 0})

    @classmethod
    def tearDownClass(cls):
        for name in cls.names.values():
            assert name.startswith(cls.prefix) and cls.prefix.startswith("serving-test-")
            cls.es.indices.delete(index=name)
        cls.es.close()

    def test_batch_authority_revision_and_replay(self):
        index = self.names[FEATURES]
        key = "FPT|2026-11-10T03:00:00Z"
        def write(origin, version, close):
            bulk(self.es, [upsert_action(index, key, dict(symbol="FPT", origin=origin, source_version=version, close=close))], refresh=True)
        write("speed", 2, 10)
        write("speed", 1, 9)
        self.assertEqual(self.es.get(index=index, id=key)["_source"]["close"], 10)
        write("batch", 1, 12)
        write("speed", 100, 99)
        write("batch", 1, 88)
        self.assertEqual(self.es.get(index=index, id=key)["_source"]["close"], 12)
        self.assertEqual(self.es.count(index=index)["count"], 1)

    def test_histogram_ohlcv_end_and_nonoverlapping_density(self):
        index = self.names[PRICE]
        docs = [dict(timestamp=t, symbol="HPG", open=o, high=h, low=l, close=c, volume=v)
                for t, o, h, l, c, v in [("2026-11-10T03:00:00Z", 10, 12, 9, 11, 100),
                                         ("2026-11-10T03:04:00Z", 11, 14, 10, 13, 200),
                                         ("2026-11-10T04:00:00Z", 13, 15, 12, 14, 999)]]
        bulk(self.es, [dict(_index=index, _id=str(i), _source=d) for i, d in enumerate(docs)], refresh=True)
        rows = price_series(self.es, index, "HPG", "2026-11-10T03:00:00Z", "1h", "2026-11-10T04:00:00Z")
        self.assertEqual(len(rows), 1)
        self.assertEqual([rows[0][f] for f in ("open", "high", "low", "close", "volume")], [10, 14, 9, 13, 300])
        index = self.names[DENSITY]
        bulk(self.es, [dict(_index=index, _id=str(m), _source=dict(symbol="HPG", window_start="2026-11-10T03:00:00Z", window_minutes=m, news_count=n)) for m, n in [(5, 2), (30, 10), (60, 20)]], refresh=True)
        self.assertEqual(news_density_series(self.es, index, "HPG", "2026-11-10T00:00:00Z", "1h")[0]["news_count"], 2)

    def test_stats_are_not_limited_to_detail_rows(self):
        index = self.names[EVENTS]
        docs = [dict(event_id=str(i), symbol="FPT", event_time="2026-11-10T03:00:00Z", event_type="PRICE_ONLY", price_anomaly=True,
                     status="COMPLETED", return_after_30m=.01) for i in range(220)]
        bulk(self.es, [dict(_index=index, _id=str(i), _source=d) for i, d in enumerate(docs)], refresh=True)
        events, count = recent_events(self.es, index, "FPT", "2026-11-10T00:00:00Z")
        summary = event_summary(self.es, index, "2026-11-10T00:00:00Z")
        self.assertEqual((len(events), count, summary["total"]), (200, 220, 220))
        self.assertEqual(summary["completed"]["after"]["count"], 220)
        filtered, count = recent_events(self.es, index, "FPT", "2026-11-10T00:00:00Z", event_types=["COMPOSITE"])
        self.assertEqual((filtered, count), ([], 0))

    def test_related_news_includes_boundaries_and_only_symbol(self):
        index = self.names[NEWS]
        docs = [dict(article_id=str(i), symbol=s, title="test", published_at=t) for i, (s, t) in enumerate([
            ("FPT", "2026-11-10T02:30:00Z"), ("FPT", "2026-11-10T03:30:00Z"),
            ("FPT", "2026-11-10T03:30:01Z"), ("HPG", "2026-11-10T03:00:00Z")])]
        bulk(self.es, [dict(_index=index, _id=str(i), _source=d) for i, d in enumerate(docs)], refresh=True)
        rows, total = related_articles(self.es, index, "FPT", "2026-11-10T03:00:00Z")
        self.assertEqual((len(rows), total), (2, 2))

    def test_completed_event_cannot_reopen(self):
        index = self.names[EVENTS]
        for version, status in [(1, "OPEN"), (2, "COMPLETED"), (3, "OPEN")]:
            bulk(self.es, [upsert_action(index, "lifecycle", dict(symbol="LIFE", origin="speed", source_version=version, status=status))], refresh=True)
        self.assertEqual(self.es.get(index=index, id="lifecycle")["_source"]["status"], "COMPLETED")

    def test_bronze_refresh_cannot_erase_gold(self):
        index = self.names[FEATURES]
        for version, close in [(0, 10), (0, 11), (1, 12), (0, 99)]:
            bulk(self.es, [upsert_action(index, "bronze-refresh", dict(symbol="BRZ", origin="batch", source_version=version, close=close))], refresh=True)
            if close == 11:
                self.assertEqual(self.es.get(index=index, id="bronze-refresh")["_source"]["close"], 11)
        self.assertEqual(self.es.get(index=index, id="bronze-refresh")["_source"]["close"], 12)


if __name__ == "__main__":
    unittest.main()

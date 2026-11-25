import unittest
from serving_sink import normalize_document


class SinkTest(unittest.TestCase):
    def event(self, **changes):
        return dict(symbol="FPT", event_time="2026-11-10T10:00:00+07:00", processed_at="2026-11-10T03:01:00Z",
                    origin="speed", source_version=1, status="OPEN", price_anomaly=True,
                    volume_anomaly=False, news_burst=False, **changes)

    def test_event_key_and_classification(self):
        key, doc = normalize_document("events", self.event())
        self.assertEqual(key, "FPT_2026-11-10T03:00:00Z")
        self.assertEqual(doc["event_type"], "PRICE_ONLY")

    def test_reject_inconsistent_event_and_partial_future(self):
        for changes in (dict(event_type="COMPOSITE"), dict(return_after_30m=.1), dict(price_anomaly="false"),
                        dict(status="CLOSED"), dict(source_version=0)):
            doc = self.event()
            doc.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                normalize_document("events", doc)

    def test_reject_nonfinite(self):
        doc = self.event(return_zscore=float("nan"))
        with self.assertRaises(ValueError):
            normalize_document("events", doc)


if __name__ == "__main__":
    unittest.main()

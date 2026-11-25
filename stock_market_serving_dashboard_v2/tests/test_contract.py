import unittest
from datetime import datetime, timezone
from contract import (classify_event, price_id, news_id, density_id, utc_string,
                      around_event)


class ContractTest(unittest.TestCase):
    def test_classification(self):
        self.assertEqual(classify_event(True, True, True), "COMPOSITE")
        self.assertEqual(classify_event(True, False, True), "PRICE_NEWS")
        self.assertEqual(classify_event(False, True, True), "NEWS_VOLUME")

    def test_replay_has_same_id(self):
        t = "2026-11-10T03:35:00Z"
        self.assertEqual(price_id("FPT", t), price_id("FPT", t))
        self.assertNotEqual(news_id("art001", "FPT"), news_id("art001", "HPG"))
        self.assertNotEqual(density_id("FPT", 5, t), density_id("FPT", 30, t))

    def test_event_window(self):
        t = datetime(2026, 11, 10, 3, 35, tzinfo=timezone.utc)
        before, after = around_event(t)
        self.assertEqual((after - before).total_seconds(), 3600)

    def test_timezone(self):
        self.assertEqual(utc_string(datetime(2026, 11, 10, 3, 35,
                            tzinfo=timezone.utc)), "2026-11-10T03:35:00Z")
        with self.assertRaises(ValueError):
            utc_string(datetime(2026, 11, 10, 3, 35))


if __name__ == "__main__":
    unittest.main()

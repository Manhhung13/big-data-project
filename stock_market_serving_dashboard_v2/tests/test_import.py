import unittest
import pandas as pd
from import_parquet import clean_stock, clean_news, article_symbols, feature_frame, parse_news_time
from contract import price_id


class ImportTest(unittest.TestCase):
    def test_equivalent_time_ids(self):
        self.assertEqual(price_id(" fpt ", "2026-11-10T10:35:00+07:00"),
                         price_id("FPT", "2026-11-10T03:35:00Z"))
        with self.assertRaises(ValueError):
            price_id("FPT", "2026-11-10T10:35:00")

    def test_news_local_timezone(self):
        self.assertEqual(str(parse_news_time("2026-11-10T10:35:00")), "2026-11-10 03:35:00+00:00")
        self.assertEqual(parse_news_time("2026-11-10T10:35:00+07:00"),
                         parse_news_time("2026-11-10T03:35:00Z"))

    def test_mapping_boundaries_and_multisymbol(self):
        self.assertEqual(set(article_symbols("FPT và HPG", "FPTX không phải mã", {"FPT", "HPG"})), {"FPT", "HPG"})
        self.assertFalse(article_symbols("fpt FPTX", "", {"FPT"}))
        self.assertEqual(article_symbols("Thế Giới Di Động", "", {"MWG"}, {"MWG": ["Thế Giới Di Động"]})["MWG"][0], "alias")

    def test_news_dedup_and_unmapped_preserved(self):
        row = dict(article_id="a1", title="FPT HPG", content="", source="cafef", url="",
                   published_at="2026-11-10T10:00:00", crawled_at="2026-11-10T10:01:00+07:00")
        docs, stats = clean_news(pd.DataFrame([row, row, dict(row, article_id="a2", title="Kinh tế chung")]),
                                 {"FPT", "HPG"}, "Asia/Ho_Chi_Minh")
        self.assertEqual(len(docs), 3)
        self.assertEqual(stats["duplicates"], 1)
        self.assertEqual({d["symbol"] for d in docs}, {"FPT", "HPG", "__UNMAPPED__"})

    def test_reject_invalid_ohlcv_naive_and_dedup(self):
        row = dict(symbol="fpt", timestamp="2026-11-10T10:00:00+07:00", open=10, high=12, low=9, close=11, volume=100, source="test")
        raw = pd.DataFrame([row, row, dict(row, low=15), dict(row, timestamp="2026-11-10T10:01:00"), dict(row, volume=-1)])
        clean, stats = clean_stock(raw)
        self.assertEqual(len(clean), 1)
        self.assertEqual(stats["invalid"], 3)
        self.assertEqual(stats["duplicates"], 1)

    def test_features_do_not_fill_lunch_gap(self):
        rows = pd.DataFrame([dict(timestamp=pd.Timestamp(t, tz="UTC"), open=10, high=12, low=9, close=c, volume=100)
                             for t, c in [("2026-11-10 04:20", 10), ("2026-11-10 04:25", 11), ("2026-11-10 06:00", 12)]])
        features = feature_frame(rows)
        self.assertEqual(len(features), 3)
        self.assertAlmostEqual(features.iloc[1]["return"], .1)
        self.assertTrue(pd.isna(features.iloc[2]["return"]))


if __name__ == "__main__":
    unittest.main()

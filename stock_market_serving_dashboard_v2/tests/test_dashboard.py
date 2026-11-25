"""Application tests against the already loaded local real + demo datasets."""
import os
import unittest
from pathlib import Path


@unittest.skipUnless(os.environ.get("RUN_DASHBOARD_TESTS") == "1", "Set RUN_DASHBOARD_TESTS=1 after loading both datasets")
class DashboardTest(unittest.TestCase):
    def test_dataset_ranges_search_and_filters(self):
        from streamlit.testing.v1 import AppTest
        at = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard.py"), default_timeout=45).run()

        def healthy():
            self.assertFalse(at.exception, [x.message for x in at.exception])
            self.assertFalse(at.error, [x.value for x in at.error])
            self.assertEqual(len(at.tabs), 4)

        healthy()
        self.assertGreater(len(at.sidebar.selectbox[1].options), 3)
        at.sidebar.selectbox[2].set_value("Toàn bộ lịch sử").run()
        healthy()
        at.sidebar.selectbox[2].set_value("Tùy chọn").run()
        healthy()
        at.sidebar.selectbox[2].set_value("Gần hiện tại (24 giờ)").run()
        healthy()
        at.sidebar.selectbox[0].set_value("demo").run()
        healthy()
        self.assertTrue(any("GIẢ LẬP" in x.value for x in at.warning))
        at.sidebar.selectbox[2].set_value("Toàn bộ lịch sử").run()
        healthy()
        at.multiselect[0].set_value(["COMPOSITE"]).run()
        healthy()
        at.text_input[0].set_value("FAKE").run()
        healthy()
        at.checkbox[0].set_value(True).run()
        healthy()


if __name__ == "__main__":
    unittest.main()

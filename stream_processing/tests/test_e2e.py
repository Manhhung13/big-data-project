"""End-to-end trên dữ liệu GIẢ LẬP: chạy cả pipeline (sinh dữ liệu -> baseline -> streaming) trong thư mục tạm,
rồi so kết quả với đáp án biết trước của simulator.

Chạy mất vài phút vì khởi động Spark nhiều lần và xử lý ~120 micro-batch.
"""
import json
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
DAY = "20261109"


def _run(module: str, env: dict, *args: str):
    subprocess.run([sys.executable, "-X", "utf8", "-m", module, *args], cwd=ROOT, env=env, check=True)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    data = tmp_path_factory.mktemp("bigdata")
    env = {**os.environ, "BIGDATA_SOURCE": "sim", "BIGDATA_DATA": str(data), "BIGDATA_MAX_FILES": "3",
           "PYTHONIOENCODING": "utf-8"}
    _run("simulator.gen_history", env)
    _run("batch.build_baseline", env)
    _run("simulator.gen_live", env)
    _run("streaming.run", env, "--mode", "availableNow", "--reset")
    conn = sqlite3.connect(data / "serving" / "serving.db")
    conn.row_factory = sqlite3.Row
    yield conn, data
    conn.close()


def events(conn) -> dict:
    return {r["event_id"]: dict(r) for r in conn.execute("SELECT * FROM market_event")}


def test_ohlcv_matches_ground_truth(result):
    """Mọi window khớp tuyệt đối với đáp án => nến gửi trùng không bị cộng đôi volume,
    nến đến trễ bị loại, bản ghi hỏng không lọt vào, open/close đúng theo event-time."""
    conn, data = result
    bars = pd.read_csv(data / "live" / "bars_clean.csv", parse_dates=["timestamp"])
    bars["window_start"] = bars["timestamp"].dt.floor("5min").dt.strftime("%Y-%m-%d %H:%M:%S")
    exp = bars.sort_values("timestamp").groupby(["symbol", "window_start"]).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"), n_bars=("close", "size")).reset_index()
    got = pd.read_sql("SELECT symbol, window_start, open, high, low, close, volume, n_bars "
                      "FROM signals WHERE n_bars IS NOT NULL", conn)
    merged = exp.merge(got, on=["symbol", "window_start"], suffixes=("_e", "_g"), how="outer", indicator=True)
    assert len(exp) == len(got) == 255 and (merged["_merge"] == "both").all()
    for col in ["open", "high", "low", "close", "volume", "n_bars"]:
        assert (merged[col + "_e"] - merged[col + "_g"]).abs().max() < 1e-9, col
    assert conn.execute("SELECT MIN(n_bars) FROM signals WHERE n_bars IS NOT NULL").fetchone()[0] == 5


def test_injected_events_are_detected_with_expected_type(result):
    conn, data = result
    ev = events(conn)
    truth = json.loads((data / "live" / "ground_truth.json").read_text(encoding="utf-8"))
    for sc in truth["scenarios"]:
        hhmm = sc["at"].replace(":", "")
        eid = f"{sc['symbol']}_{DAY}_{hhmm}"
        assert eid in ev, f"không phát hiện {eid} (kỳ vọng {sc['expect']})"
        assert ev[eid]["event_type"] == sc["expect"], (eid, ev[eid]["event_type"], sc["expect"])


def test_composite_event_is_completed_with_after_event_features(result):
    conn, _ = result
    e = events(conn)[f"FPT_{DAY}_1035"]
    assert e["status"] == "COMPLETED"                      # đã thấy >= 30' dữ liệu sau window sự kiện
    assert e["return_after_30m"] is not None and e["news_after_30m"] is not None
    assert e["return_z"] >= 3 and e["volume_z"] >= 3 and e["news_z_30m"] >= 3


def test_event_status_follows_event_time(result):
    """OPEN -> COMPLETED theo event-time: event đã có >= 30' dữ liệu sau window thì COMPLETED, ngược lại OPEN."""
    conn, _ = result
    max_t = pd.Timestamp(conn.execute("SELECT value FROM meta WHERE key = 'max_event_time'").fetchone()[0])
    assert max_t.strftime("%H:%M") == "14:44"
    statuses = Counter()
    for e in events(conn).values():
        want = "COMPLETED" if pd.Timestamp(e["window_end"]) + pd.Timedelta(minutes=30) <= max_t else "OPEN"
        assert e["status"] == want, e["event_id"]
        statuses[e["status"]] += 1
    assert statuses["OPEN"] >= 1 and statuses["COMPLETED"] >= 1


def test_control_symbol_has_few_false_positives(result):
    """MWG không có sự kiện cấy: số cờ phải thấp (luật z>=3 hoặc P99 cho phép ~vài % window báo giả)."""
    conn, _ = result
    n = conn.execute("SELECT COUNT(*) FROM market_event WHERE symbol = 'MWG'").fetchone()[0]
    assert n <= 4, f"MWG có {n} event giả trên 51 window"


def test_dead_letter_reasons(result):
    conn, _ = result
    got = Counter((r["topic"], r["reason"]) for r in conn.execute("SELECT topic, reason FROM dlq"))
    assert got == Counter({
        ("stock-price-topic", "corrupt_json"): 1,
        ("stock-price-topic", "bad_volume"): 1,
        ("stock-price-topic", "ohlc_inconsistent"): 1,
        ("stock-price-topic", "unknown_symbol"): 1,
        ("news-topic", "corrupt_json"): 1,
        ("news-topic", "bad_published_at"): 1,
    })


def test_duplicate_and_republished_articles_counted_once(result):
    conn, _ = result
    assert conn.execute("SELECT COUNT(*) FROM articles WHERE article_id LIKE '%-repub'").fetchone()[0] == 0
    # Tổng số bài-mã trong bảng articles phải bằng tổng news_5m (mỗi bài chỉ được đếm 1 lần cho mỗi mã).
    n_articles = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    n_counted = conn.execute("SELECT SUM(news_5m) FROM signals").fetchone()[0]
    assert n_articles == n_counted

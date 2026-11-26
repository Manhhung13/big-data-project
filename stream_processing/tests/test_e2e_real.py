"""End-to-end trên DỮ LIỆU THẬT (Bronze của nhánh crawl-data): baseline -> phát lại 1 ngày -> streaming.

Ngày 05/11/2026 nằm trong giai đoạn hai nguồn (VNDirect + Mirae) cùng ghi một nến, nên test này kiểm tra
luôn bước loại nến trùng: OHLCV của stream phải khớp tuyệt đối với bản đã bỏ trùng tính bằng pandas.
Bỏ qua nếu chưa clone repo nhóm. Chạy mất vài phút.
"""
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
_DEFAULT = ROOT.parent if (ROOT.parent / "data" / "bronze").exists() else ROOT.parent / "big-data-project"
TEAM_REPO = Path(os.environ.get("BIGDATA_TEAM_REPO", _DEFAULT))
DAY = "2026-11-05"

pytestmark = pytest.mark.skipif(not (TEAM_REPO / "data" / "bronze" / "stock").exists(),
                                reason="chưa có repo nhóm (git clone big-data-project cạnh thư mục này)")


def _run(module: str, env: dict, *args: str):
    subprocess.run([sys.executable, "-X", "utf8", "-m", module, *args], cwd=ROOT, env=env, check=True)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    data = tmp_path_factory.mktemp("bigdata_real")
    env = {**os.environ, "BIGDATA_SOURCE": "real", "BIGDATA_DATA": str(data), "BIGDATA_REPLAY_DATE": DAY,
           "BIGDATA_MAX_FILES": "3", "PYTHONIOENCODING": "utf-8"}
    _run("batch.build_baseline", env)
    _run("replay.real_day", env)
    _run("streaming.run", env, "--mode", "availableNow", "--reset")
    conn = sqlite3.connect(data / "serving" / "serving.db")
    conn.row_factory = sqlite3.Row
    yield conn, data
    conn.close()


def test_raw_day_really_has_duplicate_sources(result):
    _, data = result
    raw = sum(1 for f in (data / "stream_in" / "stock").glob("part-*.json")
              for _ in f.read_text(encoding="utf-8").splitlines())
    clean = len(pd.read_csv(data / "live" / "bars_clean.csv"))
    assert raw > 1.5 * clean, (raw, clean)      # ngày này mỗi phút có ~2 nến


def test_ohlcv_matches_deduplicated_bronze(result):
    conn, data = result
    bars = pd.read_csv(data / "live" / "bars_clean.csv", parse_dates=["timestamp"])
    bars["window_start"] = bars["timestamp"].dt.floor("5min").dt.strftime("%Y-%m-%d %H:%M:%S")
    exp = bars.sort_values("timestamp").groupby(["symbol", "window_start"]).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"), n_bars=("close", "size")).reset_index()
    got = pd.read_sql("SELECT symbol, window_start, open, high, low, close, volume, n_bars "
                      "FROM signals WHERE n_bars IS NOT NULL", conn)
    merged = exp.merge(got, on=["symbol", "window_start"], suffixes=("_e", "_g"), how="outer", indicator=True)
    assert len(exp) == len(got) and (merged["_merge"] == "both").all()
    assert got["symbol"].nunique() == 30
    for col in ["open", "high", "low", "close", "volume", "n_bars"]:
        assert (merged[col + "_e"] - merged[col + "_g"]).abs().max() < 1e-9, col


def test_no_dead_letters_on_clean_bronze(result):
    conn, _ = result
    assert conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0] == 0


def test_flag_rate_is_in_expected_range(result):
    """Luật z >= 3 hoặc >= P99 trên dữ liệu thật: số window bị cờ phải nhỏ (vài %), không phải hàng loạt."""
    conn, _ = result
    n, flagged = conn.execute("SELECT COUNT(*), SUM(price_anomaly OR volume_anomaly) FROM signals "
                              "WHERE n_bars IS NOT NULL").fetchone()
    assert 0 < flagged < 0.08 * n, (flagged, n)

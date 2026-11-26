"""Sinh dữ liệu "realtime" của ngày LIVE và thả vào thư mục stream_in (thay cho producer đẩy vào Kafka).

  python -m simulator.gen_live              # ghi toàn bộ ngay lập tức (dùng với --mode availableNow)
  python -m simulator.gen_live --pace 2     # ghi 1 lô mỗi 2 giây (dùng với --mode continuous)

Mỗi file stock = nến của tất cả mã trong 5 phút; mỗi file news = các bài xuất bản trong 5 phút.
Ngoài dữ liệu sạch còn có dữ liệu "bẩn" cố ý (xem simulator/scenarios.py -> DIRTY).
"""
import argparse
import datetime as dt
import json
import shutil
import time

import numpy as np
import pandas as pd

from config import PATHS
from simulator.market import NewsFactory, _hhmm_to_min, gen_day_bars
from simulator.scenarios import DIRTY, LIVE_DATE, SCENARIOS, SEED, SIM_PARAMS


def _slot_key(ts: str) -> str:
    """'2026-11-09 10:36:00' -> lô 5 phút '2026-11-09 10:35:00'."""
    t = dt.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return t.replace(minute=t.minute - t.minute % 5, second=0).strftime("%Y-%m-%d %H:%M:%S")


def _at(hhmm: str) -> str:
    return f"{LIVE_DATE:%Y-%m-%d} {hhmm}:00"


def build_live():
    # RNG của live tách khỏi history nhưng cố định -> chạy lại ra cùng dữ liệu.
    rng = np.random.default_rng(SEED + 1)
    # Giá đóng cửa cuối lịch sử -> giá mở cửa ngày live liền mạch với lịch sử.
    hist = pd.read_csv(PATHS["history_stock"])
    last = hist.groupby("symbol")["close"].last().to_dict()

    # --- nến sạch (ground truth) ---
    shocks = {s: [] for s in SIM_PARAMS}
    for sc in SCENARIOS:
        a = _hhmm_to_min(sc["at"])
        shocks[sc["symbol"]].append((a, a + 5, sc["price_shock"], sc["volume_mult"]))
    clean = pd.concat(
        [gen_day_bars(rng, LIVE_DATE, sym, last[sym], shocks[sym]) for sym in SIM_PARAMS],
        ignore_index=True,
    ).sort_values(["timestamp", "symbol"]).reset_index(drop=True)

    # --- tin: nền + burst theo kịch bản ---
    nf = NewsFactory(rng)
    articles = nf.background_day(LIVE_DATE)
    for sc in SCENARIOS:
        if sc["burst"]:
            articles += nf.burst(LIVE_DATE, sc["symbol"], sc["burst"]["count"],
                                 sc["burst"]["start"], sc["burst"]["end"])
    articles.sort(key=lambda a: a["published_at"])

    # --- luồng stock: chia lô 5 phút, chèn dữ liệu bẩn ---
    def bar_json(row, **override):
        rec = {**row, "source": "sim", "ingest_ts": row["timestamp"]}
        rec.update(override)
        return json.dumps(rec, ensure_ascii=False)

    batches = {}
    for _, row in clean.iterrows():
        batches.setdefault(_slot_key(row["timestamp"]), []).append(bar_json(row.to_dict()))

    def find_bar(sym, hhmm):
        return clean[(clean.symbol == sym) & (clean.timestamp == _at(hhmm))].iloc[0].to_dict()

    d = DIRTY["duplicate_bar"]
    batches[_slot_key(_at(d["at"]))].append(bar_json(find_bar(d["symbol"], d["at"])))
    lb = DIRTY["late_bar"]
    batches[_slot_key(_at(lb["arrives_at"]))].append(
        bar_json(find_bar(lb["symbol"], lb["bar"]), volume=lb["volume"]))
    batches[_slot_key(_at(DIRTY["malformed_stock_line"]["arrives_at"]))].append(
        '{"symbol": "FPT", "timestamp": "2026-11-09 09:3')
    for bv in DIRTY["bad_value_bars"]:
        base = find_bar("HPG" if bv["symbol"] == "XYZ" else bv["symbol"], bv["at"])
        if bv["kind"] == "negative_volume":
            line = bar_json(base, volume=-500)
        elif bv["kind"] == "high_below_low":
            line = bar_json(base, high=base["low"] - 10)
        else:
            line = bar_json(base, symbol="XYZ")
        batches[_slot_key(_at(bv["at"]))].append(line)

    # --- luồng news: lô 5 phút + bài trùng + bài hỏng ---
    nbatches = {}
    for a in articles:
        nbatches.setdefault(_slot_key(a["published_at"]), []).append(json.dumps(a, ensure_ascii=False))
    mapped = next(a for a in articles if "FPT" in a["title"] and a["published_at"] >= _at("09:00"))
    key = _slot_key(mapped["published_at"])
    nbatches[key].append(json.dumps(mapped, ensure_ascii=False))                      # gửi lại y hệt
    repub = {**mapped, "article_id": mapped["article_id"] + "-repub", "source": "ndh",
             "url": mapped["url"] + "?utm=repub"}
    nbatches[key].append(json.dumps(repub, ensure_ascii=False))                       # đăng lại ở nguồn khác
    nbatches.setdefault(_slot_key(_at("09:10")), []).append('{"article_id": "bad-json", "title": "cut off')
    nbatches.setdefault(_slot_key(_at("09:15")), []).append(json.dumps(
        {"article_id": "no-time", "title": "Bài thiếu thời gian xuất bản", "content": "FPT", "source": "ndh"}))

    return clean, articles, batches, nbatches


def write_files(batches, nbatches, pace: float):
    for p in (PATHS["stream_in_stock"], PATHS["stream_in_news"]):
        shutil.rmtree(p, ignore_errors=True)
        p.mkdir(parents=True, exist_ok=True)
    keys = sorted(set(batches) | set(nbatches))
    for i, k in enumerate(keys):
        # ghi vào file tạm rồi đổi tên -> Spark không đọc phải file đang ghi dở
        for kind, data, folder in (("stock", batches, PATHS["stream_in_stock"]),
                                   ("news", nbatches, PATHS["stream_in_news"])):
            if k in data:
                tmp, final = folder / f".tmp-{i:04d}", folder / f"part-{i:04d}.json"
                tmp.write_text("\n".join(data[k]) + "\n", encoding="utf-8")
                tmp.rename(final)
        if pace:
            time.sleep(pace)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pace", type=float, default=0, help="giây giữa hai lô (0 = ghi tất cả ngay)")
    args = ap.parse_args()

    clean, articles, batches, nbatches = build_live()
    PATHS["live_truth"].parent.mkdir(parents=True, exist_ok=True)
    clean.to_csv(PATHS["live_bars_clean"], index=False)
    PATHS["live_truth"].write_text(json.dumps(
        {"live_date": str(LIVE_DATE), "scenarios": SCENARIOS, "dirty": DIRTY}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    write_files(batches, nbatches, args.pace)
    print(f"[live] {LIVE_DATE}: {len(clean):,} clean bars in {len(batches)} files, "
          f"{len(articles)} articles in {len(nbatches)} files -> {PATHS['stream_in_stock'].parent}")


if __name__ == "__main__":
    main()

"""Sinh dữ liệu LỊCH SỬ (không có sự kiện cấy) cho 5 mã -> đầu vào của batch dựng baseline.

  python -m simulator.gen_history
"""
import json

import numpy as np
import pandas as pd

from config import PATHS
from simulator.market import NewsFactory, gen_day_bars, weekdays_before
from simulator.scenarios import HISTORY_DAYS, LIVE_DATE, SEED, SIM_PARAMS


def main():
    rng = np.random.default_rng(SEED)
    days = weekdays_before(LIVE_DATE, HISTORY_DAYS)

    frames, last = [], {s: p["price"] for s, p in SIM_PARAMS.items()}
    for d in days:
        for sym in SIM_PARAMS:
            df = gen_day_bars(rng, d, sym, last[sym])
            last[sym] = float(df["close"].iloc[-1])
            frames.append(df)
    stock = pd.concat(frames, ignore_index=True)

    news_factory = NewsFactory(rng)
    articles = [a for d in days for a in news_factory.background_day(d)]

    PATHS["history_stock"].parent.mkdir(parents=True, exist_ok=True)
    stock.to_csv(PATHS["history_stock"], index=False)
    with open(PATHS["history_news"], "w", encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps(a, ensure_ascii=False) + "\n")
    print(f"[history] {len(days)} days, {len(stock):,} bars, {len(articles):,} articles "
          f"({days[0]} -> {days[-1]})")


if __name__ == "__main__":
    main()

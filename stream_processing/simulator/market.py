"""Bộ sinh dữ liệu giả lập: nến 1 phút (có hình dạng volume intraday dạng chữ U) và tin tức."""
import datetime as dt

import numpy as np
import pandas as pd

from simulator.scenarios import DISPLAY_NAMES, SIM_PARAMS

SESSIONS = ((9 * 60, 11 * 60 + 30), (13 * 60, 14 * 60 + 45))   # 09:00-11:30, 13:00-14:45


def weekdays_before(end_date: dt.date, n: int) -> list:
    days, d = [], end_date
    while len(days) < n:
        d -= dt.timedelta(days=1)
        if d.weekday() < 5:
            days.append(d)
    return sorted(days)


def trading_minutes(d: dt.date) -> list:
    return [dt.datetime.combine(d, dt.time(m // 60, m % 60))
            for a, b in SESSIONS for m in range(a, b)]


def _hhmm_to_min(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def vol_profile(minute_of_day: np.ndarray) -> np.ndarray:
    """Hoạt động cao ở đầu phiên và gần ATC, thấp giữa phiên."""
    m = minute_of_day.astype(float)
    open_boost = np.where(m < 660, np.exp(-(m - 540) / 20.0), 0.0)
    close_boost = np.where(m >= 780, np.exp(-(884 - m) / 15.0), 0.0)
    return 1.0 + 1.5 * open_boost + 1.2 * close_boost


def gen_day_bars(rng, d: dt.date, symbol: str, last_close: float, shocks: list = ()) -> pd.DataFrame:
    """Sinh nến 1 phút cho một ngày. shocks: [(start_min, end_min, price_shock, volume_mult)]."""
    p = SIM_PARAMS[symbol]
    ts = trading_minutes(d)
    n = len(ts)
    mod = np.array([t.hour * 60 + t.minute for t in ts])
    prof = vol_profile(mod)
    sig = p["sigma"] * np.sqrt(prof)

    r = sig * rng.standard_t(5, size=n) / np.sqrt(5 / 3)      # return có đuôi dày, phương sai đơn vị
    r_organic = r.copy()   # volume chỉ gắn với biến động "tự nhiên"; cú sốc cấy vào được điều khiển riêng bằng vol_mult
    vmult = np.ones(n)
    for start, end, price_shock, vol_mult in shocks:
        mask = (mod >= start) & (mod < end)
        k = int(mask.sum())
        if k:
            r[mask] += price_shock / k                         # tổng dịch chuyển ~ price_shock trong window
            vmult[mask] *= vol_mult

    p0 = last_close * (1 + rng.normal(0, 0.004))               # gap mở cửa
    close = np.round(p0 * np.exp(np.cumsum(r)))
    open_ = np.concatenate([[round(p0)], close[:-1]])
    wick = np.abs(rng.normal(0, sig * 0.5)) * rng.random(n)
    high = np.maximum.reduce([np.round(np.maximum(open_, close) * (1 + wick)), open_, close])
    low = np.minimum.reduce([np.round(np.minimum(open_, close) * (1 - wick)), open_, close])
    volume = p["vol"] * prof * np.exp(rng.normal(0, 0.55, n)) * (1 + 0.5 * np.abs(r_organic) / sig) * vmult
    return pd.DataFrame({
        "symbol": symbol,
        "timestamp": [t.strftime("%Y-%m-%d %H:%M:%S") for t in ts],
        "open": open_, "high": high, "low": low, "close": close,
        "volume": np.maximum(np.round(volume), 100),
    })


# ---------------------------------------------------------------- tin tức
SOURCES = ["cafef", "vietstock", "ndh"]
TEMPLATES = [
    "{name} công bố kết quả kinh doanh quý {q}, lợi nhuận tăng {a}% so với cùng kỳ",
    "{name}: cổ đông lớn đăng ký mua thêm {a} nghìn cổ phiếu {sym}",
    "Khối ngoại giao dịch sôi động cổ phiếu {sym} trong phiên, giá trị {a} tỷ đồng",
    "{name} ký kết hợp tác chiến lược mới, tổng giá trị {a} tỷ đồng",
    "Nhận định cổ phiếu {sym}: vùng hỗ trợ quanh {a} nghìn đồng",
    "{name} điều chỉnh kế hoạch đầu tư, dự kiến {a} tỷ đồng trong năm nay",
]
MARKET_TEMPLATES = [
    "VN-Index {dir} {a} điểm trong phiên, thanh khoản toàn thị trường đạt {b} tỷ đồng",
    "Ngân hàng Nhà nước giữ nguyên lãi suất điều hành, thị trường kỳ vọng {a} điểm hỗ trợ",
]


class NewsFactory:
    """Sinh bài viết; mỗi bài có tiêu đề + nội dung khác nhau (để bước loại trùng theo nội dung không gộp nhầm)."""

    def __init__(self, rng):
        self.rng = rng
        self.counter = 0

    def article(self, symbol, when: dt.datetime, with_other: str = None) -> dict:
        self.counter += 1
        r = self.rng
        a, b, q = int(r.integers(5, 900)), int(r.integers(5000, 30000)), int(r.integers(1, 5))
        if symbol is None:
            title = MARKET_TEMPLATES[int(r.integers(len(MARKET_TEMPLATES)))].format(
                dir=r.choice(["tăng", "giảm"]), a=a, b=b)
            body = f"Diễn biến chung của thị trường trong ngày. Chi tiết số liệu thứ {a}-{b}."
        else:
            name = DISPLAY_NAMES[symbol]
            title = TEMPLATES[int(r.integers(len(TEMPLATES)))].format(name=name, sym=symbol, q=q, a=a)
            body = f"Theo thông tin mới nhất, {name} ({symbol}) có diễn biến đáng chú ý, số liệu {a}-{b}."
            if with_other:
                body += f" Đồng thời {DISPLAY_NAMES[with_other]} cũng được nhắc tới."
        src = SOURCES[int(r.integers(len(SOURCES)))]
        lag = int(r.integers(20, 90))
        return {
            "article_id": f"{src}-{when:%Y%m%d}-{self.counter:06d}",
            "title": f"{title} (#{self.counter})",
            "content": body,
            "published_at": when.strftime("%Y-%m-%d %H:%M:%S"),
            "source": src,
            "url": f"https://sim.local/{src}/{when:%Y%m%d}/{self.counter}",
            "crawled_at": (when + dt.timedelta(seconds=lag)).strftime("%Y-%m-%d %H:%M:%S"),
        }

    def background_day(self, d: dt.date) -> list:
        """Tin nền: Poisson theo slot 5 phút, nhiều hơn trong giờ giao dịch."""
        out, r = [], self.rng
        symbols = list(SIM_PARAMS)
        slots = [dt.datetime.combine(d, dt.time(0, 0)) + dt.timedelta(minutes=5 * i)
                 for i in range(7 * 12, 20 * 12)]                        # 07:00-20:00
        for slot in slots:
            day = 8 <= slot.hour < 17
            for sym in symbols:
                lam = SIM_PARAMS[sym]["news_rate"] * (1.0 if day else 0.2)
                for _ in range(int(r.poisson(lam))):
                    other = None
                    if r.random() < 0.05:
                        other = r.choice([s for s in symbols if s != sym])
                    out.append(self.article(sym, slot + dt.timedelta(seconds=int(r.integers(0, 300))), other))
            for _ in range(int(r.poisson(0.15 if day else 0.03))):
                out.append(self.article(None, slot + dt.timedelta(seconds=int(r.integers(0, 300)))))
        return out

    def burst(self, d: dt.date, symbol: str, count: int, start: str, end: str) -> list:
        a, b = _hhmm_to_min(start) * 60, _hhmm_to_min(end) * 60
        secs = sorted(int(x) for x in self.rng.integers(a, b + 1, size=count))
        return [self.article(symbol, dt.datetime.combine(d, dt.time(0, 0)) + dt.timedelta(seconds=s))
                for s in secs]

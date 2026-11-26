"""Tham số mô phỏng: 5 mã cổ phiếu + các kịch bản bất thường được "cấy" vào ngày live.

Mỗi kịch bản có đáp án biết trước (ground truth) để kiểm tra hệ thống có phát hiện đúng loại sự kiện không.
Mã MWG không có kịch bản nào -> dùng làm nhóm đối chứng để đếm báo động giả.
"""
import datetime as dt

HISTORY_DAYS = 120                       # số ngày giao dịch (T2-T6) dùng dựng baseline
LIVE_DATE = dt.date(2026, 11, 9)         # ngày "chạy realtime" (thứ Hai)
SEED = 20261109

# price: giá nền (VND), sigma: độ lệch chuẩn return 1 phút, vol: volume nền 1 phút
SIM_PARAMS = {
    "FPT": {"price": 120_000, "sigma": 0.0009, "vol": 15_000, "news_rate": 0.10},
    "VCB": {"price": 90_000, "sigma": 0.0007, "vol": 12_000, "news_rate": 0.10},
    "HPG": {"price": 27_000, "sigma": 0.0013, "vol": 90_000, "news_rate": 0.10},
    "VNM": {"price": 65_000, "sigma": 0.0008, "vol": 20_000, "news_rate": 0.10},
    "MWG": {"price": 60_000, "sigma": 0.0012, "vol": 18_000, "news_rate": 0.10},
}

# Tên dùng trong tiêu đề bài viết mô phỏng (khớp alias trong config.SYMBOLS).
DISPLAY_NAMES = {
    "FPT": "Tập đoàn FPT", "VCB": "Vietcombank", "HPG": "Hòa Phát",
    "VNM": "Vinamilk", "MWG": "Thế Giới Di Động",
}

# at: phút bắt đầu window 5 phút bị tác động. burst: bài tin dồn dập trong [start, end].
SCENARIOS = [
    {   # Composite: giá + khối lượng + tin cùng bất thường
        "symbol": "FPT", "at": "10:35", "expect": "PRICE+VOLUME+NEWS",
        "price_shock": +0.015, "volume_mult": 8.0,
        "burst": {"count": 6, "start": "10:12", "end": "10:34"},
    },
    {   # Chỉ giá biến động mạnh, không có volume/tin bất thường
        "symbol": "VCB", "at": "13:20", "expect": "PRICE_ONLY",
        "price_shock": -0.012, "volume_mult": 1.0, "burst": None,
    },
    {   # Volume đột biến + tin dồn dập, giá đi ngang
        "symbol": "HPG", "at": "11:00", "expect": "NEWS+VOLUME",
        "price_shock": 0.0, "volume_mult": 10.0,
        "burst": {"count": 5, "start": "10:40", "end": "10:58"},
    },
    {   # Chỉ tin dồn dập, thị trường không phản ứng
        "symbol": "VNM", "at": "14:00", "expect": "NEWS_ONLY",
        "price_shock": 0.0, "volume_mult": 1.0,
        "burst": {"count": 6, "start": "13:35", "end": "13:58"},
    },
]

# Dữ liệu "bẩn" cố ý chèn thêm vào luồng live để kiểm tra dedup / dead-letter / dữ liệu trễ.
DIRTY = {
    "duplicate_bar": {"symbol": "FPT", "at": "09:50"},          # collector gửi lại cùng 1 nến
    "late_bar": {"symbol": "FPT", "bar": "10:36", "arrives_at": "11:20", "volume": 9_999_999},
    "malformed_stock_line": {"arrives_at": "09:30"},            # JSON bị cắt dở
    "bad_value_bars": [                                          # sai giá trị nghiệp vụ
        {"symbol": "HPG", "at": "09:41", "kind": "negative_volume"},
        {"symbol": "MWG", "at": "10:02", "kind": "high_below_low"},
        {"symbol": "XYZ", "at": "10:10", "kind": "unknown_symbol"},
    ],
}

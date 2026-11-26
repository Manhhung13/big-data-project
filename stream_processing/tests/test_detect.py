"""Unit test cho logic phát hiện, map bài -> mã và feature window (DataFrame tĩnh, không cần streaming)."""
import pytest
from pyspark.sql import functions as F

from streaming.detect import detect_news, detect_stock
from streaming.features import window_features
from streaming.news_mapping import content_hash, symbols_in_text

BASELINE_DDL = (
    "symbol string, time_bucket string, n_samples_stock int, mean_return double, std_return double, "
    "p99_abs_return double, mean_volume double, std_volume double, p99_volume double, "
    "n_samples_news int, mean_news_30m double, std_news_30m double, p99_news_30m double"
)


@pytest.fixture
def baseline(make_df):
    full = dict(n_samples_stock=120, mean_return=0.0, std_return=0.002, p99_abs_return=0.005,
                mean_volume=100_000.0, std_volume=20_000.0, p99_volume=160_000.0,
                n_samples_news=120, mean_news_30m=0.5, std_news_30m=0.7, p99_news_30m=3.0)
    rows = [
        {"symbol": "FPT", "time_bucket": "10:30", **full},                                  # bình thường
        {"symbol": "FPT", "time_bucket": "10:35", **{**full, "std_return": 0.0, "std_volume": 0.0,
                                                    "std_news_30m": 0.0}},                  # std = 0
        {"symbol": "FPT", "time_bucket": "10:40", **{**full, "n_samples_stock": 10,
                                                    "n_samples_news": 10}},                 # quá ít mẫu
    ]
    return make_df(rows, BASELINE_DDL)


STOCK_DDL = "symbol string, time_bucket string, ret double, volume double"


def run_stock(make_df, baseline, rows):
    df = make_df(rows, STOCK_DDL)
    out = detect_stock(df, baseline).collect()
    return {(r.symbol, r.time_bucket, round(r.ret, 6)): r for r in out}


def test_price_anomaly_by_zscore(make_df, baseline):
    # ret = 0.007 -> z = 3.5 (>= 3); 0.003 -> z = 1.5 và < P99 -> không flag
    res = run_stock(make_df, baseline, [
        {"symbol": "FPT", "time_bucket": "10:30", "ret": 0.007, "volume": 100_000.0},
        {"symbol": "FPT", "time_bucket": "10:30", "ret": 0.003, "volume": 100_000.0},
    ])
    assert res[("FPT", "10:30", 0.007)].price_anomaly is True
    assert res[("FPT", "10:30", 0.003)].price_anomaly is False


def test_price_anomaly_by_percentile_only(make_df, baseline):
    # |ret| = 0.0055 > P99 (0.005) dù z = 2.75 < 3 -> vẫn flag; âm cũng phải flag (dùng |ret|)
    res = run_stock(make_df, baseline, [
        {"symbol": "FPT", "time_bucket": "10:30", "ret": -0.0055, "volume": 100_000.0},
    ])
    r = res[("FPT", "10:30", -0.0055)]
    assert abs(r.return_z) < 3 and r.price_anomaly is True


def test_volume_anomaly(make_df, baseline):
    res = run_stock(make_df, baseline, [
        {"symbol": "FPT", "time_bucket": "10:30", "ret": 0.0, "volume": 170_000.0},   # >= P99, z = 3.5
        {"symbol": "FPT", "time_bucket": "10:30", "ret": 0.0001, "volume": 110_000.0},
    ])
    assert res[("FPT", "10:30", 0.0)].volume_anomaly is True
    assert res[("FPT", "10:30", 0.0001)].volume_anomaly is False


def test_volume_drop_is_not_anomaly(make_df, baseline):
    # Chỉ báo khối lượng TĂNG bất thường (z >= 3), không báo khi khối lượng thấp.
    res = run_stock(make_df, baseline, [{"symbol": "FPT", "time_bucket": "10:30", "ret": 0.0, "volume": 1_000.0}])
    assert res[("FPT", "10:30", 0.0)].volume_anomaly is False


@pytest.mark.parametrize("bucket", ["10:35", "10:40", "23:55"])   # std=0 / ít mẫu / không có baseline
def test_no_flag_without_reliable_baseline(make_df, baseline, bucket):
    res = run_stock(make_df, baseline, [
        {"symbol": "FPT", "time_bucket": bucket, "ret": 0.05, "volume": 9_000_000.0}])
    r = res[("FPT", bucket, 0.05)]
    assert r.price_anomaly is False and r.volume_anomaly is False
    assert r.return_z is None and r.volume_z is None


NEWS_DDL = "symbol string, time_bucket string, news_30m int"


def run_news(make_df, baseline, rows):
    out = detect_news(make_df(rows, NEWS_DDL), baseline).collect()
    return {(r.time_bucket, r.news_30m): r for r in out}


def test_news_burst(make_df, baseline):
    res = run_news(make_df, baseline, [
        {"symbol": "FPT", "time_bucket": "10:30", "news_30m": 6},    # z ~ 7.9, >= min count
        {"symbol": "FPT", "time_bucket": "10:30", "news_30m": 3},    # z ~ 3.6 nhưng < NEWS_MIN_COUNT_30M (4)
        {"symbol": "FPT", "time_bucket": "10:30", "news_30m": 1},
    ])
    assert res[("10:30", 6)].news_burst is True
    assert res[("10:30", 3)].news_burst is False
    assert res[("10:30", 1)].news_burst is False


def test_news_burst_needs_reliable_baseline(make_df, baseline):
    res = run_news(make_df, baseline, [
        {"symbol": "FPT", "time_bucket": "10:35", "news_30m": 9},    # std = 0
        {"symbol": "FPT", "time_bucket": "10:40", "news_30m": 9},    # ít mẫu
        {"symbol": "FPT", "time_bucket": "03:00", "news_30m": 9},    # không có baseline
    ])
    assert all(r.news_burst is False for r in res.values())


# --------------------------------------------------------------- map bài -> mã
def symbols_of(make_df, texts):
    df = make_df([{"text": t} for t in texts], "text string")
    return [sorted(r.s) for r in df.select(symbols_in_text(F.col("text")).alias("s")).collect()]


def test_symbol_mapping(make_df):
    got = symbols_of(make_df, [
        "FPT công bố lợi nhuận quý 3",                         # ticker
        "Vietcombank điều chỉnh lãi suất",                     # tên công ty
        "vietcombank viết thường vẫn khớp",                    # tên: không phân biệt hoa/thường
        "Thế Giới Di Động mở thêm cửa hàng",                   # tên nhiều từ, có dấu
        "fpt viết thường",                                     # ticker: PHẢI viết hoa
        "FPTX không phải mã FPT",                              # \b: 'FPTX' không khớp, nhưng 'FPT' đứng riêng thì có
        "VN-Index tăng 10 điểm",                               # không nhắc mã nào
        "Hòa Phát và Vinamilk cùng tăng",                      # nhiều mã trong một bài
    ])
    assert got == [["FPT"], ["VCB"], ["VCB"], ["MWG"], [], ["FPT"], [], ["HPG", "VNM"]]


def test_symbol_mapping_word_boundary(make_df):
    assert symbols_of(make_df, ["XFPT và FPTX và HPGX"]) == [[]]


def test_content_hash_ignores_case_and_whitespace(make_df):
    df = make_df([{"t": "FPT  Tin  Nóng", "c": "Nội dung"}, {"t": "fpt tin nóng", "c": "nội  dung"},
                  {"t": "FPT tin khác", "c": "Nội dung"}], "t string, c string")
    h = [r.h for r in df.select(content_hash(F.col("t"), F.col("c")).alias("h")).collect()]
    assert h[0] == h[1] and h[0] != h[2]


# --------------------------------------------------------------- feature window
def test_window_features_uses_event_time_order(make_df):
    # Các nến đến LỘN THỨ TỰ: open phải là nến sớm nhất, close là nến muộn nhất.
    rows = [
        {"symbol": "FPT", "ts": "2026-11-09 10:38:00", "open": 14, "high": 15, "low": 13, "close": 14.5, "volume": 400},
        {"symbol": "FPT", "ts": "2026-11-09 10:35:00", "open": 10, "high": 11, "low": 9.5, "close": 10.5, "volume": 100},
        {"symbol": "FPT", "ts": "2026-11-09 10:36:00", "open": 10.5, "high": 12, "low": 10, "close": 11, "volume": 200},
        {"symbol": "FPT", "ts": "2026-11-09 10:40:00", "open": 99, "high": 99, "low": 99, "close": 99, "volume": 1},
    ]
    df = make_df(rows, "symbol string, ts string, open double, high double, low double, close double, volume double")
    df = df.withColumn("ts", F.to_timestamp("ts"))
    # Dùng time_bucket (chuỗi do Spark tạo theo session timezone) thay vì datetime của Python
    # vì collect() timestamp ở Python dùng timezone của máy, không phải của Spark.
    r = {x.time_bucket: x for x in window_features(df).collect()}
    w = r["10:35"]
    assert (w.open, w.close, w.high, w.low, w.volume, w.n_bars) == (10, 14.5, 15, 9.5, 700, 3)
    assert w.time_bucket == "10:35" and w.ret == pytest.approx(14.5 / 10 - 1)
    assert r["10:40"].n_bars == 1       # nến 10:40 thuộc window kế tiếp


# --------------------------------------------------------------- dữ liệu thật (nhánh crawl-data)
def test_event_time_accepts_real_formats_and_floors_to_minute(make_df):
    from streaming.schemas import STOCK_SCHEMA, parse_stock
    rows = [
        {"symbol": "FPT", "timestamp": "2026-11-05 09:15:00+07:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10},
        {"symbol": "FPT", "timestamp": "2026-11-05 09:15:56+07:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10},
        {"symbol": "FPT", "timestamp": "2026-11-05 09:16:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10},
        {"symbol": "FPT", "timestamp": "không phải giờ", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10},
    ]
    df = parse_stock(make_df(rows, STOCK_SCHEMA))
    got = [(r.t, r.invalid_reason) for r in df.select(F.date_format("ts", "HH:mm:ss").alias("t"), "invalid_reason").collect()]
    # Nến Mirae 09:15:56 và VNDirect 09:15:00 là cùng một nến -> cùng ts sau khi tròn phút.
    assert got == [("09:15:00", None), ("09:15:00", None), ("09:16:00", None), (None, "bad_timestamp")]


def test_news_published_at_formats(make_df):
    from streaming.schemas import NEWS_SCHEMA, parse_news
    rows = [{"article_id": "a", "title": "t", "published_at": "2026-11-26T11:04:00"},
            {"article_id": "b", "title": "t", "published_at": "2026-11-25T00:12:00.000000"}]
    df = parse_news(make_df(rows, NEWS_SCHEMA))
    assert [r.t for r in df.select(F.date_format("published_ts", "yyyy-MM-dd HH:mm").alias("t")).collect()] == \
        ["2026-11-26 11:04", "2026-11-25 00:12"]


def test_ambiguous_ticker_needs_context(make_df):
    syms = {"VND": {"names": ["VNDIRECT"], "ticker_context": r"(?:[Cc]ổ phiếu|[Mm]ã|CP)\s+VND\b|\(VND\)"},
            "FPT": {"names": []}}
    df = make_df([{"text": t} for t in [
        "Ngân hàng Nhà nước bơm thêm VND, chứng khoán bật tăng",     # tiền đồng -> không phải mã
        "Cổ phiếu VND bật tăng kịch trần",                             # có ngữ cảnh
        "Chứng khoán VNDirect (VND) họp bất thường",                   # alias tên + (VND)
        "FPT và VND",                                                  # VND không ngữ cảnh -> chỉ FPT
    ]], "text string")
    got = [sorted(r.s) for r in df.select(symbols_in_text(F.col("text"), syms).alias("s")).collect()]
    assert got == [[], ["VND"], ["VND"], ["FPT"]]


def test_reference_symbols_loader(tmp_path):
    import config
    p = tmp_path / "ref.csv"
    p.write_text("﻿symbol,company_name,aliases,industry\n"
                 "HPG,Hòa Phát,HPG|Hòa Phát|Dung Quất,Thép\nVND,VNDIRECT,VND|VNDIRECT,CK\n", encoding="utf-8")
    ref = config._load_reference_symbols(p)
    assert ref["HPG"]["names"] == ["Hòa Phát"]                    # bỏ ticker và alias bị loại
    assert "ticker_context" in ref["VND"] and ref["VND"]["names"] == ["VNDIRECT"]

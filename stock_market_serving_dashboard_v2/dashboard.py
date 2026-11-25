"""Elasticsearch serving dashboard for crawled and separately stored demo data."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from elasticsearch import NotFoundError
from es_common import es_client, index_names
from queries import (available_data, price_series, news_density_series, recent_events,
                     related_articles, search_articles, event_summary, news_activity)

VN = ZoneInfo("Asia/Ho_Chi_Minh")
EVENT_TYPES = ["PRICE_ONLY", "VOLUME_ONLY", "NEWS_ONLY", "PRICE_VOLUME",
               "PRICE_NEWS", "NEWS_VOLUME", "COMPOSITE"]
st.set_page_config(page_title="VN Market Events", layout="wide")
st.title("Theo dõi thị trường và sự kiện cổ phiếu")
st.caption("Elasticsearch Serving Layer · Giá, khối lượng, tin tức và phân tích lịch sử")


@st.cache_resource
def client():
    return es_client()


def dataframe(items, timecol="time"):
    frame = pd.DataFrame(items)
    if not frame.empty and timecol in frame:
        frame[timecol] = pd.to_datetime(frame[timecol], format="mixed", utc=True).dt.tz_convert(VN)
    return frame


def display_table(items, columns):
    frame = pd.DataFrame(items).reindex(columns=columns)
    for col in ("event_time", "published_at", "processed_at"):
        if col in frame:
            frame[col] = pd.to_datetime(frame[col], format="mixed", utc=True).dt.tz_convert(VN)
    config = {"url": st.column_config.LinkColumn("Bài viết", display_text="Mở bài viết")} if "url" in frame else {}
    st.dataframe(frame, hide_index=True, width="stretch", column_config=config)


def bar(frame, x, y, label):
    fig = go.Figure(go.Bar(x=frame[x], y=frame[y], name=label))
    fig.update_layout(height=300, margin=dict(l=12, r=12, t=16, b=12),
                      xaxis_title="Thời gian Việt Nam (UTC+7)" if x == "time" else "Mã cổ phiếu",
                      yaxis_title=label)
    return fig


def price_chart(frame, events, interval):
    fig = go.Figure(go.Candlestick(x=frame.time, open=frame.open, high=frame.high,
                                   low=frame.low, close=frame.close, name="OHLC"))
    # Place an event at its actual time only if that chart bucket contains prices.
    duration = {"5m": pd.Timedelta(minutes=5), "1h": pd.Timedelta(hours=1),
                "1d": pd.Timedelta(days=1)}[interval]
    for event in events:
        moment = pd.Timestamp(event["event_time"]).tz_convert(VN)
        bucket = frame[(frame.time <= moment) & (moment < frame.time + duration)]
        if not bucket.empty:
            fig.add_scatter(x=[moment], y=[bucket.iloc[-1].close], mode="markers",
                            marker=dict(symbol="x", size=10, color="#9654da"), showlegend=False,
                            name=event["event_type"], hovertext=event["event_type"])
    fig.update_layout(height=380, xaxis_rangeslider_visible=False,
                      xaxis_title="Thời gian Việt Nam (UTC+7)", yaxis_title="Giá theo đơn vị nguồn")
    return fig


dataset = st.sidebar.selectbox("Bộ dữ liệu", ["real", "demo"],
                              format_func=lambda x: "Dữ liệu crawl" if x == "real" else "Demo giả lập")
price_ix, feature_ix, news_ix, density_ix, event_ix = index_names(dataset)
if dataset == "demo":
    st.warning("DEMO GIẢ LẬP: giá, tin và sự kiện không phản ánh thị trường thực.")
else:
    st.info("Dữ liệu crawl lịch sử. Baseline và sự kiện chỉ xuất hiện khi đã nạp kết quả xử lý upstream.")

es = client()
try:
    symbols, first, last = available_data(es, price_ix)
except NotFoundError:
    st.warning("Chưa khởi tạo bộ dữ liệu này. Xem lệnh nạp trong README.md.")
    st.stop()
except Exception as exc:
    st.error(f"Không truy vấn được Elasticsearch. Kiểm tra ES_URL và docker compose ps. {exc}")
    st.stop()
if not symbols or not first or not last:
    st.warning("Chưa có dữ liệu giá. Chạy import_parquet.py --load để nạp dữ liệu crawl.")
    st.stop()

symbol = st.sidebar.selectbox("Mã cổ phiếu", symbols, index=symbols.index("FPT") if "FPT" in symbols else 0)
first_date = pd.Timestamp(first).tz_convert(VN).date()
last_date = pd.Timestamp(last).tz_convert(VN).date()
mode = st.sidebar.selectbox("Khoảng quan sát", ["7 ngày cuối dữ liệu", "30 ngày cuối dữ liệu", "Toàn bộ lịch sử", "Tùy chọn", "Gần hiện tại (24 giờ)"])
if mode == "Tùy chọn":
    dates = st.sidebar.date_input("Từ ngày / đến hết ngày", value=(max(first_date, last_date-timedelta(days=6)), last_date),
                                 min_value=first_date, max_value=max(last_date, datetime.now(VN).date()),
                                 key=f"dates-{dataset}")
    if len(dates) != 2:
        st.info("Chọn đủ ngày bắt đầu và ngày kết thúc.")
        st.stop()
    day_start, day_end = dates
elif mode == "Toàn bộ lịch sử":
    day_start, day_end = first_date, last_date
else:
    day_end = last_date
    day_start = max(first_date, day_end - timedelta(days=29 if mode.startswith("30") else 6))
start = datetime.combine(day_start, datetime.min.time(), tzinfo=VN).astimezone(timezone.utc).isoformat()
end = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=VN).astimezone(timezone.utc).isoformat()
span = (day_end-day_start).days+1
interval = "5m" if span <= 2 else "1h" if span <= 14 else "1d"
st.sidebar.caption(f"Dữ liệu đã nạp: {first_date:%d/%m/%Y} – {last_date:%d/%m/%Y}\n\nĐộ gộp biểu đồ: {interval}. Múi giờ Việt Nam (UTC+7).")
st.sidebar.caption("Giá và volume giữ nguyên đơn vị nguồn. Chưa xác nhận điều chỉnh giá và ngữ nghĩa volume với crawler.")


@st.fragment(run_every="60s")
def live_panel(symbol, start, end, interval, realtime):
    if realtime:
        now = datetime.now(timezone.utc)
        start, end, interval = (now-timedelta(days=1)).isoformat(), now.isoformat(), "5m"
    try:
        prices = dataframe(price_series(es, price_ix, symbol, start, interval, end))
        density = dataframe(news_density_series(es, density_ix, symbol, start, interval, end))
        market = event_summary(es, event_ix, start, end)
        summary = event_summary(es, event_ix, start, end, symbol)
        activity = news_activity(es, news_ix, start, end)
        events, total = recent_events(es, event_ix, symbol, start, end=end)
        st.caption(f"Cập nhật {datetime.now(VN):%d/%m/%Y %H:%M:%S} (UTC+7) · Tự truy vấn mỗi 60 giây · Khoảng [bắt đầu, kết thúc)")
        overview, detail, news, history = st.tabs(["Tổng quan thị trường", "Chi tiết cổ phiếu và sự kiện", "Tin tức", "Phân tích lịch sử"])

        with overview:
            types = market["event_types"]["buckets"]
            composites = next((b["doc_count"] for b in types if b["key"] == "COMPOSITE"), 0)
            columns = st.columns(4)
            for col, label, value in zip(columns, ["Sự kiện toàn thị trường", "Sự kiện composite", "Bài viết duy nhất (xấp xỉ)", "Bài chưa ánh xạ mã"],
                                         [market["total"], composites, activity["articles"]["value"], activity["unmapped"]["doc_count"]]):
                col.metric(label, f"{value:,}")
            st.caption("Tổng quan toàn bộ mã trong cùng khoảng ngày. Một bài có thể liên quan nhiều mã; số bài duy nhất dùng cardinality.")
            left, right = st.columns(2)
            with left:
                st.subheader("Sự kiện theo ngày")
                buckets = market["by_day"]["buckets"]
                if buckets:
                    st.plotly_chart(bar(dataframe([dict(time=b["key_as_string"], count=b["doc_count"]) for b in buckets]), "time", "count", "Số sự kiện"), width="stretch")
                else:
                    st.info("Chưa có sự kiện được nạp trong khoảng này; chưa thể kết luận thị trường không có bất thường.")
            with right:
                st.subheader("Mã có nhiều sự kiện")
                buckets = market["top_symbols"]["buckets"]
                if buckets:
                    st.plotly_chart(bar(pd.DataFrame([dict(symbol=b["key"], count=b["doc_count"]) for b in buckets]), "symbol", "count", "Số sự kiện"), width="stretch")
                else:
                    st.info("Cần kết quả market_event từ Batch/Speed Layer.")

        with detail:
            columns = st.columns(3)
            columns[0].metric("Giá đóng cửa gần nhất", f"{prices.iloc[-1].close:,.3f}" if not prices.empty else "—")
            columns[1].metric("Tổng volume theo nguồn", f"{prices.volume.sum():,.0f}" if not prices.empty else "—")
            columns[2].metric("Sự kiện có price anomaly", summary["price_anomalies"]["doc_count"])
            if not prices.empty:
                st.plotly_chart(price_chart(prices, events, interval), width="stretch")
                st.caption(f"OHLCV gộp theo {interval}; marker ở thời điểm event và giá đóng cửa của bucket chứa event. Tối đa 200 marker gần nhất.")
                st.plotly_chart(bar(prices, "time", "volume", "Volume"), width="stretch")
                movement = (prices.iloc[-1].close / prices.iloc[0].open - 1) * 100
                st.metric("Thay đổi từ giá mở đầu đến giá đóng cuối khoảng", f"{movement:+.2f}%")
            else:
                st.info("Không có OHLCV trong khoảng đã chọn.")
            st.subheader("Danh sách sự kiện")
            chosen = st.multiselect("Loại sự kiện", EVENT_TYPES, default=EVENT_TYPES, key="event-types")
            statuses = st.multiselect("Trạng thái", ["OPEN", "COMPLETED"], default=["OPEN", "COMPLETED"], key="event-status")
            filtered, found = recent_events(es, event_ix, symbol, start, end=end, event_types=chosen, statuses=statuses)
            st.caption(f"Hiển thị {len(filtered):,}/{found:,} sự kiện phù hợp. Thống kê được tính trên toàn bộ dữ liệu trong khoảng.")
            if filtered:
                display_table(filtered, ["event_time", "event_type", "status", "price_anomaly", "volume_anomaly", "news_burst", "return_zscore", "volume_zscore", "news_zscore", "origin"])
            else:
                st.info("Chưa có sự kiện phù hợp. Bản nạp Bronze không tự tính anomaly.")

        with news:
            st.subheader("Mật độ bài viết theo mã")
            if not density.empty:
                st.plotly_chart(bar(density, "time", "news_count", "Số bài liên quan"), width="stretch")
            else:
                st.info("Chưa có bài được ánh xạ tới mã này trong khoảng đã chọn.")
            st.caption("Chỉ cộng cửa sổ 5 phút không chồng lắp. Chỉ số phản ánh bài đã crawl và ánh xạ, chưa chứng minh độ phủ nguồn tin.")
            text = st.text_input("Tìm tiêu đề hoặc nội dung", key="news-search")
            all_market = st.checkbox("Tìm trên tất cả bài viết, kể cả bài chưa ánh xạ", key="all-news")
            articles, count = search_articles(es, news_ix, None if all_market else symbol, start, end, text)
            st.caption(f"Hiển thị {len(articles):,}/{count:,} bản ánh xạ bài viết (tối đa 100).")
            if articles:
                display_table(articles, ["published_at", "symbol", "title", "source", "url", "mapping_rule"])
            if events:
                selected = st.selectbox("Tin quanh sự kiện (±30 phút)", events,
                    format_func=lambda e: f"{pd.Timestamp(e['event_time']).tz_convert(VN):%d/%m %H:%M} · {e['event_type']} · {e['status']}",
                    key=f"related-{dataset}-{symbol}")
                related, count = related_articles(es, news_ix, symbol, selected["event_time"])
                st.caption(f"{len(related)}/{count} bài trong cửa sổ ±30 phút. Liên kết theo mã và published_at không chứng minh quan hệ nhân quả.")
                if related:
                    display_table(related, ["published_at", "title", "source", "url"])

        with history:
            st.subheader(f"Phân tích lịch sử {symbol}")
            if not summary["total"]:
                st.info("Chưa có historical_events/event_analysis từ upstream. Các ô trống không phải kết quả bằng 0.")
            else:
                counts = summary["event_types"]["buckets"]
                fig = go.Figure(go.Bar(x=[b["key"] for b in counts], y=[b["doc_count"] for b in counts]))
                fig.update_layout(height=300, yaxis_title="Số sự kiện", xaxis_title="Tổ hợp tín hiệu")
                st.plotly_chart(fig, width="stretch")
                completed = summary["completed"]
                before, after = completed["before"], completed["after"]
                left, right = st.columns(2)
                left.metric(f"Return trước 30 phút · n={before['count']}", f"{100*before['avg']:+.3f}%" if before["count"] else "—")
                right.metric(f"Return sau 30 phút · n={after['count']}", f"{100*after['avg']:+.3f}%" if after["count"] else "—")
                st.caption("Trung bình trên toàn bộ event COMPLETED có chỉ số tương ứng; mẫu trước và sau có thể khác nhau.")
                display_table(events, ["event_time", "event_type", "status", "return_before_30m", "return_after_30m", "return_after_1h", "return_after_1d", "news_count_before_30m", "news_count_after_30m"])
                st.caption(f"Bảng chi tiết hiển thị {len(events)}/{total} sự kiện. Chỉ số hậu sự kiện do upstream cung cấp khi đủ quan sát.")
    except Exception as exc:
        st.error(f"Không tải được dữ liệu: {exc}")


live_panel(symbol, start, end, interval, mode == "Gần hiện tại (24 giờ)")

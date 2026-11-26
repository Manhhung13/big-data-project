"""Serving sink cục bộ bằng SQLite - thay cho Elasticsearch trong prototype.

Giao diện giữ giống cách dùng ES: ghi idempotent theo khóa (upsert), cập nhật MỘT PHẦN cột
(stock stream và news stream mỗi bên chỉ ghi cột của mình vào cùng một dòng signals).
Khi chuyển sang Elasticsearch chỉ cần thay class này bằng bản dùng bulk update theo _id.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

DDL = """
CREATE TABLE IF NOT EXISTS signals (
    symbol          TEXT NOT NULL,
    window_start    TEXT NOT NULL,      -- slot 5 phút (giờ VN), khóa chung của stock + news
    window_end      TEXT,
    time_bucket     TEXT,
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    n_bars          INTEGER,
    ret             REAL,
    return_z        REAL,
    volume_z        REAL,
    price_anomaly   INTEGER NOT NULL DEFAULT 0,
    volume_anomaly  INTEGER NOT NULL DEFAULT 0,
    last_bar_ts     TEXT,
    news_5m         INTEGER NOT NULL DEFAULT 0,
    news_30m        INTEGER NOT NULL DEFAULT 0,   -- tin trong 30' kết thúc tại window_end
    news_1h         INTEGER NOT NULL DEFAULT 0,
    news_z_30m      REAL,
    news_burst      INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT,
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE IF NOT EXISTS articles (
    article_id   TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    title        TEXT,
    published_at TEXT,
    source       TEXT,
    url          TEXT,
    updated_at   TEXT,
    PRIMARY KEY (article_id, symbol)
);

CREATE TABLE IF NOT EXISTS dlq (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic       TEXT,
    reason      TEXT,
    raw         TEXT,
    received_at TEXT
);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);

-- Market Event = các slot có >= 1 cờ bất thường (mục 8.2). Là VIEW nên luôn phản ánh cờ mới nhất,
-- không bao giờ còn "event ma" khi window được cập nhật lại.
DROP VIEW IF EXISTS market_event;
CREATE VIEW market_event AS
SELECT
    s.symbol || '_' || strftime('%Y%m%d_%H%M', s.window_start) AS event_id,
    s.symbol,
    s.window_start AS event_time,
    s.window_end,
    s.price_anomaly, s.volume_anomaly, s.news_burst,
    CASE s.price_anomaly * 4 + s.volume_anomaly * 2 + s.news_burst
        WHEN 7 THEN 'PRICE+VOLUME+NEWS'
        WHEN 6 THEN 'PRICE+VOLUME'
        WHEN 5 THEN 'PRICE+NEWS'
        WHEN 3 THEN 'NEWS+VOLUME'
        WHEN 4 THEN 'PRICE_ONLY'
        WHEN 2 THEN 'VOLUME_ONLY'
        WHEN 1 THEN 'NEWS_ONLY'
    END AS event_type,
    s.ret, s.return_z, s.volume, s.volume_z, s.news_30m, s.news_z_30m,
    -- window đã khép: event time đã thấy vượt qua cuối window (dữ liệu thật không phải phút nào cũng có nến)
    CASE WHEN s.window_end <= (SELECT value FROM meta WHERE key = 'max_event_time') THEN 1 ELSE 0 END AS is_final,
    -- OPEN -> COMPLETED: đủ 30' dữ liệu sau window sự kiện (theo event-time đã thấy, không theo đồng hồ máy)
    CASE WHEN datetime(s.window_end, '+30 minutes') <= (SELECT value FROM meta WHERE key = 'max_event_time')
         THEN 'COMPLETED' ELSE 'OPEN' END AS status,
    (SELECT c.close FROM signals c
      WHERE c.symbol = s.symbol AND c.window_start = datetime(s.window_start, '+30 minutes'))
        / s.close - 1 AS return_after_30m,
    (SELECT COALESCE(SUM(n.news_5m), 0) FROM signals n
      WHERE n.symbol = s.symbol
        AND n.window_start > s.window_start
        AND n.window_start <= datetime(s.window_start, '+30 minutes')) AS news_after_30m
FROM signals s
WHERE s.price_anomaly = 1 OR s.volume_anomaly = 1 OR s.news_burst = 1;
"""


class SqliteSink:
    def __init__(self, path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def _conn(self):
        # Nhiều query streaming (thread khác nhau) ghi đồng thời -> mỗi lần ghi một connection, WAL + timeout.
        conn = sqlite3.connect(self.path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_schema(self):
        conn = self._conn()
        try:
            conn.executescript(DDL)
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _now():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def upsert(self, table: str, key_cols: list, rows: list):
        """Ghi idempotent theo khóa. Chỉ ghi đè những cột có trong rows (cập nhật một phần)."""
        if not rows:
            return
        rows = [{**r, "updated_at": self._now()} for r in rows]
        cols = list(rows[0].keys())
        updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in key_cols)
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
            f"ON CONFLICT({', '.join(key_cols)}) DO UPDATE SET {updates}"
        )
        conn = self._conn()
        try:
            conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
            conn.commit()
        finally:
            conn.close()

    def insert(self, table: str, rows: list):
        if not rows:
            return
        cols = list(rows[0].keys())
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
        conn = self._conn()
        try:
            conn.executemany(sql, [tuple(r[c] for c in cols) for r in rows])
            conn.commit()
        finally:
            conn.close()

    def set_meta_max(self, key: str, value: str):
        """Lưu giá trị lớn nhất (chuỗi ISO so sánh được theo thứ tự thời gian) - dùng làm 'event time hiện tại'."""
        if value is None:
            return
        conn = self._conn()
        try:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)",
                (key, value),
            )
            conn.commit()
        finally:
            conn.close()

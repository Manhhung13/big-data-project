"""In kết quả từ serving db: danh sách Market Event, dead-letter, số liệu tổng quan.

  python -m streaming.report
"""
import sqlite3

from config import PATHS


def fmt(v, spec="{:.2f}", none="-"):
    return none if v is None else spec.format(v)


def main():
    conn = sqlite3.connect(PATHS["serving_db"])
    conn.row_factory = sqlite3.Row

    n_sig, n_sym = conn.execute("SELECT COUNT(*), COUNT(DISTINCT symbol) FROM signals WHERE n_bars IS NOT NULL").fetchone()
    n_art = conn.execute("SELECT COUNT(DISTINCT article_id) FROM articles").fetchone()[0]
    print(f"stock windows: {n_sig} ({n_sym} mã) | bài đã map vào mã: {n_art}")

    print("\n== MARKET EVENTS ==")
    print(f"{'event_id':<20}{'type':<20}{'status':<10}{'ret%':>7}{'ret_z':>7}{'vol_z':>7}{'news30':>7}{'news_z':>7}{'ret+30m%':>9}{'news+30m':>9}")
    for r in conn.execute("SELECT * FROM market_event ORDER BY event_time, symbol"):
        print(f"{r['event_id']:<20}{r['event_type']:<20}{r['status']:<10}"
              f"{fmt(None if r['ret'] is None else r['ret'] * 100):>7}{fmt(r['return_z']):>7}{fmt(r['volume_z']):>7}"
              f"{r['news_30m']:>7}{fmt(r['news_z_30m']):>7}"
              f"{fmt(None if r['return_after_30m'] is None else r['return_after_30m'] * 100):>9}{r['news_after_30m']:>9}")

    print("\n== SỐ SỰ KIỆN THEO MÃ / LOẠI ==")
    for r in conn.execute("SELECT symbol, event_type, COUNT(*) n FROM market_event GROUP BY 1, 2 ORDER BY 1, 3 DESC"):
        print(f"  {r['symbol']}  {r['event_type']:<20}{r['n']}")

    print("\n== DEAD-LETTER ==")
    for r in conn.execute("SELECT topic, reason, COUNT(*) n FROM dlq GROUP BY 1, 2 ORDER BY 1, 2"):
        print(f"  {r['topic']:<20}{r['reason']:<20}{r['n']}")


if __name__ == "__main__":
    main()

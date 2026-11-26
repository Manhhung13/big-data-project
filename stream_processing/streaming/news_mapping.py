"""Map bài viết -> mã cổ phiếu bằng dictionary/alias (mục 7.2). Dùng chung cho batch và stream."""
import re

from pyspark.sql import Column, functions as F

from config import SYMBOLS


def symbols_in_text(text: Column, symbols: dict = SYMBOLS) -> Column:
    """Trả về array<string> các mã xuất hiện trong text.

    - Ticker: phân biệt hoa/thường, đúng word-boundary ("FPT" khớp, "fpt"/"FPTX" không khớp).
    - Tên công ty/alias: không phân biệt hoa/thường, word-boundary theo Unicode (có dấu tiếng Việt).
    Dựng thành biểu thức tĩnh (không join) nên chạy được trong streaming mà không bị giới hạn join.
    """
    hits = []
    for sym, meta in symbols.items():
        # Mã trùng từ thông dụng (vd "VND" = tiền đồng) chỉ được tính khi có ngữ cảnh "cổ phiếu VND", "(VND)"...
        ticker_pat = meta.get("ticker_context") or rf"\b{re.escape(sym)}\b"
        cond = text.rlike(rf"(?U){ticker_pat}")
        for name in meta.get("names", []):
            cond = cond | text.rlike(rf"(?iU)\b{re.escape(name)}\b")
        hits.append(F.when(cond, F.lit(sym)))
    return F.filter(F.array(*hits), lambda x: x.isNotNull())


def news_text(title: Column, content: Column) -> Column:
    return F.concat_ws(" ", F.coalesce(title, F.lit("")), F.coalesce(content, F.lit("")))


def content_hash(title: Column, content: Column) -> Column:
    """Hash nội dung đã chuẩn hóa (thường hóa + gộp khoảng trắng) để loại bài đăng lại/đăng trùng nguồn."""
    norm = F.regexp_replace(F.lower(news_text(title, content)), r"\s+", " ")
    return F.sha2(F.trim(norm), 256)

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# Unit test chạy trên cấu hình giả lập (ngưỡng và danh mục mã cố định, không phụ thuộc repo nhóm).
os.environ.setdefault("BIGDATA_SOURCE", "sim")


@pytest.fixture(scope="session")
def spark():
    from spark_env import get_spark
    s = get_spark("tests", cores=2, shuffle_partitions=2)
    yield s
    s.stop()


@pytest.fixture
def make_df(spark, tmp_path):
    """Tạo DataFrame từ list dict qua file JSON tạm.

    Không dùng spark.createDataFrame(list) vì Python worker của PySpark 4.0 crash trên Windows + Python 3.13.
    """
    counter = [0]

    def _make(rows, ddl):
        counter[0] += 1
        p = tmp_path / f"df{counter[0]}.json"
        p.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
        return spark.read.schema(ddl).json(str(p))

    return _make

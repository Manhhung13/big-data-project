"""Nạp baseline do Batch Layer ghi, có làm mới định kỳ.

Static DataFrame đọc từ file KHÔNG tự thấy file mới khi query đang chạy; vì vậy baseline được nạp
trong foreachBatch qua provider này và tải lại sau mỗi TTL (mục 7: "baseline do Batch Layer cập nhật định kỳ").
"""
import threading
import time

from pyspark.sql import DataFrame, SparkSession


class BaselineProvider:
    def __init__(self, spark: SparkSession, path, ttl_seconds: int):
        self.spark = spark
        self.path = str(path)
        self.ttl = ttl_seconds
        self._df = None
        self._loaded_at = 0.0
        self._lock = threading.Lock()

    def _load(self):
        df = self.spark.read.parquet(self.path).cache()
        df.count()  # materialize trước khi bỏ bản cũ
        if self._df is not None:
            self._df.unpersist()
        self._df, self._loaded_at = df, time.monotonic()

    def get(self) -> DataFrame:
        with self._lock:
            if self._df is None or time.monotonic() - self._loaded_at > self.ttl:
                self._load()
            return self._df

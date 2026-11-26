"""Tạo SparkSession dùng chung cho batch / streaming / test.

Trên Windows Spark cần winutils.exe + hadoop.dll. Project dùng bản đặt trong ./hadoop/bin
và trỏ HADOOP_HOME tới đó (chỉ trong process này, không đụng cấu hình máy).
"""
import os
import sys

from config import ROOT, TZ


def _setup_windows_env():
    if os.name != "nt":
        return
    hadoop_home = ROOT / "hadoop"
    if (hadoop_home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ["PATH"]


def get_spark(app_name: str, cores: int = 6, shuffle_partitions: int = 2):
    _setup_windows_env()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", TZ)        # window / time_bucket phải theo giờ VN
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        # Không tạo file .crc cạnh mỗi file checkpoint: trên Windows mỗi thao tác file đều gọi winutils nên rất tốn.
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.python.worker.faulthandler.enabled", "true")        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

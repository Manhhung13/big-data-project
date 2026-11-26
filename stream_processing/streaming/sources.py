"""Nguồn streaming. Prototype đọc file JSON-lines từ thư mục (mỗi file = một lô message)
thay cho Kafka. Phần xử lý phía sau chỉ nhận DataFrame có đúng schema nên khi chuyển sang
Kafka chỉ cần thay hàm này bằng readStream("kafka") + from_json(value, schema)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from config import MAX_FILES_PER_TRIGGER


def read_json_stream(spark: SparkSession, path, schema: StructType) -> DataFrame:
    return (
        spark.readStream.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .json(str(path))
    )

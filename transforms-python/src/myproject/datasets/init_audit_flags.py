from transforms.api import transform, Output
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession


@transform(
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/audit_flags"),
)
def compute(out):
    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("flag_id", StringType(), False),
        StructField("filing_id", StringType(), False),
        StructField("reason", StringType(), True),
        StructField("flagged_by", StringType(), True),
        StructField("flagged_at", TimestampType(), True),
        StructField("status", StringType(), True),
    ])

    empty_df = spark.createDataFrame([], schema=schema)
    out.write_dataframe(empty_df)
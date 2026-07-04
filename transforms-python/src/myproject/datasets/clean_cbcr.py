from transforms.api import transform, Input, Output
from pyspark.sql import Window
from pyspark.sql import functions as F

RAW_COLUMNS = {
    "Reference area": "jurisdiction",
    "Counterpart area": "counterpart_jurisdiction",
    "Measure": "measure",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "obs_value",
}
DEDUP_KEYS = ["jurisdiction", "counterpart_jurisdiction", "measure", "year"]


@transform(
    raw=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_raw"),
    output=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_clean"),
)
def compute(raw, output):
    df = raw.dataframe()

    df = df.select(*RAW_COLUMNS.keys())
    for old_name, new_name in RAW_COLUMNS.items():
        df = df.withColumnRenamed(old_name, new_name)

    df = df.withColumn("year", F.col("year").cast("int")) \
           .withColumn("obs_value", F.col("obs_value").cast("double"))

    df = df.dropna(subset=["jurisdiction", "year", "obs_value"])

    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.lit(1))
    df = df.withColumn("row_num", F.row_number().over(window))
    df = df.filter(F.col("row_num") == 1).drop("row_num")

    output.write_dataframe(df)
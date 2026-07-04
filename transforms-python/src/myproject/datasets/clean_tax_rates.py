from transforms.api import transform, Input, Output
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW_COLUMNS = {
    "Reference area": "jurisdiction",
    "Measure": "measure",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "tax_rate",
}

DEDUP_KEYS = ["jurisdiction", "measure", "year"]


def select_and_rename(df):
    missing = [c for c in RAW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Expected columns not found: {missing}. "
            f"Available columns: {df.columns}"
        )
    df = df.select(*RAW_COLUMNS.keys())
    for old_name, new_name in RAW_COLUMNS.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def cast_types(df):
    return (
        df.withColumn("year", F.col("year").cast("int"))
          .withColumn("tax_rate", F.col("tax_rate").cast("double"))
    )


def deduplicate(df):
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.lit(1))
    df = df.withColumn("row_num", F.row_number().over(window))
    return df.filter(F.col("row_num") == 1).drop("row_num")


@transform(
    raw=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/tax_rates_raw"),
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/tax_rates_clean"),
)
def compute(raw, out):
    spark = SparkSession.builder.getOrCreate()
    path = raw.filesystem().hadoop_path

    raw_df = spark.read.csv(path, header=True, inferSchema=False)

    clean_df = select_and_rename(raw_df)
    clean_df = cast_types(clean_df)
    clean_df = clean_df.dropna(subset=["jurisdiction", "year", "tax_rate"])
    clean_df = deduplicate(clean_df)

    out.write_dataframe(clean_df)
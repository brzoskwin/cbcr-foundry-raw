from transforms.api import transform, Input, Output
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW_COLUMNS = {
    "Reference area": "jurisdiction",
    "Counterpart area": "counterpart_jurisdiction",
    "Measure": "measure",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "obs_value",
}

DEDUP_KEYS = ["jurisdiction", "counterpart_jurisdiction", "measure", "year"]


def select_and_rename(df):
    missing = [c for c in RAW_COLUMNS if c not in df.columns]
    if missing:
<<<<<<< HEAD
        raise ValueError(f"Expected columns not found: {missing}. Available: {df.columns}")
=======
        raise ValueError(
            f"Expected columns not found: {missing}. "
            f"Available columns: {df.columns}"
        )
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
    df = df.select(*RAW_COLUMNS.keys())
    for old_name, new_name in RAW_COLUMNS.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def cast_types(df):
    return (
        df.withColumn("year", F.col("year").cast("int"))
<<<<<<< HEAD
        .withColumn("obs_value", F.col("obs_value").cast("double"))
    )


def add_ingestion_metadata(df):
    return df.withColumn("_ingested_at", F.current_timestamp())


def deduplicate(df):
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("_ingested_at").desc())
=======
          .withColumn("obs_value", F.col("obs_value").cast("double"))
    )


def deduplicate(df):
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.lit(1))
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
    df = df.withColumn("row_num", F.row_number().over(window))
    return df.filter(F.col("row_num") == 1).drop("row_num")


@transform(
    raw=Input("ri.foundry.main.dataset.19cf92fd-547c-4ded-8444-c7baf6666305"),
    out=Output("ri.foundry.main.dataset.e3d5b917-7337-44d7-a66e-b6afa00fdb5b"),
)
def compute(raw, out):
    spark = SparkSession.builder.getOrCreate()
    path = raw.filesystem().hadoop_path
<<<<<<< HEAD
=======

>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
    raw_df = spark.read.csv(path, header=True, inferSchema=False)

    clean_df = select_and_rename(raw_df)
    clean_df = cast_types(clean_df)
<<<<<<< HEAD
    clean_df = add_ingestion_metadata(clean_df)
=======
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
    clean_df = clean_df.dropna(subset=["jurisdiction", "year", "obs_value"])
    clean_df = deduplicate(clean_df)

    out.write_dataframe(clean_df)
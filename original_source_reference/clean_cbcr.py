"""
clean_cbcr.py
Cleans the raw CbCR CSV (data/raw/cbcr_raw.csv) using PySpark:
- keeps only the columns which are actually need
- renames columns to simple snake_case names
- casts types (year as int, value as double)
- removes duplicate rows using a window function (ROW_NUMBER pattern),
  based on FULL bilateral combination (jurisdiction + counterpart +
  measure + year)
  """


from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "cbcr_raw.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "cbcr_clean.csv"

RAW_COLUMNS = {
    "Reference area": "jurisdiction",
    "Counterpart area": "counterpart_jurisdiction",
    "Measure": "measure",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "obs_value",
}

# Full bilateral key: dedup keeps one row per unique combination of these
# columns, instead of collapsing all jurisdictions together.
DEDUP_KEYS = ["jurisdiction", "counterpart_jurisdiction", "measure", "year"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("clean_cbcr")
        .master("local[*]")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )


def load_raw(spark: SparkSession, path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}. Run ingest_cbcr.py first."
        )
    return spark.read.csv(str(path), header=True, inferSchema=False)


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
        .withColumn("obs_value", F.col("obs_value").cast("double"))
    )


def deduplicate(df):
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.lit(1))
    df = df.withColumn("row_num", F.row_number().over(window))
    return df.filter(F.col("row_num") == 1).drop("row_num")


def main():
    spark = get_spark()
    raw_df = load_raw(spark, INPUT_FILE)
    print(f"Raw rows loaded: {raw_df.count()}")

    clean_df = select_and_rename(raw_df)
    clean_df = cast_types(clean_df)

    nulls_year = clean_df.filter(F.col("year").isNull()).count()
    nulls_value = clean_df.filter(F.col("obs_value").isNull()).count()
    print(f"Rows with null year: {nulls_year} | null obs_value: {nulls_value}")

    clean_df = clean_df.dropna(subset=["jurisdiction", "year", "obs_value"])
    print(f"Rows after dropna: {clean_df.count()}")

    clean_df = deduplicate(clean_df)
    row_count = clean_df.count()
    print(f"Clean CbCR rows (final, bilateral pairs preserved): {row_count}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    clean_df.toPandas().to_csv(OUTPUT_FILE, index=False)
    print(f"Saved cleaned data to {OUTPUT_FILE}")

    spark.stop()


if __name__ == "__main__":
    main()
"""
clean_tax_rates.py
Cleans the raw CIT tax rates CSV (data/raw/tax_rates_raw.csv) using PySpark:
- keeps only the columns which are actually need
- renames columns to simple snake_case names
- casts types (year as int, rate as double)
- removes duplicate rows using a window function (ROW_NUMBER pattern)
- writes the result to data/processed/tax_rates_clean.csv
"""

from pathlib import Path
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "tax_rates_raw.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_FILE = OUTPUT_DIR / "tax_rates_clean.csv"

RAW_COLUMNS = {
    "Reference area": "jurisdiction",
    "Measure": "measure",
    "TIME_PERIOD": "year",
    "OBS_VALUE": "tax_rate",
}

DEDUP_KEYS = ["jurisdiction", "measure", "year"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("clean_tax_rates")
        .master("local[*]")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )


def load_raw(spark: SparkSession, path: Path):
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}. Run ingest_tax_rates.py first."
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
        .withColumn("tax_rate", F.col("tax_rate").cast("double"))
    )


def deduplicate(df):
    window = Window.partitionBy(*DEDUP_KEYS).orderBy(F.lit(1))
    df = df.withColumn("row_num", F.row_number().over(window))
    return df.filter(F.col("row_num") == 1).drop("row_num")


def main():
    spark = get_spark()
    raw_df = load_raw(spark, INPUT_FILE)
    print(f"Raw rows loaded: {raw_df.count()}")
    print(f"Available columns: {raw_df.columns}")

    clean_df = select_and_rename(raw_df)
    clean_df = cast_types(clean_df)

    nulls_year = clean_df.filter(F.col("year").isNull()).count()
    nulls_rate = clean_df.filter(F.col("tax_rate").isNull()).count()
    print(f"Rows with null year: {nulls_year} | null tax_rate: {nulls_rate}")

    clean_df = clean_df.dropna(subset=["jurisdiction", "year", "tax_rate"])
    print(f"Rows after dropna: {clean_df.count()}")

    clean_df = deduplicate(clean_df)
    row_count = clean_df.count()
    print(f"Clean tax rate rows (final): {row_count}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    clean_df.toPandas().to_csv(OUTPUT_FILE, index=False)
    print(f"Saved cleaned data to {OUTPUT_FILE}")

    spark.stop()


if __name__ == "__main__":
    main()
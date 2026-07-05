from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LOOKBACK_DAYS = 7


@transform(
    cbcr_clean=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_clean"),
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_incremental_snapshot"),
)
def compute(cbcr_clean, out):
    df = cbcr_clean.dataframe()

    lookback_cutoff = F.date_sub(F.current_date(), LOOKBACK_DAYS)

    on_time = df.filter(F.to_date(F.col("_ingested_at")) < lookback_cutoff)
    late_arriving = df.filter(F.to_date(F.col("_ingested_at")) >= lookback_cutoff)

    merged = on_time.unionByName(late_arriving)

    dedup_window = Window.partitionBy(
        "jurisdiction", "counterpart_jurisdiction", "measure", "year"
    ).orderBy(F.col("_ingested_at").desc())

    snapshot = (
        merged.withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    out.write_dataframe(snapshot)
from transforms.api import transform, Input, Output
from pyspark.sql import functions as F

PROFIT_MEASURE = "Profit (loss) before income tax"
TAX_PAID_MEASURE = "Income tax paid (on cash basis)"
STATUTORY_MEASURE = "Corporate income tax rate"


def aggregate_cbcr_totals(cbcr_df):
    return cbcr_df.groupBy("jurisdiction", "year", "measure").agg(
        F.sum("obs_value").alias("obs_value")
    )


def pivot_measures(totals_df):
    wanted = totals_df.filter(
        F.col("measure").isin([PROFIT_MEASURE, TAX_PAID_MEASURE])
    )
    pivoted = (
        wanted.groupBy("jurisdiction", "year")
        .pivot("measure", [PROFIT_MEASURE, TAX_PAID_MEASURE])
        .agg(F.sum("obs_value"))
    )
    pivoted = pivoted.withColumnRenamed(PROFIT_MEASURE, "profit_before_tax")
    pivoted = pivoted.withColumnRenamed(TAX_PAID_MEASURE, "tax_paid")
    return pivoted


def compute_effective_rate(df):
    return df.withColumn(
        "effective_tax_rate",
        F.when(
            (F.col("profit_before_tax").isNotNull()) & (F.col("profit_before_tax") > 0),
            F.col("tax_paid") / F.col("profit_before_tax"),
        ).otherwise(F.lit(None)),
    )


def filter_statutory_rates(tax_df):
    statutory = tax_df.filter(F.col("measure") == STATUTORY_MEASURE)
    statutory = statutory.withColumnRenamed("tax_rate", "statutory_rate")
    return statutory.select("jurisdiction", "year", "statutory_rate")


@transform(
    cbcr=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_clean"),
    tax=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/tax_rates_clean"),
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/effective_vs_statutory"),
)
def compute(cbcr, tax, out):
    cbcr_df = cbcr.dataframe()
    tax_df = tax.dataframe()

    totals = aggregate_cbcr_totals(cbcr_df)
    pivoted = pivot_measures(totals)
    pivoted = compute_effective_rate(pivoted)

    statutory = filter_statutory_rates(tax_df)

    merged = pivoted.join(statutory, on=["jurisdiction", "year"], how="inner")

    merged = merged.withColumn(
        "effective_tax_rate_pct", F.col("effective_tax_rate") * 100
    )
    merged = merged.withColumn(
        "gap_statutory_minus_effective",
        F.col("statutory_rate") - F.col("effective_tax_rate_pct"),
    )

    merged = merged.withColumn(
        "id", F.concat_ws("_", F.col("jurisdiction"), F.col("year").cast("string"))
    )

    merged = merged.orderBy(F.col("gap_statutory_minus_effective").desc())

    out.write_dataframe(merged)
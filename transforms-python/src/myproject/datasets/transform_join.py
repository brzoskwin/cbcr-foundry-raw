from transforms.api import transform, Input, Output
from pyspark.sql import functions as F
<<<<<<< HEAD
from pyspark.sql.window import Window


PROFIT_MEASURE = "Profit (loss) before income tax"
TAX_PAID_MEASURE = "Income tax paid (on cash basis)"
EMPLOYEES_MEASURE = "Employees"
MNE_GROUPS_MEASURE = "Multinational enterprise groups"
=======

PROFIT_MEASURE = "Profit (loss) before income tax"
TAX_PAID_MEASURE = "Income tax paid (on cash basis)"
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
STATUTORY_MEASURE = "Corporate income tax rate"


def aggregate_cbcr_totals(cbcr_df):
    return cbcr_df.groupBy("jurisdiction", "year", "measure").agg(
        F.sum("obs_value").alias("obs_value")
    )


def pivot_measures(totals_df):
    wanted = totals_df.filter(
<<<<<<< HEAD
        F.col("measure").isin(
            [PROFIT_MEASURE, TAX_PAID_MEASURE, EMPLOYEES_MEASURE, MNE_GROUPS_MEASURE]
        )
    )
    pivoted = (
        wanted.groupBy("jurisdiction", "year")
        .pivot(
            "measure",
            [PROFIT_MEASURE, TAX_PAID_MEASURE, EMPLOYEES_MEASURE, MNE_GROUPS_MEASURE],
        )
        .agg(F.sum("obs_value"))
    )
    pivoted = (
        pivoted
        .withColumnRenamed(PROFIT_MEASURE, "profit_before_tax")
        .withColumnRenamed(TAX_PAID_MEASURE, "tax_paid")
        .withColumnRenamed(EMPLOYEES_MEASURE, "employees")
        .withColumnRenamed(MNE_GROUPS_MEASURE, "mne_group_count")
    )
=======
        F.col("measure").isin([PROFIT_MEASURE, TAX_PAID_MEASURE])
    )
    pivoted = (
        wanted.groupBy("jurisdiction", "year")
        .pivot("measure", [PROFIT_MEASURE, TAX_PAID_MEASURE])
        .agg(F.sum("obs_value"))
    )
    pivoted = pivoted.withColumnRenamed(PROFIT_MEASURE, "profit_before_tax")
    pivoted = pivoted.withColumnRenamed(TAX_PAID_MEASURE, "tax_paid")
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
    return pivoted


def compute_effective_rate(df):
<<<<<<< HEAD
    df = df.withColumn(
=======
    return df.withColumn(
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
        "effective_tax_rate",
        F.when(
            (F.col("profit_before_tax").isNotNull()) & (F.col("profit_before_tax") > 0),
            F.col("tax_paid") / F.col("profit_before_tax"),
        ).otherwise(F.lit(None)),
    )
<<<<<<< HEAD
    df = df.withColumn(
        "profit_per_employee",
        F.when(
            (F.col("employees").isNotNull()) & (F.col("employees") > 0),
            F.col("profit_before_tax") / F.col("employees"),
        ).otherwise(F.lit(None)),
    )
    return df
=======
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d


def filter_statutory_rates(tax_df):
    statutory = tax_df.filter(F.col("measure") == STATUTORY_MEASURE)
    statutory = statutory.withColumnRenamed("tax_rate", "statutory_rate")
<<<<<<< HEAD
    statutory = statutory.select("jurisdiction", "year", "statutory_rate")
    statutory = statutory.withColumn(
        "id", F.concat_ws("_", F.col("jurisdiction"), F.col("year").cast("string"))
    )
    return statutory


def apply_qualify_pattern(df):
    """
    Equivalent of SQL QUALIFY: keep only jurisdictions where
    employees > median AND profit_per_employee is in the top quartile (per year).
    """
    window_year = Window.partitionBy("year")

    median_employees = F.expr(
        "percentile_approx(employees, 0.5)"
    ).over(window_year)

    q3_profit_per_employee = F.expr(
        "percentile_approx(profit_per_employee, 0.75)"
    ).over(window_year)

    flagged = df.withColumn("median_employees_year", median_employees)
    flagged = flagged.withColumn("q3_profit_per_employee_year", q3_profit_per_employee)

    flagged = flagged.withColumn(
        "audit_signal",
        (F.col("employees") > F.col("median_employees_year"))
        & (F.col("profit_per_employee") >= F.col("q3_profit_per_employee_year")),
    )
    return flagged
=======
    return statutory.select("jurisdiction", "year", "statutory_rate")
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d


@transform(
    cbcr=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_clean"),
    tax=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/tax_rates_clean"),
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/effective_vs_statutory"),
<<<<<<< HEAD
    out_top10=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/top10_beps_gap"),
    out_audit=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/audit_candidates"),
    out_jurisdictions=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/jurisdictions_dim"),
    out_tax_rates=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/tax_rates_with_id"),
)
def compute(cbcr, tax, out, out_top10, out_audit, out_jurisdictions, out_tax_rates):
=======
)
def compute(cbcr, tax, out):
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
    cbcr_df = cbcr.dataframe()
    tax_df = tax.dataframe()

    totals = aggregate_cbcr_totals(cbcr_df)
    pivoted = pivot_measures(totals)
    pivoted = compute_effective_rate(pivoted)

    statutory = filter_statutory_rates(tax_df)

<<<<<<< HEAD
    merged = pivoted.join(
        statutory.drop("id"), on=["jurisdiction", "year"], how="inner"
    )
=======
    merged = pivoted.join(statutory, on=["jurisdiction", "year"], how="inner")
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d

    merged = merged.withColumn(
        "effective_tax_rate_pct", F.col("effective_tax_rate") * 100
    )
    merged = merged.withColumn(
        "gap_statutory_minus_effective",
        F.col("statutory_rate") - F.col("effective_tax_rate_pct"),
    )
<<<<<<< HEAD
    merged = merged.withColumn(
        "id", F.concat_ws("_", F.col("jurisdiction"), F.col("year").cast("string"))
    )
    merged = merged.orderBy(F.col("gap_statutory_minus_effective").desc())

    out.write_dataframe(merged)

    top10 = merged.orderBy(F.col("gap_statutory_minus_effective").desc()).limit(10)
    out_top10.write_dataframe(top10)

    qualified = apply_qualify_pattern(merged)
    audit_candidates = qualified.filter(F.col("audit_signal") == True)
    out_audit.write_dataframe(audit_candidates)

    jurisdictions = merged.select("jurisdiction").distinct()
    out_jurisdictions.write_dataframe(jurisdictions)

    out_tax_rates.write_dataframe(statutory)
=======

    merged = merged.withColumn(
        "id", F.concat_ws("_", F.col("jurisdiction"), F.col("year").cast("string"))
    )

    merged = merged.orderBy(F.col("gap_statutory_minus_effective").desc())

    out.write_dataframe(merged)
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d

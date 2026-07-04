"""
transform_join.py
Joins cleaned CbCR data with CIT tax rates for calculation of the effective vs
statutory tax rate for each (jurisdiction, year) pair.

Logic:
1. Pivot CbCR so each jurisdiction+year has "profit_before_tax" and
   "tax_paid" as separate columns (currently they're rows tagged by
   'measure').
2. Execute effective_tax_rate = tax_paid / profit_before_tax.
3. Join with tax_rates_clean.csv on (jurisdiction, year) to attach the
   statutory CIT rate.
4. Calculate the gap = statutory_rate - effective_tax_rate. A large
   positive gap is a signal of profit shifting / aggressive tax planning.
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CBCR_FILE = PROJECT_ROOT / "data" / "processed" / "cbcr_clean.csv"
TAX_FILE = PROJECT_ROOT / "data" / "processed" / "tax_rates_clean.csv"
OUTPUT_FILE = PROJECT_ROOT / "data" / "processed" / "effective_vs_statutory.csv"

PROFIT_MEASURE = "Profit (loss) before income tax"
TAX_PAID_MEASURE = "Income tax paid (on cash basis)"
STATUTORY_MEASURE = "Corporate income tax rate"


def load_cbcr() -> pd.DataFrame:
    if not CBCR_FILE.exists():
        raise FileNotFoundError(f"{CBCR_FILE} not found. Run clean_cbcr.py first.")
    return pd.read_csv(CBCR_FILE)


def load_tax_rates() -> pd.DataFrame:
    if not TAX_FILE.exists():
        raise FileNotFoundError(f"{TAX_FILE} not found. Run clean_tax_rates.py first.")
    return pd.read_csv(TAX_FILE)


def aggregate_cbcr_totals(cbcr: pd.DataFrame) -> pd.DataFrame:
    totals = (
        cbcr.groupby(["jurisdiction", "year", "measure"], as_index=False)
        ["obs_value"].sum()
    )
    return totals


def pivot_measures(totals: pd.DataFrame) -> pd.DataFrame:
    wanted = totals[totals["measure"].isin([PROFIT_MEASURE, TAX_PAID_MEASURE])]
    pivoted = wanted.pivot_table(
        index=["jurisdiction", "year"],
        columns="measure",
        values="obs_value",
        aggfunc="sum",
    ).reset_index()
    pivoted = pivoted.rename(columns={
        PROFIT_MEASURE: "profit_before_tax",
        TAX_PAID_MEASURE: "tax_paid",
    })
    for col in ["profit_before_tax", "tax_paid"]:
        if col not in pivoted.columns:
            pivoted[col] = pd.NA
    return pivoted


def compute_effective_rate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["effective_tax_rate"] = df.apply(
        lambda r: (r["tax_paid"] / r["profit_before_tax"])
        if pd.notna(r["profit_before_tax"]) and r["profit_before_tax"] > 0
        else pd.NA,
        axis=1,
    )
    return df


def filter_statutory_rates(tax: pd.DataFrame) -> pd.DataFrame:
    statutory = tax[tax["measure"] == STATUTORY_MEASURE].copy()
    statutory = statutory.rename(columns={"tax_rate": "statutory_rate"})
    return statutory[["jurisdiction", "year", "statutory_rate"]]


def main():
    cbcr = load_cbcr()
    tax = load_tax_rates()

    totals = aggregate_cbcr_totals(cbcr)
    pivoted = pivot_measures(totals)
    pivoted = compute_effective_rate(pivoted)

    statutory = filter_statutory_rates(tax)

    merged = pivoted.merge(statutory, on=["jurisdiction", "year"], how="inner")

    merged["effective_tax_rate_pct"] = merged["effective_tax_rate"] * 100
    merged["gap_statutory_minus_effective"] = (
        merged["statutory_rate"] - merged["effective_tax_rate_pct"]
    )

    merged = merged.sort_values(
        "gap_statutory_minus_effective", ascending=False
    )

    print(f"CbCR jurisdiction-years with profit+tax data: {len(pivoted)}")
    print(f"Matched with statutory rates: {len(merged)}")
    print()
    print("Top 10 largest gaps (statutory - effective), possible profit shifting signals:")
    print(merged[[
        "jurisdiction", "year", "statutory_rate",
        "effective_tax_rate_pct", "gap_statutory_minus_effective"
    ]].head(10).to_string(index=False))

    merged.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
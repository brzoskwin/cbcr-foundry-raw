"""
inspect_measures.py
Quick diagnostic: list distinct values in the 'measure' column of both
cleaned datasets.
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CBCR_FILE = PROJECT_ROOT / "data" / "processed" / "cbcr_clean.csv"
TAX_FILE = PROJECT_ROOT / "data" / "processed" / "tax_rates_clean.csv"

cbcr = pd.read_csv(CBCR_FILE)
tax = pd.read_csv(TAX_FILE)

print("=== CbCR distinct measures ===")
for m in sorted(cbcr["measure"].dropna().unique()):
    print(f"- {m}")

print()
print("=== Tax rates distinct measures ===")
for m in sorted(tax["measure"].dropna().unique()):
    print(f"- {m}")

print()
print("=== CbCR years range ===", cbcr["year"].min(), "-", cbcr["year"].max())
print("=== Tax rates years range ===", tax["year"].min(), "-", tax["year"].max())
print()
print("=== CbCR jurisdiction sample ===", sorted(cbcr["jurisdiction"].unique())[:10])
print("=== Tax rates jurisdiction sample ===", sorted(tax["jurisdiction"].unique())[:10])
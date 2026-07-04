"""
debug_cbcr_columns.py
Quick diagnostic: inspect raw column names and sample values from
data/raw/cbcr_raw.csv using pandas (fast, no Spark/Java needed).
Run this first to see exactly why 'year'/'obs_value' end up null.
"""

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "cbcr_raw.csv"

df = pd.read_csv(INPUT_FILE, nrows=20)

print("=== COLUMN NAMES ===")
for i, c in enumerate(df.columns):
    print(f"{i}: {repr(c)}")

print()
print("=== FIRST 5 ROWS (Time period / Observation value) ===")
candidates_year = [c for c in df.columns if "period" in c.lower() or "time" in c.lower()]
candidates_val = [c for c in df.columns if "obs" in c.lower() or "value" in c.lower()]
print("year-like columns found:", candidates_year)
print("value-like columns found:", candidates_val)

for c in candidates_year + candidates_val:
    print(f"\n--- {repr(c)} sample values ---")
    print(df[c].head(10).tolist())

# CbCR Tax Rate Analysis — Foundry Pipeline

> A Palantir Foundry data pipeline detecting Base Erosion and Profit Shifting (BEPS) signals by reconciling OECD Country-by-Country Reporting data with statutory corporate tax rates and Eurostat macro indicators.

---

## Overview

This project ingests, cleans, and joins three public datasets — **OECD CbCR**, **OECD Corporate Income Tax Rates**, and **Eurostat tax-to-GDP ratio** — inside a Foundry Code Repository. The pipeline computes each jurisdiction's *effective* tax rate and compares it against the *statutory* rate to surface the largest gaps, a classic signal of profit shifting. Flagged jurisdictions feed into an Ontology-backed governance workflow with approval and audit trail.

## Pipeline Architecture

```
ingest_cbcr.py            → cbcr_raw              → clean_cbcr.py        → cbcr_clean
ingest_tax_rate.py        → tax_rates_raw         → clean_tax_rates.py   → tax_rates_clean
ingest_eurostat.py        → eurostat_tax_gdp_raw  → clean_eurostat.py    → eurostat_tax_gdp_clean

cbcr_clean                → incremental_batches.py → cbcr_incremental_snapshot

cbcr_clean + tax_rates_clean → transform_join.py →  effective_vs_statutory
                                                  →  top10_beps_gap
                                                  →  audit_candidates
                                                  →  jurisdictions_dim
                                                  →  tax_rates_with_id

init_audit_flags.py       → audit_flags  (seed dataset for the FlagForAudit action)
```

## Datasets

| File | Input | Output | Purpose |
|---|---|---|---|
| `ingest_cbcr.py` | OECD SDMX API (`DSD_CBCR@DF_CBCRI`) | `cbcr_raw.csv` | Pulls raw CbCR data from OECD, with retry (3x) and 60s timeout |
| `ingest_tax_rate.py` | OECD SDMX API (`DSD_TAX_CIT@DF_CIT`) | `tax_rates_raw.csv` | Pulls statutory CIT rates per jurisdiction and year |
| `ingest_eurostat.py` | Eurostat API (`gov_10a_taxag`) | `eurostat_tax_gdp_raw.json` | Pulls tax-to-GDP ratio in JSON-stat format |
| `clean_cbcr.py` | `cbcr_raw` | `cbcr_clean` | Renames columns, casts types, drops nulls, deduplicates on `(jurisdiction, counterpart_jurisdiction, measure, year)` |
| `clean_tax_rates.py` | `tax_rates_raw` | `tax_rates_clean` | Renames columns, casts `tax_rate` to double, deduplicates on `(jurisdiction, measure, year)` |
| `clean_eurostat.py` | `eurostat_tax_gdp_raw` | `eurostat_tax_gdp_clean` | Custom JSON-stat parser, filters by tax category and unit of measure (% of GDP) |
| `incremental_batches.py` | `cbcr_clean` | `cbcr_incremental_snapshot` | Splits on-time vs. late-arriving records (7-day lookback), merges and deduplicates |
| `transform_join.py` | `cbcr_clean`, `tax_rates_clean` | `effective_vs_statutory`, `top10_beps_gap`, `audit_candidates`, `jurisdictions_dim`, `tax_rates_with_id` | Aggregates CbCR, pivots measures, joins with statutory rates, computes `effective_tax_rate` and the BEPS gap |
| `init_audit_flags.py` | — | `audit_flags` | Seeds an empty dataset with the audit-trail schema used by the `FlagForAudit` action |
| `examples.py` | — | — | Boilerplate template using `LightweightInput` / `LightweightOutput` (Polars, lazy) |

## Core Logic

**Ingestion** — every `ingest_*.py` module uses `external_systems.Source` with three retry attempts and a 60-second timeout per request.

**Normalization** — raw OECD column names (`Reference area`, `Measure`, `TIME_PERIOD`, `OBS_VALUE`) are mapped to standardized names, with validation against missing columns.

**Deduplication** — implemented via:

```python
Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("_ingested_at").desc())
```

removing duplicate resubmissions while keeping the latest record.

**Incremental snapshot** — `incremental_batches.py` separates records into `on_time` and `late_arriving` buckets based on a 7-day lookback window, then merges and deduplicates the result into a single snapshot.

**Pivot and join** — `transform_join.py` aggregates CbCR by `(jurisdiction, year, measure)`, pivots into columns (`profit_before_tax`, `tax_paid`, `employees`, `mne_group_count`), computes:

- `effective_tax_rate = tax_paid / profit_before_tax`
- `profit_per_employee = profit_before_tax / employees`

then joins with statutory rates on `(jurisdiction, year)`.

**BEPS signal** — the gap between nominal and effective rates:

```python
gap_statutory_minus_effective = statutory_rate - effective_tax_rate_pct
```

sorted descending; the top 10 jurisdictions are written to `top10_beps_gap`.

**QUALIFY pattern** — `apply_qualify_pattern` flags jurisdictions where `employees` exceeds the yearly median **and** `profit_per_employee` sits in the top quartile (`percentile_approx(0.75)`) — the SQL `QUALIFY` equivalent, used as an audit trigger.

**Audit trail seed** — `init_audit_flags.py` initializes an empty dataset with the schema (`flag_id`, `filing_id`, `reason`, `flagged_by`, `flagged_at`, `status`) required by the `FlagForAudit` Ontology action.

## Ontology Model

| Component | Details |
|---|---|
| Object Types | `MNEGroup`, `Jurisdiction`, `CbCRFiling`, `TaxRate` |
| Link Types | `MNEGroup → CbCRFiling` (1:N), `CbCRFiling → Jurisdiction` (N:1), `Jurisdiction → TaxRate` (1:N by year) |
| Derived Property | `effective_tax_rate` on `CbCRFiling`, computed from linked data |
| Action Type | `FlagForAudit` — requires approval, writes an audit trail (who, when, reason — e.g. "effective rate < 5pp below nominal rate") |

## Known Issues

- `ingest_cbcr.py`, `clean_cbcr.py`, and `transform_join.py` contain unresolved Git merge conflicts (`<<<<<<< HEAD` / `=======`). The `HEAD` branch is the more complete version, adding `employees`, `mne_group_count`, `profit_per_employee`, and the extra outputs (`top10_beps_gap`, `audit_candidates`, `jurisdictions_dim`, `tax_rates_with_id`). These need to be merged into `main` before further development.
- `filter_statutory_rates` (HEAD version) adds an `id` column that must be dropped before the join (`statutory.drop("id")`) — consider generating `id` once, at the end of the pipeline, instead.

## Requirements

- Python 3.9+
- PySpark (Foundry runtime)
- `transforms-expectations` library for data quality checks:
  - `profit_before_tax >= 0`
  - `tax_paid <= profit_before_tax`
  - `revenue >= profit_before_tax`
- Configured `external_systems.Source` connections for the OECD SDMX and Eurostat APIs

## Roadmap

- [ ] Resolve remaining Git merge conflicts across `ingest_cbcr.py`, `clean_cbcr.py`, and `transform_join.py`
- [ ] Build Workshop dashboard with MNEGroup table sorted by BEPS gap, wired to `FlagForAudit`
- [ ] Build Contour heatmap of effective tax rate by jurisdiction
- [ ] Configure RBAC: `TAX_ANALYST` (view) vs. `TAX_MANAGER` (approve)

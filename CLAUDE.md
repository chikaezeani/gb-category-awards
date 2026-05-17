# GB 2026 Category Awards — CLAUDE.md

## Agent Instructions

**At the start of every session:**
1. Read this file (`CLAUDE.md`) in full
2. Read `working_memory.md` in full
These two files together are the complete context for this project. Do not proceed with any task without reading both.

**At the completion of every task:**
- Update `working_memory.md` to reflect what was done, any key decisions made, files changed, and updated next steps

---

## What This Project Does

This is an ETL + ranking pipeline that computes **GB Foods 2026 Category Awards** across Nigeria. The awards are **area-based** — every metric, ranking, and output is aggregated and ranked at the **Area** level (and rolled up to Region). KAM and ASM are reference attributes that describe who manages each area; they are not the subject of the awards and do not drive any joins or scoring.

The pipeline ingests raw monthly sell-out (transaction) and sell-in (invoice) data, maps each transaction to its area, scores each area on four KPIs per product category, and produces ranked award tables. A Streamlit app serves the results.

---

## Tech Stack

- **Python 3.13** (installed at `C:\Users\pc\AppData\Local\Programs\Python\Python313\python`)
- **Polars ≥ 1.9.0** — all data transforms (no Pandas except Excel ingestion via `pd.read_excel`)
- **Streamlit** — awards viewer app
- **Parquet** — intermediate format for all data after ingestion

---

## Repository Layout

```
data/
  raw/                        # Source files — CSVs and Excel, never modified by pipeline
    sales_YYYY_MM.csv         # Monthly sell-out (one file per month, 2024 + 2025)
    org_structure_YYYY_MM.xlsx # Monthly rep → area/region org map
    distance_YYYY_MM.xlsx     # Monthly rep distance scores
    cash_recon_YYYY_MM.xlsx   # Monthly rep cash reconciliation scores
    sell_in_data.xlsx         # Full sell-in invoice data (Date, Value, Category, KAM, Region, Area)
    ASM_KAM_Area_Mapping.xlsx # Area reference table: 36 areas with their Region, ZSM, ASM/TSM, KAM
    ASM_Rep_Mapping.csv       # Rep code → Area/Region static fallback (1,084 reps)
    Weights.xlsx              # KPI weights (must sum to 1.0)
    sku_alias.xlsx            # SKU name aliases for category matching
    KAM Map.xlsx              # (legacy, superseded by ASM_KAM_Area_Mapping)
  raw_parquet/                # Parquet cache of everything in raw/ — auto-created by pipeline
  reference/
    category_mapping.csv      # mapping_key → target_category lookup
  processed/
    runs/
      YYYYMMDD_HHMMSS/        # Versioned run output (approved or rejected)
        run_manifest.json
        normalized_sell_out/  # Partitioned by year=/month=
        normalized_sell_in.parquet
        sell_out_monthly.parquet
        sell_in_monthly.parquet
        area_kpi_awards.parquet
        region_kpi_awards.parquet
        reconciliation_summary.parquet
        exception_summary.parquet
        exceptions/           # CSV dumps of bad rows

src/gb_awards/
  config.py       # TARGET_CATEGORIES, required columns, thresholds, AppPaths
  ingest.py       # Raw files → raw_parquet/ (idempotent; skips existing)
  io.py           # read_parquet(), discover_files(), inventory_files()
  normalize.py    # normalize_sell_out(), normalize_sell_in(), category assignment
  ranking.py      # aggregate_sell_out/in(), compute_awards()
  pipeline.py     # run_pipeline() — orchestrates everything, writes run outputs
  app.py          # Streamlit awards viewer
  utils.py        # Polars expression helpers, file helpers
  cli.py          # CLI entry point: `gb-awards etl`

data/reference/
  category_mapping.csv

tests/
app.py            # Streamlit entry point (calls src/gb_awards/app.py)
```

---

## How to Run

### Run the ETL pipeline
```bash
python -c "
import sys; sys.path.insert(0, 'src')
from gb_awards.pipeline import run_pipeline
run_dir = run_pipeline('.')
print(run_dir)
"
```
Or via CLI:
```bash
python -m gb_awards.cli etl --root .
```

### Run the Streamlit app
```bash
streamlit run app.py
```

### Check pipeline status after a run
```python
import json
from pathlib import Path
runs = sorted(Path('data/processed/runs').iterdir(), reverse=True)
manifest = json.loads((runs[0] / 'run_manifest.json').read_text())
print(manifest['status'], manifest['failures'])
```

---

## Pipeline Flow

```
raw/ files
    ↓  ingest.py  (idempotent — skips if parquet already exists)
raw_parquet/
    ↓  normalize.py
normalized_sell_out  (14.8M rows)
normalized_sell_in   (151K rows, scoped to areas in ASM_KAM_Area_Mapping)
    ↓  ranking.py aggregate_*()
sell_out_monthly / sell_in_monthly
    ↓  ranking.py compute_awards()
area_kpi_awards / region_kpi_awards
    ↓  pipeline.py validation gates
run_manifest.json  →  status: approved / rejected
```

---

## Ingestion (ingest.py)

All ingestion is **idempotent** — parquet files are only created once. To force re-ingestion, delete the relevant `.parquet` file from `data/raw_parquet/`.

| Source | Function | Output |
|---|---|---|
| `sales_202*.csv` | `ingest_sell_out_csvs()` | `sales_YYYY_MM.parquet` |
| `*.xlsx` in `raw/` | `ingest_excel_files()` | `<name>.parquet` |
| `*.csv` in `raw/` (non-sales) | `ingest_raw_reference_csvs()` | `<name>.parquet` |
| `*.csv` in `reference/` | `ingest_reference_csvs()` | `<name>.parquet` |

All CSVs are read with `infer_schema_length=0` (all columns as Utf8). Excel files are read via `pd.read_excel(dtype=str)` then converted to Polars.

---

## Rep → Area Mapping (3-level fallback)

Sell-out rows get their area assigned via a 3-level fallback:

1. **Monthly org join** — `(year, month, field_rep_username)` matched to `org_structure_YYYY_MM` + `distance_YYYY_MM` parquets
2. **Rep area fallback** — most common area for that rep across all 2025 org months (covers 2024 rows with no monthly file)
3. **Static ASM_Rep_Mapping** — case-insensitive match on rep code to `ASM_Rep_Mapping.parquet` (1,084 reps)

Reps still unmapped after all three levels are recorded in `exceptions/unmapped_reps.csv`. The gate fails if unmapped sell-out value exceeds **1% of total**.

---

## Sell-In Area Mapping

The primary join key for sell-in is the **`Area` column** in `sell_in_data.xlsx`. This is a direct area name written into the data — it is uppercased during normalization and matched against the 36 areas defined in `ASM_KAM_Area_Mapping.xlsx`.

`ASM_KAM_Area_Mapping.xlsx` is a **reference table** — it defines the 37 valid areas and their management attributes (Region, ZSM, ASM/TSM, KAM). It is not a join driver. The KAM column in the sell-in data is used as a secondary fallback only when `Area` is blank, joining on `KAM` name to look up the area. Sell-in rows with no resolvable area are out of scope and excluded (recorded in `exceptions/unmapped_kams.csv` for audit). There is no failure gate for this — exclusion is expected.

---

## Category Assignment

Categories are resolved in priority order:
1. SKU name → `category_mapping.csv`
2. SKU alias → `category_mapping.csv`
3. Brand → `category_mapping.csv`
4. Product category field → `category_mapping.csv`
5. Pattern fallback: Mayo (with ml < 20 → Sachet, else → Jar), Tomato, Cubes, Spices

**Target categories:** `Tomato`, `Mayo Jar`, `Mayo Sachet`, `Cubes`, `Spices`

---

## Scoring & Ranking

Two growth KPIs are weighted (defined in `Weights.xlsx`, must sum to 1.0):

| KPI | Weight key |
|---|---|
| Sell-In Growth vs YA | `sell_in_growth` |
| Sell-Out Growth vs YA | `sell_out_growth` |

**Hygiene** = `Cash Reconciliation Score × Distance Score` — acts as a quality multiplier on the sell-out component. Areas with poor hygiene have their sell-out points dampened.

**Scoring formula:**
```
Total Points = (Sell In Growth Rank Points  × sell_in_weight)
             + (Sell Out Growth Rank Points × sell_out_weight × Hygiene)
```

**Rank points** within each category: `valid_count + 1 - rank` (highest growth = most points). Nulls get 0 points.

Tiebreaker sort order: Weighted Score → Hygiene → Sell-In Growth → Sell-Out Growth → area/region name.

---

## How Area-Level Hygiene is Computed

Distance and cash reconciliation scores are collected at **rep level, monthly** (from `distance_YYYY_MM.xlsx` and `cash_recon_YYYY_MM.xlsx`). They are rolled up to a single annual area score in two steps:

**Step 1 — Monthly area score** (rep → area):
```
area_distance_jan = Σ(rep_distance_jan × rep_jan_sales) / Σ(rep_jan_sales)
```
Each rep's monthly score is weighted by their contribution to the area's sell-out value that month.

**Step 2 — Annual area score** (monthly → yearly):
```
area_distance_annual = Σ(area_distance_m × month_sales) / Σ(month_sales)
```
Each month's area score is weighted by that month's contribution to the area's full-year sell-out value.

Mathematically these two steps collapse into a single value-weighted average across all (rep, month) combinations — which is exactly what `weighted_avg_expr("distance_score", "sell_out_value")` computes in `_quality_compare()`.

**Notes:**
- Distance and cash recon scores only exist for **2025** — 2024 rows have null scores and are excluded from the weighted average automatically
- Reps with no score (null) in a given month are excluded from that month's calculation
- `Hygiene = area_distance_annual × area_cash_recon_annual`

---

## Validation Gates

The pipeline sets `status: rejected` if any gate fails:

| Gate | Threshold |
|---|---|
| Unmapped sell-out value | > 1% of total sell-out |
| Unmapped category value | > 0.5% of combined total |
| Duplicate sell-out partitions | any |
| Duplicate KAM mapping keys | any |
| Invalid dates | any |
| Invalid numeric values | any |

---

## Areas & Regions (36 areas, 7 regions)

| Region | Areas |
|---|---|
| LAGOS | GREATER LAGOS, IKORODU-KETU, LAGOS EAST, LAGOS NORTH I, LAGOS NORTH II, LAGOS WEST |
| NORTH-CENTRAL | ABUJA EAST, ABUJA WEST, BENUE-LOKOJA, NIGER, PLATEAU-NASSARAWA |
| NORTH-EAST | ADAMAWA-TARABA, BAUCHI-GOMBE, BORNO-YOBE |
| NORTH-WEST | KADUNA, KANO-KATSINA-JIGAWA, SOKOTO-KEBBI-ZAMFARA |
| SOUTH-EAST | ABAKALIKI, ANAMBRA-ASABA, DELTA, EDO, ENUGU, OWERRI |
| SOUTH-SOUTH | ABA, BAYELSA, CALABAR, PORT-HARCOURT I, PORT-HARCOURT II, UYO |
| SOUTH-WEST | ABEOKUTA, EKITI, KWARA, ONDO, OSUN, OYO I, OYO II |

**Note:** PLATEAU and NASSARAWA are combined as `PLATEAU-NASSARAWA` in the mapping. Region names in the sell-in data use "North Central" (no hyphen) — this is uppercased to "NORTH CENTRAL" during normalization; only the `area` column drives the awards join, not region.

---

## Adding New Monthly Data

Each month, add the following files to `data/raw/`:
- `sales_YYYY_MM.csv` — sell-out transactions
- `org_structure_YYYY_MM.xlsx` — org map
- `distance_YYYY_MM.xlsx` — distance scores
- `cash_recon_YYYY_MM.xlsx` — cash recon scores

The pipeline will auto-ingest them on next run (idempotent — existing parquets are skipped).

To refresh the sell-in data: replace `data/raw/sell_in_data.xlsx`, delete `data/raw_parquet/sell_in_data.parquet`, then re-run.

To refresh the area reference table: update `data/raw/ASM_KAM_Area_Mapping.xlsx`, delete `data/raw_parquet/ASM_KAM_Area_Mapping.parquet`, then re-run.

---

## Key Design Decisions

- **All transforms use Polars** — no Pandas in the pipeline except `pd.read_excel()` for ingestion (Polars has no native Excel reader)
- **`how="diagonal_relaxed"`** for all `pl.concat()` calls — monthly files have different schemas (Null vs String for some columns)
- **`how="full", coalesce=True`** for outer joins (Polars 1.9.0 syntax)
- **`infer_schema_length=0`** on all `pl.read_csv()` — reads every column as Utf8 to avoid type inference errors on messy data
- **Awards are area-based** — all scoring, ranking, and output is at the Area level; KAM and ASM are reference metadata only, not join keys or award subjects
- **Sell-in joins by Area** — the `Area` column in `sell_in_data.xlsx` is the primary key; KAM name is a secondary fallback only; `ASM_KAM_Area_Mapping` defines the valid area universe, not the join logic
- **Area names are uppercased** — all area/region values normalized to uppercase to ensure consistent joins across data sources
- **Run versioning** — every pipeline execution writes to a new timestamped folder; the app always shows the latest approved run

---

## Polars Version Notes (1.9.0)

- `rank()` does **not** support `nulls_last` parameter — handle nulls separately
- `str.to_datetime()` does **not** support `use_earliest` parameter
- Pivot uses `on=` not `columns=`
- Outer join uses `how="full", coalesce=True`

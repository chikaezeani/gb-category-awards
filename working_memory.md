# Working Memory — GB 2026 Category Awards

_This file is maintained by the agent. Update it at the completion of every task._
_Read this file alongside `CLAUDE.md` at the start of every session._

---

## Project Status
**Pipeline:** ✅ Approved — zero failures
**Last run:** `20260517_211226`
**Awards output:** 252 area awards (36 areas × 7 categories), 49 region awards (7 regions × 7 categories)

---

## What Has Been Built (Session History)

### Session 1 — Initial Build
- Read `Plans/StartOff_Plan.md` and understood the project goal
- Built the full ETL pipeline from scratch:
  - `ingest.py` — raw CSV/Excel → Parquet (idempotent)
  - `normalize.py` — sell-out and sell-in normalization, category assignment
  - `ranking.py` — aggregation, scoring, ranking
  - `pipeline.py` — orchestrator with validation gates and versioned run output
  - `app.py` — Streamlit viewer
  - `utils.py` — Polars expression helpers
  - `config.py` — constants, paths, thresholds
- Migrated everything to **Polars** (removed Pandas from all transforms; kept only for Excel ingestion)
- All raw files converted to Parquet first before any processing

### Session 2 — Bug Fixes
Fixed a series of errors that prevented the pipeline from running:

| Error | Fix |
|---|---|
| `SchemaError: type String incompatible with Null` in `pl.concat` | Changed all concat calls to `how="diagonal_relaxed"` |
| `DataValidationError: missing required columns` | Moved `validate_columns()` call to BEFORE `_rename_existing()` in header normalization |
| `TypeError: use_earliest not a valid argument` | Removed `use_earliest=True` from all `str.to_datetime()` calls (not in Polars 1.9.0) |
| `DuplicateError: column "notes_right" already exists` | Used `cat_map_slim = category_map.select(["mapping_key", "target_category"])` for all joins |
| `TypeError: rank() got unexpected argument 'nulls_last'` | Removed `nulls_last=True` from rank call (not in Polars 1.9.0) |

### Session 3 — Rep/KAM Mapping Fixes
- **Problem:** Pipeline rejected with unmapped sell-out (1.57%) and unmapped sell-in (5.04%)
- **Sell-in fix:** User updated `ASM_KAM_Area_Mapping.xlsx` with 4 missing KAMs. Parquet refreshed.
- **Sell-out fix:** Discovered `ASM_Rep_Mapping.csv` (1,084 reps) as a 3rd-level fallback
  - Added `ingest_raw_reference_csvs()` to ingest non-sales CSVs from `raw/`
  - Added `load_rep_static_mapping()` in `normalize.py`
  - Applied as 3rd-level fallback after monthly org join and rep_area_fallback
  - Unmapped sell-out dropped from 1.57% → 0.81% ✅
- **Sell-in scope change:** User clarified awards are area-based, not KAM-based
  - Sell-in is now scoped by the `Area` column in `sell_in_data.xlsx` (direct area name)
  - Rows with no resolvable area are excluded silently (not a failure gate)
  - Removed the unmapped sell-in validation gate from `pipeline.py`

### Session 4 — Data Verification & Sell-In Area Fix
- Verified all 37 areas have both sell-in and sell-out data
- User updated `sell_in_data.xlsx` with a direct `Area` column
  - Deleted old `sell_in_data.parquet`, re-converted from updated Excel
  - Updated `normalize_sell_in()` to use `Area` column directly (uppercased), with KAM join as fallback
- Fixed **PLATEAU-NASSARAWA** gap:
  - Sell-in data had `"Plateau-Nassarawa"` (mixed case) — uppercasing fixed the match
  - `ASM_KAM_Area_Mapping.parquet` had PLATEAU and NASSARAWA as separate rows — combined into one `PLATEAU-NASSARAWA` entry
  - All 36 areas now have sell-in data ✅

### Session 5 — Scoring Formula Change
- **Old formula:** 4 KPIs each with own weight and rank points (sell-in growth, sell-out growth, distance, cash recon)
- **New formula:**
  ```
  Total Points = (Sell In Growth Rank Points × sell_in_weight)
               + (Sell Out Growth Rank Points × sell_out_weight × Hygiene)
  Hygiene = Cash Reconciliation Score × Distance Score
  ```
- User updated `Weights.xlsx` — now only 2 keys: `sell_in_growth` (0.5) and `sell_out_growth` (0.5)
- Updated `Weights.parquet` (deleted and re-created)
- Updated `ranking.py`: removed Distance and Cash Recon rank points, new weighted score formula
- Updated `pipeline.py` `validate_weights()` — now only requires 2 keys
- Updated `CLAUDE.md` scoring section

### Session 6 — Documentation
- Wrote full `CLAUDE.md` covering: project overview, layout, pipeline flow, ingestion, mapping fallbacks, category assignment, scoring, validation gates, areas/regions, monthly data instructions, design decisions, Polars version notes
- Updated CLAUDE.md to clarify **awards are area-based** (KAM/ASM are metadata only, not join keys)
- Documented two-step hygiene calculation (rep-level monthly → area-level annual via value-weighted avg)
- Created this `working_memory.md`

### Session 9 — Unmapped Reps: Channel Exclusion & Pattern Mapping
- **Problem:** ₦2.86B in unmapped sell-out value (145,486 rows)
- **Fix in `normalize.py`** — two additions after `pl.concat(frames, ...)`:
  1. **FS/WP channel exclusion** (early filter before exceptions): rows where `field_rep_username` contains "FS" or "WP" (case-insensitive) are removed from the pipeline entirely and logged as a new `excluded_channels` exception. This covers Food Service (`GB/FS/KAO/*`, `KN/FS/*`, etc.) and Womenpreneur (`gb/se/wp/onitsha/*`) channels.
  2. **Level 4 username pattern fallback**: for rows still unmapped after the 3-level fallback, pattern-matches on `field_rep_username` to assign area — KADUNA, EKITI, KWARA, ONDO, OSUN.
- **Result (run `20260517_161615`):**
  - `excluded_channels`: 38,266 rows / ₦2.13B (FS+WP, intentionally excluded)
  - `unmapped_reps`: 70,985 rows / ₦540M (~0.15% of total — down from ₦2.86B) ✅
- **Remaining unmapped:** ~₦540M from LG, ABJ, OYO, OGUN, PHC patterns — accepted as-is (below 1% gate)

### Session 8 — Mayo Sell-In Bug Fix
- **Bug:** Mayo Jar and Mayo Sachet showed 0 sell-in data for all areas
- **Root cause:** Sell-in `Category = "Mayo"` correctly mapped to `"Mayo Dynamic"` via `category_mapping.csv`, but `normalize_sell_in()` only resolved `Mayo Dynamic → Jar/Sachet` when `target_category` was `null`. The 46,886 Mayo rows with `target_category = "Mayo Dynamic"` were dropped by `aggregate_sell_in()` which filters to `TARGET_CATEGORIES` only.
- **Fix:** In `normalize_sell_in()`, changed the fallback condition from `target_category.is_null()` to `target_category.is_null() | target_category.eq("Mayo Dynamic")` so all Mayo rows get resolved using ml-value from the Description column.
- **Result:** Mayo Jar: ₦34.9B (2024) / ₦50.8B (2025); Mayo Sachet: ₦8.1B (2024) / ₦11.6B (2025); all 36 areas covered for both categories ✅
- Latest run `20260517_140237`: approved, 0 failures ✅

### Session 7 — Region Name Fix & Area Count Correction
- **Bug:** Region-level awards had 13 "regions" (53 rows) instead of 7 (35 rows)
  - Root cause: sell-in raw `Region` column had space-separated names ("NORTH CENTRAL") while sell-out org files had hyphenated names ("NORTH-CENTRAL") — treated as separate groups during ranking
  - Fix: removed the raw Region column override in `normalize_sell_in()`; now re-derives region from the resolved `area` via a canonical join on `ASM_KAM_Area_Mapping` after area is set
- **Correction:** Area count is 36 (not 37) — PLATEAU + NASSARAWA were merged into PLATEAU-NASSARAWA, reducing the count from 37 to 36. Updated CLAUDE.md (was "37 areas, 6 regions" → "36 areas, 7 regions")
- Latest run `20260517_132723`: approved, 0 failures, 180 area awards, 35 region awards ✅

---

## Current Data State

### Raw Files Present
- **Sell-out:** `sales_2024_01` through `sales_2024_12`, `sales_2025_01` through `sales_2025_12` (24 files)
- **Org structure:** `org_structure_2025_01` through `org_structure_2025_12` (12 files, 2025 only)
- **Distance:** `distance_2025_01` through `distance_2025_12` (no 2024 files)
- **Cash recon:** `cash_recon_2025_01` through `cash_recon_2025_12` (no 2024 files)
- **Sell-in:** `sell_in_data.xlsx` — includes `Area` and `Region` columns directly
- **Mappings:** `ASM_KAM_Area_Mapping.xlsx` (37 areas), `ASM_Rep_Mapping.csv` (1,084 reps)

### Key Numbers (Latest Approved Run)
| Metric | Value |
|---|---|
| Sell-Out 2024 | ₦146.4B / 17.8M cases / 1.22B units |
| Sell-Out 2025 | ₦204.5B / 21.2M cases / 1.34B units |
| Sell-In 2024 | ₦129.4B / 21.7M qty |
| Sell-In 2025 | ₦156.2B / 22.7M qty |
| Normalized sell-out rows | 14,863,932 |
| Normalized sell-in rows | 151,011 |
| Area awards | 180 |
| Region awards | 53 |

---

## Key Design Decisions (Immutable — Do Not Change Without Discussion)

1. **Awards are area-based** — KAM and ASM are display metadata only. Never use KAM/ASM as a join key or scoring dimension.
2. **Sell-in primary join = Area column** — the `Area` column in `sell_in_data.xlsx` is the source of truth. KAM is a fallback only.
3. **Sell-in scope = ASM_KAM_Area_Mapping** — rows with no resolvable area are excluded silently. No failure gate.
4. **3-level rep fallback** — monthly org → rep mode area across 2025 → ASM_Rep_Mapping static table.
5. **Hygiene as sell-out multiplier** — hygiene is NOT a separately ranked KPI. It multiplies the sell-out rank points only.
6. **All area names uppercased** — enforced during normalization. Ensures consistent joins.
7. **PLATEAU-NASSARAWA is one combined area** — both are merged in `ASM_KAM_Area_Mapping.parquet`. The Excel file has them combined too.
8. **Polars 1.9.0 constraints** — `rank()` has no `nulls_last`; `str.to_datetime()` has no `use_earliest`; pivot uses `on=`; outer join uses `how="full", coalesce=True`.

---

## Files That Are Manually Managed (Not Auto-Regenerated)

If any of these source files are updated, the corresponding parquet **must be manually deleted** before the next pipeline run so it gets re-ingested:

| Source File | Parquet to Delete | When to Refresh |
|---|---|---|
| `data/raw/sell_in_data.xlsx` | `data/raw_parquet/sell_in_data.parquet` | Sell-in data updated |
| `data/raw/ASM_KAM_Area_Mapping.xlsx` | `data/raw_parquet/ASM_KAM_Area_Mapping.parquet` | Area/KAM/ASM assignments change |
| `data/raw/ASM_Rep_Mapping.csv` | `data/raw_parquet/ASM_Rep_Mapping.parquet` | New reps added to static mapping |
| `data/raw/Weights.xlsx` | `data/raw_parquet/Weights.parquet` | KPI weights change |
| `data/raw/sku_alias.xlsx` | `data/raw_parquet/sku_alias.parquet` | SKU aliases updated |
| `data/reference/category_mapping.csv` | `data/raw_parquet/category_mapping.parquet` | Category mapping updated |

---

## Known Limitations / Watch Points

- **~0.81% sell-out value unmapped** — reps like `GB/FS/KAO/*` and `gb/se/wp/onitsha/*` are not in any org file or ASM_Rep_Mapping. This is below the 1% gate threshold and is accepted. These appear to be non-standard channel reps.
- **No 2024 hygiene scores** — distance and cash recon files only exist for 2025. Hygiene is therefore computed from 2025 data only.
- **PLATEAU-NASSARAWA has one ASM/KAM** — when merging PLATEAU and NASSARAWA, the PLATEAU entry (ABAH, MARY / Mary, Abah) was kept. NASSARAWA's manager (OMEIFE, DANIEL) was dropped. Confirm with business if this is correct.
- **BORNO-YOBE, ABAKALIKI, ABA ASM are Vacant** — recorded in the mapping as "Vacant (formerly ...)". These areas still participate in awards via sell-in and sell-out data.

---

### Session 10 — Add "Total" Aggregate Category
- Added a 6th synthetic "Total" category to `compute_awards()` in `ranking.py`
- "Total" sums sell-in and sell-out values across all 5 categories per area/region, then ranks areas on that combined performance
- Hygiene for "Total" = value-weighted avg across all categories combined (same grain, all categories)
- "Total" does NOT go in `TARGET_CATEGORIES` (that would break ingestion filters) — produced entirely inside `compute_awards()` after pivoting
- Added "Total" to the category dropdown in `app.py`
- Area awards: 180 → 216 (36 × 6); Region awards: 35 → 42 (7 × 6). Run `20260517_164214`: approved ✅

### Session 11 — Cloud Deployment
- Added password gate to `src/gb_awards/app.py` — password is `GBFoods2026`
- Created `.gitignore` (excludes raw data, raw_parquet, heavy pipeline intermediates, .venv)
- Created `requirements.txt` (streamlit, polars, pandas, openpyxl, pyarrow)
- Initialized git repo, committed all files, pushed to GitHub: `github.com/chikaezeani/gb-category-awards` (public repo)
- Deployed on Streamlit Community Cloud — live at: **https://gb-category-awards.streamlit.app**

### Session 12 — Innovations Category
- Added `INNOVATIONS_SELL_OUT_PATTERNS` and `INNOVATIONS_SELL_IN_PATTERNS` to `config.py` — the only missing piece; `ranking.py`, `pipeline.py`, and `app.py` already had the full Innovations implementation
- **Innovations = Cubes + Peppered Chicken Tomato + Asun Tomato + Gino Hot Pepper**
- Sell-out matched via `sku` column (lowercased): `"peppered chicken tomato"`, `"asun tomato"`, `"gino hot pepper"`
- Sell-in matched via `Description` column (lowercased): `"peppered chicken"`, `"tsm asun"`, `"gi pepper pwd"` — plus full Cubes category
- **Scoring:** same weighted rank-points formula as other categories but on 2025 absolute values (no YoY growth — new products have no 2024 baseline). Rank points from sell-in 2025 value rank + sell-out 2025 value rank, then `Weighted Score = (SI Rank Points × sell_in_weight) + (SO Rank Points × sell_out_weight × Hygiene)`
- Area awards: 216 → 252 (36 × 7); Region awards: 42 → 49 (7 × 7). Run `20260517_210312`: approved ✅

## Next Steps
- [ ] Generate a new GitHub token when pushing future pipeline runs (old token was deleted)

## Known Corrections
- Area count is **36** (not 37) — PLATEAU-NASSARAWA is one merged area
- Region count is **7** (not 6) — CLAUDE.md had both wrong; now corrected

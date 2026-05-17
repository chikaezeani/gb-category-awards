from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from .config import (
    KAM_MAPPING_REQUIRED_COLUMNS,
    SELL_IN_REQUIRED_COLUMNS,
    SELL_OUT_REQUIRED_COLUMNS,
    TARGET_CATEGORIES,
)
from .io import discover_files, read_parquet
from .utils import norm_str, normalize_text, parse_dates, parse_money


class DataValidationError(Exception):
    """Raised when a quality gate blocks the run."""


@dataclass
class ExceptionBundle:
    tables: dict[str, pl.DataFrame] = field(default_factory=dict)

    def add(self, name: str, frame: pl.DataFrame) -> None:
        self.tables[name] = frame

    def get(self, name: str) -> pl.DataFrame:
        return self.tables.get(name, pl.DataFrame())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def validate_columns(df: pl.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise DataValidationError(f"{label} is missing required columns: {missing}")


def parse_period_from_name(path: Path) -> tuple[int, int]:
    match = re.search(r"_(\d{4})_(\d{2})", path.stem)
    if not match:
        raise DataValidationError(f"Unable to infer period from file name: {path.name}")
    return int(match.group(1)), int(match.group(2))


def _rename_existing(df: pl.DataFrame, rename_map: dict[str, str]) -> pl.DataFrame:
    """Rename only columns that actually exist — silently skip missing keys."""
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(existing) if existing else df


def _normalize_sell_out_headers(df: pl.DataFrame) -> pl.DataFrame:
    """Standardise sell-out column names across all monthly file variants."""
    header_map = {
        "CREATED AT": "created_at",
        "CREATED_AT": "created_at",
        "CREATED": "created_at",
        "Sales ID": "sales_id",
        "Field Rep Username": "field_rep_username",
        "Field Rep Name": "field_rep_name",
        "Region": "raw_region",
        "Product Category": "source_category",
        "Product Brand": "source_brand",
        "Product Name (SKU)": "sku",
        "Total Value": "total_value",
    }
    # Drop completely empty/unnamed columns first
    df = df.select([c for c in df.columns if c.strip()])
    # Validate using original column names BEFORE renaming
    validate_columns(df, SELL_OUT_REQUIRED_COLUMNS, "sell-out data")
    return _rename_existing(df, header_map)


# ---------------------------------------------------------------------------
# Reference data loaders
# ---------------------------------------------------------------------------

def load_category_mapping(raw_parquet_dir: Path) -> pl.DataFrame:
    df = read_parquet(raw_parquet_dir / "category_mapping.parquet")
    df = df.with_columns([
        pl.col("mapping_key").str.strip_chars().str.to_lowercase().alias("mapping_key"),
        norm_str(pl.col("target_category")).alias("target_category"),
    ])
    allowed = set(TARGET_CATEGORIES) | {"Mayo Dynamic"}
    unknown = sorted(set(df["target_category"].to_list()) - allowed - {""})
    if unknown:
        raise DataValidationError(f"Unexpected categories in category mapping: {unknown}")
    return df


def load_sku_aliases(raw_parquet_dir: Path) -> pl.DataFrame:
    df = read_parquet(raw_parquet_dir / "sku_alias.parquet")
    df = _rename_existing(df, {"SKU": "sku", "Alias": "alias"})
    return df.select([
        norm_str(pl.col("sku")).alias("sku"),
        norm_str(pl.col("alias")).alias("alias"),
    ])


# ---------------------------------------------------------------------------
# Category assignment
# ---------------------------------------------------------------------------

def assign_target_categories(df: pl.DataFrame, category_map: pl.DataFrame) -> pl.DataFrame:
    """Vectorised category mapping with Mayo ml-based sub-classification."""
    lookup = dict(zip(category_map["mapping_key"].to_list(), category_map["target_category"].to_list()))

    # Build normalised lookup key columns
    df = df.with_columns([
        ("sku::" + norm_str(pl.col("sku")).str.to_lowercase()).alias("_sku_key"),
        ("alias::" + norm_str(pl.col("alias")).str.to_lowercase()).alias("_alias_key"),
        ("brand::" + norm_str(pl.col("source_brand")).str.to_lowercase()).alias("_brand_key"),
        ("category::" + norm_str(pl.col("source_category")).str.to_lowercase()).alias("_cat_key"),
    ])

    # Only keep the two columns we need to avoid dragging extra columns into every join
    cat_map_slim = category_map.select(["mapping_key", "target_category"])
    for key_col, result_col in [
        ("_sku_key", "_cat_sku"),
        ("_alias_key", "_cat_alias"),
        ("_brand_key", "_cat_brand"),
        ("_cat_key", "_cat_cat"),
    ]:
        df = df.join(
            cat_map_slim.rename({"mapping_key": key_col, "target_category": result_col}),
            on=key_col,
            how="left",
        )

    # Combine with precedence: sku > alias > brand > category
    df = df.with_columns(
        pl.coalesce(["_cat_sku", "_cat_alias", "_cat_brand", "_cat_cat"]).alias("target_category")
    )

    # Combined text for fallback pattern matching
    combined = (
        norm_str(pl.col("source_category")).str.to_lowercase() + " "
        + norm_str(pl.col("source_brand")).str.to_lowercase() + " "
        + norm_str(pl.col("sku")).str.to_lowercase() + " "
        + norm_str(pl.col("alias")).str.to_lowercase()
    )
    has_ml = combined.str.contains(r"\d+(?:\.\d+)?\s*ml")
    ml_value = combined.str.extract(r"(\d+(?:\.\d+)?)\s*ml", group_index=1).cast(pl.Float64, strict=False)
    is_mayo = combined.str.contains(r"mayo|mayonnaise|bama")
    is_sachet = is_mayo & has_ml & (ml_value < 20)

    df = df.with_columns([
        combined.alias("_combined"),
        ml_value.alias("_ml_value"),
        is_mayo.alias("_is_mayo"),
        is_sachet.alias("_is_sachet"),
    ])

    # Resolve Mayo Dynamic rows from the mapping
    is_mayo_dynamic = pl.col("target_category").eq("Mayo Dynamic")
    df = df.with_columns(
        pl.when(is_mayo_dynamic & pl.col("_is_sachet"))
        .then(pl.lit("Mayo Sachet"))
        .when(is_mayo_dynamic)
        .then(pl.lit("Mayo Jar"))
        .otherwise(pl.col("target_category"))
        .alias("target_category")
    )

    # General fallback for rows still unclassified
    df = df.with_columns(
        pl.when(pl.col("target_category").is_null() & pl.col("_is_sachet"))
        .then(pl.lit("Mayo Sachet"))
        .when(pl.col("target_category").is_null() & pl.col("_is_mayo"))
        .then(pl.lit("Mayo Jar"))
        .when(pl.col("target_category").is_null() & pl.col("_combined").str.contains(r"tomato|jumbo"))
        .then(pl.lit("Tomato"))
        .when(pl.col("target_category").is_null() & pl.col("_combined").str.contains(r"cube"))
        .then(pl.lit("Cubes"))
        .when(pl.col("target_category").is_null() & pl.col("_combined").str.contains(r"spice|curry|thyme"))
        .then(pl.lit("Spices"))
        .otherwise(pl.col("target_category"))
        .alias("target_category")
    )

    return df.drop([c for c in df.columns if c.startswith("_")])


# ---------------------------------------------------------------------------
# Rep / org mapping
# ---------------------------------------------------------------------------

def load_rep_static_mapping(raw_parquet_dir: Path) -> pl.DataFrame:
    """Load the static rep→area fallback from ASM_Rep_Mapping.parquet.

    Returns a DataFrame with columns (field_rep_username [lowercase], area, region).
    Returns an empty frame if the file doesn't exist.
    """
    path = raw_parquet_dir / "ASM_Rep_Mapping.parquet"
    if not path.exists():
        return pl.DataFrame(schema={"field_rep_username": pl.Utf8, "area": pl.Utf8, "region": pl.Utf8})
    df = read_parquet(path)
    # Strip spaces from column names
    col_renames = {c: c.strip() for c in df.columns if c != c.strip()}
    if col_renames:
        df = df.rename(col_renames)
    # Strip whitespace from string values
    df = df.with_columns([
        pl.col(c).str.strip_chars()
        for c in df.columns
        if df.schema[c] == pl.Utf8
    ])
    required = {"Rep Code", "Regions", "Area"}
    if not required.issubset(set(df.columns)):
        return pl.DataFrame(schema={"field_rep_username": pl.Utf8, "area": pl.Utf8, "region": pl.Utf8})
    return (
        df.filter(pl.col("Rep Code").is_not_null() & pl.col("Rep Code").ne(""))
        .select([
            pl.col("Rep Code").str.to_lowercase().alias("field_rep_username"),
            norm_str(pl.col("Area")).alias("area"),
            norm_str(pl.col("Regions")).alias("region"),
        ])
        .unique("field_rep_username", keep="first")
    )


def build_monthly_rep_mapping(
    raw_parquet_dir: Path,
    exceptions: ExceptionBundle,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build per-month rep→area mapping.

    Returns
    -------
    monthly_mapping
        One row per (year, month, field_rep_username).
    rep_area_fallback
        One row per field_rep_username — most common area across all months.
        Used to assign area to sell-out rows from years with no monthly org file.
    """
    records: list[pl.DataFrame] = []
    duplicate_tables: list[pl.DataFrame] = []

    for org_path in sorted(discover_files(raw_parquet_dir, "org_structure_*.parquet")):
        year, month = parse_period_from_name(org_path)
        distance_path = raw_parquet_dir / f"distance_{year}_{month:02d}.parquet"
        if not distance_path.exists():
            continue

        org = _rename_existing(read_parquet(org_path), {
            "rep_usercode": "field_rep_username",
            "rep_name": "field_rep_name",
            "area": "org_area",
            "region": "org_region",
        }).with_columns(norm_str(pl.col("field_rep_username")).alias("field_rep_username"))
        org = org.filter(pl.col("field_rep_username").ne(""))

        distance = _rename_existing(read_parquet(distance_path), {
            "AREA": "distance_area",
            "REGIONS": "distance_region",
            "FIELD REP USERNAME": "field_rep_username",
            "distance_score": "distance_score",
        }).with_columns(norm_str(pl.col("field_rep_username")).alias("field_rep_username"))
        distance = distance.filter(pl.col("field_rep_username").ne(""))

        # Record duplicates (for audit) but deduplicate before use
        for frame_name, frame in [("org_structure", org), ("distance", distance)]:
            dups = frame.filter(pl.col("field_rep_username").is_duplicated())
            if not dups.is_empty():
                dups = dups.with_columns([
                    pl.lit(year).alias("year"),
                    pl.lit(month).alias("month"),
                    pl.lit(frame_name).alias("source"),
                ])
                duplicate_tables.append(dups)

        distance = distance.unique("field_rep_username", keep="first")
        org = org.unique("field_rep_username", keep="first")

        # Outer join: take area from distance first, fall back to org
        monthly = (
            distance.select(["field_rep_username", "distance_area", "distance_region", "distance_score"])
            .join(
                org.select(["field_rep_username", "field_rep_name", "org_area", "org_region"]),
                on="field_rep_username",
                how="full",
                coalesce=True,
            )
        )
        monthly = monthly.with_columns([
            pl.lit(year).alias("year"),
            pl.lit(month).alias("month"),
            pl.coalesce([norm_str(pl.col("distance_area")), norm_str(pl.col("org_area"))]).alias("area"),
            pl.coalesce([norm_str(pl.col("distance_region")), norm_str(pl.col("org_region"))]).alias("region"),
            pl.col("distance_score").cast(pl.Float64, strict=False),
        ])
        records.append(monthly)

    dups_frame = pl.concat(duplicate_tables, how="diagonal_relaxed") if duplicate_tables else pl.DataFrame()
    exceptions.add("duplicate_mapping_keys", dups_frame)

    monthly_mapping = pl.concat(records, how="diagonal_relaxed") if records else pl.DataFrame()

    # Consolidated fallback: most common area per rep across all months
    rep_area_fallback = (
        monthly_mapping.filter(pl.col("area").ne("") & pl.col("area").is_not_null())
        .group_by(["field_rep_username", "area", "region"])
        .agg(pl.len().alias("_cnt"))
        .sort(["field_rep_username", "_cnt"], descending=[False, True])
        .group_by("field_rep_username", maintain_order=True)
        .first()
        .select(["field_rep_username", "area", "region"])
    )

    return monthly_mapping, rep_area_fallback


def build_cash_recon(raw_parquet_dir: Path) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    for path in sorted(discover_files(raw_parquet_dir, "cash_recon_*.parquet")):
        year, month = parse_period_from_name(path)
        df = _rename_existing(read_parquet(path), {"Rep Code": "field_rep_username", "cash_recon_score": "cash_recon_score"})
        df = df.filter(norm_str(pl.col("field_rep_username")).ne(""))
        df = df.with_columns([
            pl.lit(year).alias("year"),
            pl.lit(month).alias("month"),
            norm_str(pl.col("field_rep_username")).alias("field_rep_username"),
            pl.col("cash_recon_score").cast(pl.Float64, strict=False),
        ])
        frames.append(df.select(["year", "month", "field_rep_username", "cash_recon_score"]))
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


# ---------------------------------------------------------------------------
# Sell-out normalisation
# ---------------------------------------------------------------------------

def normalize_sell_out(
    raw_parquet_dir: Path,
    exceptions: ExceptionBundle,
) -> pl.DataFrame:
    category_map = load_category_mapping(raw_parquet_dir)
    sku_alias = load_sku_aliases(raw_parquet_dir)
    monthly_mapping, rep_area_fallback = build_monthly_rep_mapping(raw_parquet_dir, exceptions)
    rep_static_mapping = load_rep_static_mapping(raw_parquet_dir)
    cash_recon = build_cash_recon(raw_parquet_dir)

    frames: list[pl.DataFrame] = []
    partition_inventory: list[tuple[int, int]] = []

    for path in sorted(discover_files(raw_parquet_dir, "sales_*.parquet")):
        year, month = parse_period_from_name(path)
        partition_inventory.append((year, month))

        df = _normalize_sell_out_headers(read_parquet(path))
        df = df.with_columns([
            pl.lit(path.name).alias("source_file"),
            pl.lit(year).cast(pl.Int32).alias("year"),
            pl.lit(month).cast(pl.Int32).alias("month"),
            parse_dates(pl.col("created_at")).alias("created_at"),
            parse_money(pl.col("total_value")).alias("total_value_numeric"),
            norm_str(pl.col("field_rep_username")).alias("field_rep_username"),
            norm_str(pl.col("source_category")).alias("source_category"),
            norm_str(pl.col("source_brand")).alias("source_brand"),
            norm_str(pl.col("sku")).alias("sku"),
        ])

        # SKU alias join
        df = df.join(sku_alias, on="sku", how="left").with_columns(
            pl.col("alias").fill_null("")
        )

        # Category assignment
        df = assign_target_categories(df, category_map)

        # Monthly rep mapping join
        monthly_cols = monthly_mapping.select(
            ["year", "month", "field_rep_username", "area", "region", "distance_score"]
        ) if not monthly_mapping.is_empty() else pl.DataFrame(
            schema={"year": pl.Int32, "month": pl.Int32, "field_rep_username": pl.Utf8,
                    "area": pl.Utf8, "region": pl.Utf8, "distance_score": pl.Float64}
        )
        df = df.join(monthly_cols, on=["year", "month", "field_rep_username"], how="left")

        # Fallback area for years/reps with no monthly org file (e.g. all 2024 data)
        if not rep_area_fallback.is_empty():
            df = df.join(
                rep_area_fallback.rename({"area": "_fb_area", "region": "_fb_region"}),
                on="field_rep_username",
                how="left",
            ).with_columns([
                pl.when(pl.col("area").is_null() | pl.col("area").eq(""))
                .then(pl.col("_fb_area"))
                .otherwise(pl.col("area"))
                .alias("area"),
                pl.when(pl.col("region").is_null() | pl.col("region").eq(""))
                .then(pl.col("_fb_region"))
                .otherwise(pl.col("region"))
                .alias("region"),
            ]).drop(["_fb_area", "_fb_region"])

        # 3rd-level fallback: static ASM rep mapping (ASM_Rep_Mapping.csv)
        # Joins case-insensitively via a temporary lowercase key column
        if not rep_static_mapping.is_empty():
            df = df.with_columns(
                pl.col("field_rep_username").str.to_lowercase().alias("_rep_lc")
            ).join(
                rep_static_mapping.rename({
                    "field_rep_username": "_rep_lc",
                    "area": "_sm_area",
                    "region": "_sm_region",
                }),
                on="_rep_lc",
                how="left",
            ).with_columns([
                pl.when(pl.col("area").is_null() | pl.col("area").eq(""))
                .then(pl.col("_sm_area"))
                .otherwise(pl.col("area"))
                .alias("area"),
                pl.when(pl.col("region").is_null() | pl.col("region").eq(""))
                .then(pl.col("_sm_region"))
                .otherwise(pl.col("region"))
                .alias("region"),
            ]).drop(["_rep_lc", "_sm_area", "_sm_region"])

        # Cash recon join
        df = df.join(
            cash_recon if not cash_recon.is_empty() else pl.DataFrame(
                schema={"year": pl.Int32, "month": pl.Int32, "field_rep_username": pl.Utf8, "cash_recon_score": pl.Float64}
            ),
            on=["year", "month", "field_rep_username"],
            how="left",
        )

        frames.append(df)

    full = pl.concat(frames, how="diagonal_relaxed")

    # Channel exclusion: Food Service (FS) and Womenpreneur (WP) are non-standard channels
    # excluded from awards scoring entirely — not counted as unmapped
    _rep_upper = pl.col("field_rep_username").str.to_uppercase()
    _is_excluded = _rep_upper.str.contains("FS") | _rep_upper.str.contains("WP")
    exceptions.add("excluded_channels", full.filter(_is_excluded))
    full = full.filter(~_is_excluded)

    # Level 4 fallback: pattern-match field_rep_username for known area codes
    _rep_up = pl.col("field_rep_username").str.to_uppercase()
    _still_unmapped = pl.col("area").is_null() | pl.col("area").eq("")
    full = full.with_columns([
        pl.when(_still_unmapped)
        .then(
            pl.when(_rep_up.str.contains("KADUNA")).then(pl.lit("KADUNA"))
            .when(_rep_up.str.contains("EKITI")).then(pl.lit("EKITI"))
            .when(_rep_up.str.contains("KWARA")).then(pl.lit("KWARA"))
            .when(_rep_up.str.contains("ONDO")).then(pl.lit("ONDO"))
            .when(_rep_up.str.contains("OSUN")).then(pl.lit("OSUN"))
            .otherwise(pl.col("area"))
        )
        .otherwise(pl.col("area"))
        .alias("area"),
        pl.when(_still_unmapped & (pl.col("region").is_null() | pl.col("region").eq("")))
        .then(
            pl.when(_rep_up.str.contains("KADUNA")).then(pl.lit("NORTH-WEST"))
            .when(_rep_up.str.contains("EKITI")).then(pl.lit("SOUTH-WEST"))
            .when(_rep_up.str.contains("KWARA")).then(pl.lit("SOUTH-WEST"))
            .when(_rep_up.str.contains("ONDO")).then(pl.lit("SOUTH-WEST"))
            .when(_rep_up.str.contains("OSUN")).then(pl.lit("SOUTH-WEST"))
            .otherwise(pl.col("region"))
        )
        .otherwise(pl.col("region"))
        .alias("region"),
    ])

    # Partition checks
    dupes = [p for p in partition_inventory if partition_inventory.count(p) > 1]
    all_years = sorted(full["year"].drop_nulls().unique().to_list())
    missing = [
        {"year": y, "month": m}
        for y in all_years
        for m in range(1, 13)
        if (y, m) not in set(partition_inventory)
    ]
    exceptions.add("duplicate_sell_out_partitions",
        pl.DataFrame(list(dict.fromkeys(map(tuple, dupes))), schema=["year", "month"]) if dupes else pl.DataFrame()
    )
    exceptions.add("missing_sell_out_partitions", pl.DataFrame(missing) if missing else pl.DataFrame())

    # Exception tables
    exceptions.add("invalid_dates", full.filter(pl.col("created_at").is_null()))
    exceptions.add("invalid_numeric_values", full.filter(pl.col("total_value_numeric").is_null()))
    exceptions.add("unmapped_reps",
        full.filter(pl.col("area").is_null() | pl.col("area").eq(""))
    )
    exceptions.add("unmapped_categories", full.filter(pl.col("target_category").is_null()))

    return full.with_columns([
        norm_str(pl.col("area")).alias("area"),
        norm_str(pl.col("region")).alias("region"),
    ])


# ---------------------------------------------------------------------------
# Sell-in normalisation
# ---------------------------------------------------------------------------

def normalize_sell_in(
    raw_parquet_dir: Path,
    exceptions: ExceptionBundle,
) -> pl.DataFrame:
    df = read_parquet(raw_parquet_dir / "sell_in_data.parquet")
    validate_columns(df, SELL_IN_REQUIRED_COLUMNS, "sell-in data")

    mapping = read_parquet(raw_parquet_dir / "ASM_KAM_Area_Mapping.parquet")
    validate_columns(mapping, KAM_MAPPING_REQUIRED_COLUMNS, "ASM/KAM mapping")
    mapping = _rename_existing(mapping, {"Area": "area", "Region": "region", "KAM": "kam"})
    mapping = mapping.with_columns(norm_str(pl.col("kam")).alias("kam"))

    # Duplicate KAM check
    dups = mapping.filter(pl.col("kam").is_duplicated())
    exceptions.add("duplicate_kam_mapping_keys", dups)

    category_map = load_category_mapping(raw_parquet_dir)

    df = df.with_columns([
        parse_dates(pl.col("Date")).alias("Date"),
        parse_money(pl.col("Value")).alias("Value"),
        norm_str(pl.col("Category")).alias("Category"),
        norm_str(pl.col("KAM")).alias("KAM"),
    ])

    # Sell-in category mapping (uses Category + Description as sku proxy)
    desc_col = pl.col("Description") if "Description" in df.columns else pl.lit("")
    df = df.with_columns(norm_str(desc_col).alias("_description"))

    # Build lookup keys and join category map
    df = df.with_columns([
        ("category::" + pl.col("Category").str.to_lowercase()).alias("_cat_key"),
        ("sku::" + pl.col("_description").str.to_lowercase()).alias("_desc_key"),
    ])
    cat_map_slim = category_map.select(["mapping_key", "target_category"])
    df = (
        df.join(cat_map_slim.rename({"mapping_key": "_cat_key", "target_category": "_cat_from_cat"}), on="_cat_key", how="left")
        .join(cat_map_slim.rename({"mapping_key": "_desc_key", "target_category": "_cat_from_desc"}), on="_desc_key", how="left")
    )
    df = df.with_columns(
        pl.coalesce(["_cat_from_cat", "_cat_from_desc"]).alias("target_category")
    )

    # Fallback pattern matching for sell-in
    combined = (pl.col("Category").str.to_lowercase() + " " + pl.col("_description").str.to_lowercase())
    ml_value = combined.str.extract(r"(\d+(?:\.\d+)?)\s*ml", group_index=1).cast(pl.Float64, strict=False)
    is_mayo = combined.str.contains(r"mayo|mayonnaise|bama")
    # Resolve both unclassified rows (null) and rows mapped to "Mayo Dynamic" by the category table
    needs_mayo_resolve = pl.col("target_category").is_null() | pl.col("target_category").eq("Mayo Dynamic")
    df = df.with_columns(
        pl.when(needs_mayo_resolve & is_mayo & (ml_value < 20))
        .then(pl.lit("Mayo Sachet"))
        .when(needs_mayo_resolve & is_mayo)
        .then(pl.lit("Mayo Jar"))
        .when(pl.col("target_category").is_null() & combined.str.contains(r"tomato|jumbo"))
        .then(pl.lit("Tomato"))
        .when(pl.col("target_category").is_null() & combined.str.contains(r"cube"))
        .then(pl.lit("Cubes"))
        .when(pl.col("target_category").is_null() & combined.str.contains(r"spice|curry|thyme"))
        .then(pl.lit("Spices"))
        .otherwise(pl.col("target_category"))
        .alias("target_category")
    )

    # Use Area/Region columns from the data if present, otherwise fall back to KAM mapping
    mapping_dedup = mapping.unique("kam", keep="first")
    df = df.join(mapping_dedup.select(["kam", "area", "region"]), left_on="KAM", right_on="kam", how="left")

    if "Area" in df.columns:
        df = df.with_columns([
            pl.when(norm_str(pl.col("Area")).ne("") & norm_str(pl.col("Area")).is_not_null())
            .then(norm_str(pl.col("Area")).str.to_uppercase())
            .otherwise(pl.col("area"))
            .alias("area"),
        ])

    # Re-derive region from area using the canonical mapping (ASM_KAM_Area_Mapping is the
    # authority for region names). The raw Region column in sell-in data uses space-separated
    # names like "NORTH CENTRAL" while the canonical form is "NORTH-CENTRAL" — using the raw
    # column would create split groups in region-level ranking.
    _area_to_region = (
        mapping.select(["area", "region"])
        .with_columns(pl.col("area").str.strip_chars().str.to_uppercase().alias("area"))
        .unique("area", keep="first")
        .filter(pl.col("area").str.len_chars().gt(0))
    )
    df = df.with_columns(pl.col("area").str.strip_chars().str.to_uppercase().alias("area"))
    df = (
        df.join(_area_to_region.rename({"region": "_canonical_region"}), on="area", how="left")
        .with_columns(pl.coalesce([pl.col("_canonical_region"), pl.col("region")]).alias("region"))
        .drop("_canonical_region")
    )

    df = df.with_columns([
        pl.col("Date").dt.year().cast(pl.Int32).alias("year"),
        pl.col("Date").dt.month().cast(pl.Int32).alias("month"),
        norm_str(pl.col("area")).alias("area"),
        norm_str(pl.col("region")).alias("region"),
    ])

    # Rows with no area = KAM not in the mapping → out of scope, excluded from output
    exceptions.add("unmapped_kams", df.filter(pl.col("area").is_null() | pl.col("area").eq("")))
    df = df.filter(pl.col("area").is_not_null() & pl.col("area").ne(""))

    exceptions.add("sell_in_invalid_dates", df.filter(pl.col("Date").is_null()))
    exceptions.add("sell_in_invalid_numeric_values", df.filter(pl.col("Value").is_null()))

    return df.drop([c for c in df.columns if c.startswith("_")])

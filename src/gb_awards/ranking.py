from __future__ import annotations

from pathlib import Path

import polars as pl

from .config import TARGET_CATEGORIES
from .io import read_parquet
from .utils import safe_growth, weighted_avg_expr


def load_weights(raw_parquet_dir: Path) -> dict[str, float]:
    df = read_parquet(raw_parquet_dir / "Weights.parquet")
    key_col, val_col = df.columns[0], df.columns[1]
    return {
        str(row[key_col]).strip().lower(): float(row[val_col])
        for row in df.to_dicts()
        if row[key_col] is not None and row[val_col] is not None
    }


def aggregate_sell_out(normalized_sell_out: pl.DataFrame) -> pl.DataFrame:
    base = normalized_sell_out.filter(
        pl.col("target_category").is_in(TARGET_CATEGORIES)
        & pl.col("area").ne("")
        & pl.col("area").is_not_null()
        & pl.col("total_value_numeric").is_not_null()
    )
    return base.group_by(
        ["year", "month", "target_category", "field_rep_username", "area", "region"]
    ).agg([
        pl.col("total_value_numeric").sum().alias("sell_out_value"),
        pl.col("distance_score").mean().alias("distance_score"),
        pl.col("cash_recon_score").mean().alias("cash_recon_score"),
    ])


def aggregate_sell_in(normalized_sell_in: pl.DataFrame) -> pl.DataFrame:
    base = normalized_sell_in.filter(
        pl.col("target_category").is_in(TARGET_CATEGORIES)
        & pl.col("area").ne("")
        & pl.col("area").is_not_null()
        & pl.col("Value").is_not_null()
    )
    return base.group_by(
        ["year", "month", "target_category", "KAM", "area", "region"]
    ).agg(pl.col("Value").sum().alias("sell_in_value"))


def _year_compare(df: pl.DataFrame, value_column: str, grain: str) -> pl.DataFrame:
    """Pivot year→columns, producing {value_column}_2024 and {value_column}_2025."""
    grouped = (
        df.group_by([grain, "target_category", "year"])
        .agg(pl.col(value_column).sum())
    )
    pivot = grouped.pivot(
        values=value_column,
        index=[grain, "target_category"],
        on="year",
        aggregate_function="sum",
    )
    # Ensure both year columns exist
    for yr in [2024, 2025]:
        col_name = str(yr)
        if col_name not in pivot.columns:
            pivot = pivot.with_columns(pl.lit(0.0).alias(col_name))
    return pivot.rename({
        "2024": f"{value_column}_2024",
        "2025": f"{value_column}_2025",
    }).with_columns([
        pl.col(f"{value_column}_2024").fill_null(0.0),
        pl.col(f"{value_column}_2025").fill_null(0.0),
    ])


def _quality_compare(df: pl.DataFrame, grain: str) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame(schema={
            grain: pl.Utf8, "target_category": pl.Utf8,
            "distance_score": pl.Float64, "cash_recon_score": pl.Float64,
        })
    return df.group_by([grain, "target_category"]).agg([
        weighted_avg_expr("distance_score", "sell_out_value").alias("distance_score"),
        weighted_avg_expr("cash_recon_score", "sell_out_value").alias("cash_recon_score"),
    ])


def compute_awards(
    sell_out_monthly: pl.DataFrame,
    sell_in_monthly: pl.DataFrame,
    weights: dict[str, float],
    grain: str,
) -> pl.DataFrame:
    sell_in = _year_compare(sell_in_monthly, "sell_in_value", grain)
    sell_out = _year_compare(sell_out_monthly, "sell_out_value", grain)
    quality = _quality_compare(sell_out_monthly, grain)

    # Synthetic "Total" category: sum across all 5 categories per grain
    sell_in_total = (
        sell_in
        .group_by(grain)
        .agg([pl.col("sell_in_value_2024").sum(), pl.col("sell_in_value_2025").sum()])
        .with_columns(pl.lit("Total").alias("target_category"))
    )
    sell_out_total = (
        sell_out
        .group_by(grain)
        .agg([pl.col("sell_out_value_2024").sum(), pl.col("sell_out_value_2025").sum()])
        .with_columns(pl.lit("Total").alias("target_category"))
    )
    sell_in = pl.concat([sell_in, sell_in_total], how="diagonal_relaxed")
    sell_out = pl.concat([sell_out, sell_out_total], how="diagonal_relaxed")

    if not sell_out_monthly.is_empty():
        quality_total = (
            sell_out_monthly
            .group_by(grain)
            .agg([
                weighted_avg_expr("distance_score", "sell_out_value").alias("distance_score"),
                weighted_avg_expr("cash_recon_score", "sell_out_value").alias("cash_recon_score"),
            ])
            .with_columns(pl.lit("Total").alias("target_category"))
        )
        quality = pl.concat([quality, quality_total], how="diagonal_relaxed")

    awards = (
        sell_in
        .join(sell_out, on=[grain, "target_category"], how="full", coalesce=True)
        .join(quality, on=[grain, "target_category"], how="left")
        .with_columns([
            pl.col("sell_in_value_2024").fill_null(0.0),
            pl.col("sell_in_value_2025").fill_null(0.0),
            pl.col("sell_out_value_2024").fill_null(0.0),
            pl.col("sell_out_value_2025").fill_null(0.0),
        ])
    )

    awards = awards.with_columns([
        safe_growth(pl.col("sell_in_value_2025"), pl.col("sell_in_value_2024")).alias("Sell In Growth vs YA"),
        safe_growth(pl.col("sell_out_value_2025"), pl.col("sell_out_value_2024")).alias("Sell Out Growth vs YA"),
        pl.col("distance_score").alias("Distance Score"),
        pl.col("cash_recon_score").alias("Cash Reconciliation Score"),
        (pl.col("distance_score") * pl.col("cash_recon_score")).alias("Hygiene"),
    ])

    # Rank points for growth metrics only: highest = most points; null = 0 pts
    for metric, points_col in [
        ("Sell In Growth vs YA", "Sell In Growth Rank Points"),
        ("Sell Out Growth vs YA", "Sell Out Growth Rank Points"),
    ]:
        valid_count = pl.col(metric).is_not_null().sum().over("target_category")
        metric_rank = pl.col(metric).rank(method="min", descending=True).over("target_category")
        awards = awards.with_columns(
            pl.when(pl.col(metric).is_not_null())
            .then(valid_count + 1 - metric_rank)
            .otherwise(0)
            .alias(points_col)
        )

    # Total Points = (Sell In Growth Rank Points * sell_in_weight)
    #              + (Sell Out Growth Rank Points * sell_out_weight * Hygiene)
    # Hygiene = Cash Reconciliation Score * Distance Score  (acts as a quality multiplier)
    awards = awards.with_columns(
        (
            pl.col("Sell In Growth Rank Points") * weights["sell_in_growth"]
            + pl.col("Sell Out Growth Rank Points") * weights["sell_out_growth"]
            * pl.col("Hygiene").fill_null(0.0)
        ).alias("Weighted Score")
    )

    # Sort then assign rank within each category
    grain_title = grain.title()
    awards = awards.rename({
        "target_category": "Category",
        grain: grain_title,
        "sell_in_value_2024": "Sell In 2024",
        "sell_in_value_2025": "Sell In 2025",
        "sell_out_value_2024": "Sell Out 2024",
        "sell_out_value_2025": "Sell Out 2025",
    })

    awards = awards.sort(
        by=["Category", "Weighted Score", "Hygiene", "Sell In Growth vs YA", "Sell Out Growth vs YA", grain_title],
        descending=[False, True, True, True, True, False],
        nulls_last=True,
    )

    # Rank = position within each Category group (1 = best)
    awards = awards.with_row_index("_idx").with_columns(
        (pl.col("_idx") - pl.col("_idx").min().over("Category") + 1).cast(pl.Int32).alias("Rank")
    ).drop("_idx")

    # Attach Region to area-grain table
    if grain == "area" and not sell_out_monthly.is_empty():
        area_region = (
            sell_out_monthly.group_by("area")
            .agg(pl.col("region").drop_nulls().mode().first().alias("Region"))
            .rename({"area": "Area"})
        )
        awards = awards.join(area_region, on="Area", how="left")

    col_order = [
        grain_title,
        *(["Region"] if grain == "area" else []),
        "Category",
        "Sell In 2024", "Sell In 2025", "Sell In Growth vs YA",
        "Sell Out 2024", "Sell Out 2025", "Sell Out Growth vs YA",
        "Distance Score", "Cash Reconciliation Score", "Hygiene",
        "Sell In Growth Rank Points", "Sell Out Growth Rank Points",
        "Weighted Score", "Rank",
    ]
    return awards.select([c for c in col_order if c in awards.columns])

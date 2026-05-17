from __future__ import annotations

from pathlib import Path

import polars as pl

from .config import AppPaths, THRESHOLDS
from .ingest import run_ingestion
from .io import discover_files, inventory_files
from .normalize import DataValidationError, ExceptionBundle, normalize_sell_in, normalize_sell_out
from .ranking import aggregate_innovations_sell_in, aggregate_innovations_sell_out, aggregate_sell_in, aggregate_sell_out, compute_awards, load_weights
from .utils import compact_records, ensure_dir, latest_approved_run, now_run_id, write_json


def validate_weights(weights: dict[str, float]) -> None:
    required = {"sell_in_growth", "sell_out_growth"}
    missing = sorted(required - set(weights))
    if missing:
        raise DataValidationError(f"Weights.xlsx is missing KPI weights: {missing}")
    total = sum(weights[key] for key in required)
    if abs(total - 1.0) > 1e-9:
        raise DataValidationError(f"Weights must sum to 1.0, found {total}")


def _failures_from_exceptions(
    normalized_sell_out: pl.DataFrame,
    normalized_sell_in: pl.DataFrame,
    exceptions: ExceptionBundle,
) -> list[str]:
    failures: list[str] = []

    if not exceptions.get("duplicate_sell_out_partitions").is_empty():
        failures.append("Duplicate sell-out partitions detected.")
    if not exceptions.get("missing_sell_out_partitions").is_empty():
        failures.append("Expected sell-out year/month partitions are missing.")
    if not exceptions.get("duplicate_kam_mapping_keys").is_empty():
        failures.append("Duplicate KAM mapping keys detected.")

    total_sell_out = normalized_sell_out["total_value_numeric"].fill_null(0).sum()
    total_sell_in = normalized_sell_in["Value"].fill_null(0).sum()

    unmapped_reps_frame = exceptions.get("unmapped_reps")
    unmapped_sell_out = (
        unmapped_reps_frame["total_value_numeric"].fill_null(0).sum()
        if "total_value_numeric" in unmapped_reps_frame.columns else 0.0
    )
    unmapped_kams_frame = exceptions.get("unmapped_kams")
    unmapped_sell_in = (
        unmapped_kams_frame["Value"].fill_null(0).sum()
        if "Value" in unmapped_kams_frame.columns else 0.0
    )

    unmapped_cat_sell_out = (
        exceptions.get("unmapped_categories")["total_value_numeric"].fill_null(0).sum()
        if "total_value_numeric" in exceptions.get("unmapped_categories").columns else 0.0
    )
    unmapped_cat_sell_in = (
        normalized_sell_in.filter(pl.col("target_category").is_null())["Value"].fill_null(0).sum()
        if "Value" in normalized_sell_in.columns else 0.0
    )
    unmapped_category_value = unmapped_cat_sell_out + unmapped_cat_sell_in

    if total_sell_out and unmapped_sell_out / total_sell_out > THRESHOLDS["unmapped_sell_out_ratio"]:
        failures.append("Unmapped sell-out value exceeded 1% of total sell-out value.")
    # Sell-in scope is defined by ASM_KAM_Area_Mapping — rows for names not in that file
    # are expected non-KAMs and are excluded from processing, not flagged as failures.
    combined_total = total_sell_out + total_sell_in
    if combined_total and unmapped_category_value / combined_total > THRESHOLDS["unmapped_category_ratio"]:
        failures.append("Unmapped category value exceeded 0.5% of total value.")

    if not exceptions.get("invalid_dates").is_empty() or not exceptions.get("sell_in_invalid_dates").is_empty():
        failures.append("Invalid dates detected in normalized datasets.")
    if not exceptions.get("invalid_numeric_values").is_empty() or not exceptions.get("sell_in_invalid_numeric_values").is_empty():
        failures.append("Invalid numeric values detected in normalized datasets.")

    return failures


def _reconciliation_summary(normalized_sell_out: pl.DataFrame) -> pl.DataFrame:
    return (
        normalized_sell_out
        .group_by(["year", "month"])
        .agg([
            pl.col("sales_id").len().alias("raw_rows"),
            pl.col("total_value_numeric").fill_null(0).sum().alias("processed_value"),
        ])
        .sort(["year", "month"])
    )


def _write_exception_tables(run_dir: Path, exceptions: ExceptionBundle) -> pl.DataFrame:
    records: list[dict] = []
    exceptions_dir = ensure_dir(run_dir / "exceptions")
    for name, frame in exceptions.tables.items():
        if frame.is_empty():
            records.append({"exception_name": name, "row_count": 0, "value_impact": 0.0})
            continue
        frame.write_csv(exceptions_dir / f"{name}.csv")
        value_col = next((c for c in ["total_value_numeric", "Value"] if c in frame.columns), None)
        value_impact = float(frame[value_col].fill_null(0).sum()) if value_col else 0.0
        records.append({
            "exception_name": name,
            "row_count": len(frame),
            "value_impact": value_impact,
            "sample": compact_records(frame),
        })
    summary = pl.DataFrame([{k: v for k, v in r.items() if k != "sample"} for r in records]).sort("exception_name")
    summary.write_parquet(run_dir / "exception_summary.parquet")
    return summary


def _write_normalized_sell_out(normalized_sell_out: pl.DataFrame, run_dir: Path) -> None:
    base_dir = ensure_dir(run_dir / "normalized_sell_out")
    for year in sorted(normalized_sell_out["year"].drop_nulls().unique().to_list()):
        for month in sorted(
            normalized_sell_out.filter(pl.col("year") == year)["month"].drop_nulls().unique().to_list()
        ):
            partition_dir = ensure_dir(base_dir / f"year={int(year)}" / f"month={int(month):02d}")
            (
                normalized_sell_out
                .filter((pl.col("year") == year) & (pl.col("month") == month))
                .write_parquet(partition_dir / "part.parquet")
            )


def run_pipeline(root: Path | str) -> Path:
    paths = AppPaths(Path(root))
    run_dir = ensure_dir(paths.processed_dir / now_run_id())
    exceptions = ExceptionBundle()
    status = "approved"
    failures: list[str] = []

    try:
        run_ingestion(paths.raw_dir, paths.reference_dir, paths.raw_parquet_dir)

        weights = load_weights(paths.raw_parquet_dir)
        validate_weights(weights)

        normalized_sell_out = normalize_sell_out(paths.raw_parquet_dir, exceptions)
        normalized_sell_in = normalize_sell_in(paths.raw_parquet_dir, exceptions)

        sell_out_monthly = aggregate_sell_out(normalized_sell_out)
        sell_in_monthly = aggregate_sell_in(normalized_sell_in)
        innovations_sell_out = aggregate_innovations_sell_out(normalized_sell_out)
        innovations_sell_in = aggregate_innovations_sell_in(normalized_sell_in)

        _write_normalized_sell_out(normalized_sell_out, run_dir)
        normalized_sell_in.write_parquet(run_dir / "normalized_sell_in.parquet")
        sell_out_monthly.write_parquet(run_dir / "sell_out_monthly.parquet")
        sell_in_monthly.write_parquet(run_dir / "sell_in_monthly.parquet")

        area_awards = compute_awards(sell_out_monthly, sell_in_monthly, innovations_sell_out, innovations_sell_in, weights, "area")
        region_awards = compute_awards(sell_out_monthly, sell_in_monthly, innovations_sell_out, innovations_sell_in, weights, "region")
        area_awards.write_parquet(run_dir / "area_kpi_awards.parquet")
        region_awards.write_parquet(run_dir / "region_kpi_awards.parquet")

        reconciliation = _reconciliation_summary(normalized_sell_out)
        reconciliation.write_parquet(run_dir / "reconciliation_summary.parquet")

        exception_summary = _write_exception_tables(run_dir, exceptions)
        failures.extend(_failures_from_exceptions(normalized_sell_out, normalized_sell_in, exceptions))
        if failures:
            status = "rejected"

        manifest = {
            "run_id": run_dir.name,
            "status": status,
            "failures": failures,
            "source_files": inventory_files(discover_files(paths.raw_dir, "*")),
            "weights": weights,
            "row_counts": {
                "normalized_sell_out": len(normalized_sell_out),
                "normalized_sell_in": len(normalized_sell_in),
                "sell_out_monthly": len(sell_out_monthly),
                "sell_in_monthly": len(sell_in_monthly),
                "area_awards": len(area_awards),
                "region_awards": len(region_awards),
            },
            "totals": {
                "sell_out_value": float(normalized_sell_out["total_value_numeric"].fill_null(0).sum()),
                "sell_in_value": float(normalized_sell_in["Value"].fill_null(0).sum()),
            },
            "reconciliation_summary_path": str(run_dir / "reconciliation_summary.parquet"),
            "outputs": {
                "area_kpi_awards": str(run_dir / "area_kpi_awards.parquet"),
                "region_kpi_awards": str(run_dir / "region_kpi_awards.parquet"),
                "normalized_sell_in": str(run_dir / "normalized_sell_in.parquet"),
                "normalized_sell_out": str(run_dir / "normalized_sell_out"),
                "exception_summary": str(run_dir / "exception_summary.parquet"),
            },
            "exception_summary": exception_summary.to_dicts(),
        }
        write_json(run_dir / "run_manifest.json", manifest)

    except Exception as exc:
        status = "rejected"
        write_json(run_dir / "run_manifest.json", {
            "run_id": run_dir.name,
            "status": status,
            "failures": [str(exc)],
        })
        raise

    return run_dir


def resolve_run_for_app(root: Path | str, run_id: str | None = None) -> Path:
    paths = AppPaths(Path(root))
    if run_id:
        return paths.processed_dir / run_id
    latest = latest_approved_run(paths.processed_dir)
    if latest is None:
        raise FileNotFoundError("No approved ETL run found. Run the pipeline first.")
    return latest

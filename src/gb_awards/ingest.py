from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl

from .io import discover_files
from .utils import ensure_dir


def _excel_to_parquet(excel_path: Path, parquet_path: Path) -> None:
    """Read Excel with pandas (Polars has no native Excel reader), write Parquet with Polars."""
    pdf = pd.read_excel(excel_path, dtype=str)
    pl.from_pandas(pdf).write_parquet(parquet_path)


def ingest_sell_out_csvs(raw_dir: Path, raw_parquet_dir: Path) -> list[Path]:
    """Convert sales_202*_**.csv → Parquet using Polars (fast multi-threaded read)."""
    ensure_dir(raw_parquet_dir)
    converted: list[Path] = []
    for csv_path in sorted(discover_files(raw_dir, "sales_202*.csv")):
        parquet_path = raw_parquet_dir / csv_path.with_suffix(".parquet").name
        if not parquet_path.exists():
            pl.read_csv(
                csv_path,
                infer_schema_length=0,   # keep all columns as Utf8
                encoding="utf8-lossy",
                truncate_ragged_lines=True,
            ).write_parquet(parquet_path)
        converted.append(parquet_path)
    return converted


def ingest_excel_files(raw_dir: Path, raw_parquet_dir: Path) -> list[Path]:
    """Convert every *.xlsx in raw_dir → Parquet."""
    ensure_dir(raw_parquet_dir)
    converted: list[Path] = []
    for excel_path in sorted(discover_files(raw_dir, "*.xlsx")):
        parquet_path = raw_parquet_dir / excel_path.with_suffix(".parquet").name
        if not parquet_path.exists():
            _excel_to_parquet(excel_path, parquet_path)
        converted.append(parquet_path)
    return converted


def ingest_reference_csvs(reference_dir: Path, raw_parquet_dir: Path) -> list[Path]:
    """Convert reference *.csv files → Parquet."""
    ensure_dir(raw_parquet_dir)
    converted: list[Path] = []
    for csv_path in sorted(discover_files(reference_dir, "*.csv")):
        parquet_path = raw_parquet_dir / csv_path.with_suffix(".parquet").name
        if not parquet_path.exists():
            pl.read_csv(csv_path, infer_schema_length=0).write_parquet(parquet_path)
        converted.append(parquet_path)
    return converted


def ingest_raw_reference_csvs(raw_dir: Path, raw_parquet_dir: Path) -> list[Path]:
    """Convert non-sales *.csv files in raw_dir → Parquet (e.g. ASM_Rep_Mapping.csv)."""
    ensure_dir(raw_parquet_dir)
    converted: list[Path] = []
    for csv_path in sorted(discover_files(raw_dir, "*.csv")):
        if csv_path.stem.lower().startswith("sales_"):
            continue  # handled by ingest_sell_out_csvs
        parquet_path = raw_parquet_dir / csv_path.with_suffix(".parquet").name
        if not parquet_path.exists():
            pl.read_csv(csv_path, infer_schema_length=0).write_parquet(parquet_path)
        converted.append(parquet_path)
    return converted


def run_ingestion(
    raw_dir: Path,
    reference_dir: Path,
    raw_parquet_dir: Path,
) -> dict[str, object]:
    """Run full ingestion — skips files already converted."""
    sell_out = ingest_sell_out_csvs(raw_dir, raw_parquet_dir)
    excel = ingest_excel_files(raw_dir, raw_parquet_dir)
    reference = ingest_reference_csvs(reference_dir, raw_parquet_dir)
    raw_ref = ingest_raw_reference_csvs(raw_dir, raw_parquet_dir)
    return {
        "sell_out_parquet_files": [str(p) for p in sell_out],
        "excel_parquet_files": [str(p) for p in excel],
        "reference_parquet_files": [str(p) for p in reference],
        "raw_reference_parquet_files": [str(p) for p in raw_ref],
    }

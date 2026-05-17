from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl


# ---------------------------------------------------------------------------
# Scalar helpers (used in Python logic, not Polars expressions)
# ---------------------------------------------------------------------------

def slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def normalize_text(value: Any) -> str:
    """Scalar normalise for use in Python-level dict lookups and comparisons."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    if text in {"#N/A", "nan", "NaN", "None"}:
        return ""
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------
# Polars column expression helpers
# ---------------------------------------------------------------------------

def norm_str(expr: pl.Expr) -> pl.Expr:
    """Normalise a string column: strip, collapse whitespace, blank sentinel values."""
    cleaned = expr.cast(pl.Utf8).str.strip_chars().str.replace_all(r"\s+", " ")
    return (
        pl.when(expr.is_null() | cleaned.is_in(["#N/A", "nan", "NaN", "None", ""]))
        .then(pl.lit(""))
        .otherwise(cleaned)
    )


def parse_money(expr: pl.Expr) -> pl.Expr:
    """Strip currency formatting and cast to Float64; null for blank/non-numeric."""
    stripped = (
        expr.cast(pl.Utf8)
        .str.replace_all(",", "")
        .str.replace_all(r"[^\d.\-]", "")
    )
    return pl.when(stripped.is_null() | stripped.eq("")).then(None).otherwise(stripped.cast(pl.Float64, strict=False))


def parse_dates(expr: pl.Expr) -> pl.Expr:
    """Parse mixed date/datetime string formats to Datetime.

    Handles:
      - ISO  "2024-01-31 23:59:58"
      - US   "1/31/2025 23:11"  (with time, no seconds)
      - US   "1/15/2025"        (date only, some rows in same file)
    """
    return (
        expr.str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
        .fill_null(expr.str.to_datetime(format="%m/%d/%Y %H:%M", strict=False))
        .fill_null(expr.str.to_datetime(format="%m/%d/%Y", strict=False))
        .fill_null(expr.str.to_datetime(format="%Y-%m-%d", strict=False))
    )


def safe_growth(current: pl.Expr, previous: pl.Expr) -> pl.Expr:
    """YoY growth rate; null when 2024 baseline is zero or missing."""
    return pl.when(previous > 0).then((current - previous) / previous).otherwise(None)


def weighted_avg_expr(value_col: str, weight_col: str) -> pl.Expr:
    """Weighted mean expression for use inside group_by().agg()."""
    v = pl.col(value_col)
    w = pl.col(weight_col)
    valid = v.is_not_null() & w.is_not_null() & (w > 0)
    numerator = (pl.when(valid).then(v * w).otherwise(0)).sum()
    denominator = (pl.when(valid).then(w).otherwise(0)).sum()
    return pl.when(denominator > 0).then(numerator / denominator).otherwise(None)


def compact_records(df: pl.DataFrame, limit: int = 10) -> list[dict[str, Any]]:
    return df.head(limit).to_dicts()


# ---------------------------------------------------------------------------
# File / run helpers
# ---------------------------------------------------------------------------

def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def now_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def month_range(years: list[int]) -> list[tuple[int, int]]:
    return [(year, month) for year in years for month in range(1, 13)]


def latest_approved_run(processed_dir: Path) -> Path | None:
    if not processed_dir.exists():
        return None
    approved: list[Path] = []
    for run_dir in sorted(processed_dir.iterdir(), reverse=True):
        manifest = run_dir / "run_manifest.json"
        if not manifest.exists():
            continue
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        if payload.get("status") == "approved":
            approved.append(run_dir)
    return approved[0] if approved else None

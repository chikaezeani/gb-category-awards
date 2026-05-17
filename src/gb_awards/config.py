from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

TARGET_CATEGORIES = [
    "Tomato",
    "Mayo Jar",
    "Mayo Sachet",
    "Cubes",
    "Spices",
]

WEIGHT_ALIASES = {
    "sell_in_growth": "Sell In Growth",
    "sell_out_growth": "Sell Out Growth",
    "distance": "Distance Score",
    "cash_reconciliation": "Cash Reconciliation Score",
}

SELL_OUT_REQUIRED_COLUMNS = {
    "Sales ID",
    "Field Rep Username",
    "Product Category",
    "Product Brand",
    "Product Name (SKU)",
    "Total Value",
}

SELL_IN_REQUIRED_COLUMNS = {
    "Date",
    "Value",
    "Category",
    "KAM",
}

KAM_MAPPING_REQUIRED_COLUMNS = {
    "Area",
    "Region",
    "KAM",
}

THRESHOLDS = {
    "unmapped_sell_in_ratio": 0.01,
    "unmapped_sell_out_ratio": 0.01,
    "unmapped_category_ratio": 0.005,
}


@dataclass(frozen=True)
class AppPaths:
    root: Path

    @property
    def raw_dir(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def raw_parquet_dir(self) -> Path:
        return self.root / "data" / "raw_parquet"

    @property
    def reference_dir(self) -> Path:
        return self.root / "data" / "reference"

    @property
    def processed_dir(self) -> Path:
        return self.root / "data" / "processed" / "runs"

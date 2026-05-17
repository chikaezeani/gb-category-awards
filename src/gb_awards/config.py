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

# Innovation SKU patterns for sell-OUT (matched against 'sku' column, lowercase)
# PCT: Gino Peppered Chicken Tomato by 50g
# ASUN Tomato: Asun Tomato 50g
# Gino Pepper: Gino Hot Pepper 3.5g / 4g
INNOVATIONS_SELL_OUT_PATTERNS = [
    "peppered chicken tomato",
    "asun tomato",
    "gino hot pepper",
]

# Innovation SKU patterns for sell-IN (matched against 'Description' column, lowercase)
# PCT: TSM GM PEPPERED CHICKEN 50 x 50G NG 24
# ASUN Tomato: TSM ASUN FLV 50 x 50g (Sac) NG 25
# Gino Pepper: GI PEPPER PWD 4gX10X20 NG 25 / PLUS 10 PROMO 25
INNOVATIONS_SELL_IN_PATTERNS = [
    "peppered chicken",
    "tsm asun",
    "gi pepper pwd",
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

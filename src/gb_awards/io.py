from __future__ import annotations

from pathlib import Path

import polars as pl

from .utils import hash_file


def discover_files(directory: Path, pattern: str) -> list[Path]:
    return sorted(directory.glob(pattern))


def read_parquet(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def inventory_files(paths: list[Path]) -> list[dict[str, str | int]]:
    return [
        {
            "name": path.name,
            "path": str(path),
            "size_bytes": path.stat().st_size,
            "sha256": hash_file(path),
        }
        for path in paths
        if path.is_file()
    ]

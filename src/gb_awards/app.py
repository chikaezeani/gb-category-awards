from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path

import pandas as pd
import polars as pl
import streamlit as st

from .config import TARGET_CATEGORIES
from .pipeline import resolve_run_for_app


def _load_manifest(run_dir: Path) -> dict:
    return json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))


def _load_awards(run_dir: Path, grain: str) -> pd.DataFrame:
    file_name = "area_kpi_awards.parquet" if grain == "Area" else "region_kpi_awards.parquet"
    return pl.read_parquet(run_dir / file_name).to_pandas()


def _download_excel(frame: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, sheet_name="Awards")
    output.seek(0)
    return output.read()


_PASSWORD = "GBFoods2026"


def _check_password() -> bool:
    if st.session_state.get("authenticated"):
        return True
    st.set_page_config(page_title="GB 2026 Category Awards", layout="centered")
    st.title("GB 2026 Category Awards")
    pwd = st.text_input("Password", type="password")
    if st.button("Login"):
        if pwd == _PASSWORD:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    return False


def main(root: str = ".") -> None:
    if not _check_password():
        return
    st.set_page_config(page_title="GB 2026 Category Awards", layout="wide")
    st.title("GB 2026 Category Awards Ranking App")

    processed_dir = Path(root) / "data" / "processed" / "runs"
    run_ids: list[str] = []
    if processed_dir.exists():
        for path in sorted(processed_dir.iterdir(), reverse=True):
            if not path.is_dir() or not (path / "run_manifest.json").exists():
                continue
            manifest = _load_manifest(path)
            if manifest.get("status") == "approved":
                run_ids.append(path.name)
    if not run_ids:
        st.error("No approved ETL run found. Run the pipeline first.")
        return
    default_run = resolve_run_for_app(root).name
    selected_run = st.sidebar.selectbox("Approved Run", options=run_ids, index=run_ids.index(default_run) if default_run in run_ids else 0)
    run_dir = resolve_run_for_app(root, selected_run)
    manifest = _load_manifest(run_dir)
    if manifest.get("status") != "approved":
        st.error("Selected run is not approved. Choose an approved ETL run.")
        return

    category = st.sidebar.selectbox("Category", TARGET_CATEGORIES + ["Total", "Innovations"], index=0)
    grain = st.sidebar.radio("Grain", options=["Area", "Region"], index=0)
    search = st.sidebar.text_input("Search")

    awards = _load_awards(run_dir, grain)
    filtered = awards[awards["Category"] == category].copy()
    if search:
        anchor = grain
        filtered = filtered[filtered[anchor].astype(str).str.contains(search, case=False, na=False)]

    st.subheader(f"{category} Ranking by {grain}")
    display = filtered.copy()
    if category == "Innovations":
        for col in ["Sell In 2024", "Sell Out 2024"]:
            if col in display.columns:
                display[col] = ""
    for col in ["Sell In 2024", "Sell In 2025", "Sell Out 2024", "Sell Out 2025"]:
        if col in display.columns:
            display[col] = display[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) and x != "" else "")
    for col in ["Sell In Growth vs YA", "Sell Out Growth vs YA"]:
        if col in display.columns:
            display[col] = display[col].apply(lambda x: f"{x * 100:.1f}%" if pd.notna(x) else "")
    for col in ["Weighted Score"]:
        if col in display.columns:
            display[col] = display[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "")
    st.dataframe(display, use_container_width=True, hide_index=True)
    st.download_button("Export CSV", filtered.to_csv(index=False).encode("utf-8"), file_name=f"{category}_{grain.lower()}_awards.csv")
    st.download_button(
        "Export Excel",
        _download_excel(filtered),
        file_name=f"{category}_{grain.lower()}_awards.xlsx",
    )

    st.subheader("Data Quality")
    quality_cols = st.columns(4)
    quality_cols[0].metric("ETL Status", manifest.get("status", "unknown").title())
    quality_cols[1].metric("Sell Out Rows", manifest.get("row_counts", {}).get("normalized_sell_out", 0))
    quality_cols[2].metric("Sell In Rows", manifest.get("row_counts", {}).get("normalized_sell_in", 0))
    quality_cols[3].metric("Exceptions", len(manifest.get("exception_summary", [])))

    st.json(
        {
            "run_id": manifest.get("run_id"),
            "failures": manifest.get("failures", []),
            "totals": manifest.get("totals", {}),
            "source_files": manifest.get("source_files", []),
        },
        expanded=False,
    )

    if (run_dir / "exception_summary.parquet").exists():
        st.subheader("Exception Summary")
        st.dataframe(pd.read_parquet(run_dir / "exception_summary.parquet"), use_container_width=True, hide_index=True)

    if (run_dir / "reconciliation_summary.parquet").exists():
        st.subheader("Reconciliation Summary")
        st.dataframe(pd.read_parquet(run_dir / "reconciliation_summary.parquet"), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

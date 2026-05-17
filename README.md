# GB 2026 Category Awards

This project implements the StartOff plan as a repeatable ETL pipeline plus a Streamlit dashboard.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Run ETL

```bash
python3 -m gb_awards.cli etl --root .
```

Each ETL run writes versioned outputs into `data/processed/runs/YYYYMMDD_HHMMSS/`.

## Run App

```bash
streamlit run app.py
```

The dashboard reads only approved processed outputs and never touches raw CSV/XLSX files during normal use.

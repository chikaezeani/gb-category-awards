from __future__ import annotations

import argparse
from pathlib import Path

from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="GB Category Awards utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    etl_parser = subparsers.add_parser("etl", help="Run the ETL pipeline")
    etl_parser.add_argument("--root", default=".", help="Project root")

    args = parser.parse_args()
    if args.command == "etl":
        run_dir = run_pipeline(Path(args.root).resolve())
        print(run_dir)


if __name__ == "__main__":
    main()

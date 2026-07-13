"""CLI entry point.

Usage:
    python -m el.run --source ERP
    python -m el.run --source ERP --batch-value 2026Q1
    python -m el.run --source MES --tables ORDER_FACT,dim_customer
"""
from __future__ import annotations

import argparse
import logging

from el.pipeline import run_source


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract-Load MSSQL/Oracle -> ClickHouse via dlt",
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source instance name (key in config/sources.yml, e.g. ERP)",
    )
    parser.add_argument(
        "--batch-value",
        default=None,
        help="Batch value for 'batch' mode tables; if omitted, the latest value is used per table",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated subset of tables to run (default: all tables of the source)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    only = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None
    run_source(args.source, batch_value=args.batch_value, only_tables=only)


if __name__ == "__main__":
    main()

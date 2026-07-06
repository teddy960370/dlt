"""el — Extract-Load pipeline: MSSQL / Oracle -> ClickHouse via dlt.

Connection info is read from .env; the table catalog from config/sources.yml.
Entry point: ``python -m el.run --source <name> [--batch-value <v>] [--tables a,b]``.
"""

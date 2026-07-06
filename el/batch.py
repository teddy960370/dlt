"""Batch value resolution (from source) and ClickHouse pre-delete.

For 'batch' mode tables: before loading a batch, remove any existing rows of the
same batch value from the ClickHouse target, so re-running a batch replaces it.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


def fetch_latest_batch_value(engine: Engine, schema: str, table: str, column: str) -> Any:
    """Return MAX(column) from the source table — the latest batch value."""
    qualified = f"{schema}.{table}" if schema else table
    sql = text(f"SELECT MAX({column}) AS v FROM {qualified}")
    with engine.connect() as conn:
        return conn.execute(sql).scalar()


def _ch_literal(value: Any) -> str:
    """Render a Python value as a ClickHouse SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    s = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _quote_ident(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def delete_batch(pipeline, source_table: str, batch_column: str, value: Any) -> bool:
    """Delete rows of one batch from the ClickHouse target table.

    Resolves the destination's physical table/column names via dlt's naming
    convention, so it works regardless of dlt's dataset-prefix and casing rules.

    Returns True if a DELETE ran, False if the target table does not exist yet
    (first load — dlt will create it).
    """
    naming = pipeline.naming
    norm_table = naming.normalize_table_identifier(source_table)
    norm_col = naming.normalize_identifier(batch_column)

    with pipeline.sql_client() as client:
        db_name, phys_table = client.make_qualified_table_name_path(norm_table, quote=False)
        exists = client.execute_sql(
            "SELECT count() FROM system.tables "
            f"WHERE database = {_ch_literal(db_name)} AND name = {_ch_literal(phys_table)}"
        )
        if not exists or not exists[0][0]:
            return False

        qualified = client.make_qualified_table_name(norm_table)
        client.execute_sql(
            f"DELETE FROM {qualified} WHERE {_quote_ident(norm_col)} = {_ch_literal(value)}"
        )
        return True

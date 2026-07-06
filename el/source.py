"""Build a dlt resource for one source table according to its mode."""
from __future__ import annotations

from typing import Any

from sqlalchemy.engine import Engine

from dlt.sources.sql_database import sql_table

from el.settings import SourceDefinition, TableConfig


def build_resource(
    engine: Engine,
    source: SourceDefinition,
    table: TableConfig,
    batch_value: Any = None,
):
    """Return a dlt resource configured for the table's load mode.

    - full_replace: read whole table, write_disposition="replace".
    - scd2:         read whole snapshot, merge/scd2 with natural key.
    - batch:        filter source by ``batch_column == batch_value``, append.
    """
    common = dict(credentials=engine, table=table.name, schema=source.schema)

    if table.mode == "full_replace":
        return sql_table(**common, write_disposition="replace")

    if table.mode == "scd2":
        return sql_table(
            **common,
            write_disposition={"disposition": "merge", "strategy": "scd2"},
            merge_key=table.scd_natural_key,
        )

    if table.mode == "batch":
        column = table.batch_column
        value = batch_value

        def query_adapter(query, sa_table):
            return query.where(sa_table.c[column] == value)

        return sql_table(
            **common,
            write_disposition="append",
            query_adapter_callback=query_adapter,
        )

    raise ValueError(f"Unknown mode: {table.mode}")

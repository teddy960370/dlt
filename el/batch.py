"""Batch value resolution (from source) and ClickHouse pre-delete.

For 'batch' mode tables: before loading a batch, remove any existing rows of the
same batch value from the ClickHouse target, so re-running a batch replaces it.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from el.source import Node, iter_postorder


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


def delete_batch_tree(pipeline, root: Node, batch_column: str, value: Any) -> dict[str, int]:
    """Post-order delete a batch parent and all descendant child tables in ClickHouse.

    Each node's rows that belong to this batch are deleted:
      root  -> WHERE batch_column = value
      child -> WHERE child_key IN (SELECT parent_key FROM parent_ch WHERE membership(parent))

    Children are deleted before parents so ancestor rows are still present to
    identify which descendant rows to remove. A node whose ClickHouse table does
    not exist yet (first load) is skipped. Physical table/column names are resolved
    via dlt's naming convention. Returns {node.path: deleted_count}.
    """
    naming = pipeline.naming
    counts: dict[str, int] = {}

    with pipeline.sql_client() as client:

        def qualified(name: str) -> str:
            return client.make_qualified_table_name(naming.normalize_table_identifier(name))

        def col(name: str) -> str:
            return _quote_ident(naming.normalize_identifier(name))

        def table_exists(name: str) -> bool:
            db, phys = client.make_qualified_table_name_path(
                naming.normalize_table_identifier(name), quote=False
            )
            r = client.execute_sql(
                "SELECT count() FROM system.tables "
                f"WHERE database = {_ch_literal(db)} AND name = {_ch_literal(phys)}"
            )
            return bool(r and r[0][0])

        def membership_ch(node: Node) -> str:
            if node.parent is None:
                return f"{col(batch_column)} = {_ch_literal(value)}"
            return (
                f"{col(node.child_key)} IN "
                f"(SELECT {col(node.parent_key)} FROM {qualified(node.parent.table_name)} "
                f"WHERE {membership_ch(node.parent)})"
            )

        for node in iter_postorder(root):
            if not table_exists(node.table_name):
                counts[node.path] = 0
                continue
            where = membership_ch(node)
            cnt = client.execute_sql(f"SELECT count() FROM {qualified(node.table_name)} WHERE {where}")
            n = int(cnt[0][0]) if cnt and cnt[0] else 0
            if n:
                client.execute_sql(f"DELETE FROM {qualified(node.table_name)} WHERE {where}")
            counts[node.path] = n

    return counts

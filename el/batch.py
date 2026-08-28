"""Batch value resolution (from source) and ClickHouse pre-delete.

For 'batch' mode tables: before loading a batch, remove any existing rows of the
same batch value from the ClickHouse target, so re-running a batch replaces it.

All dlt-internal name resolution is delegated to el.ch_internal (the single place
that touches dlt internals — see the banner there).
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from el.ch_internal import ch_literal, open_ch_namer
from el.source import Node, iter_postorder


def fetch_latest_batch_value(engine: Engine, schema: str, table: str, column: str) -> Any:
    """Return MAX(column) from the source table — the latest batch value."""
    qualified = f"{schema}.{table}" if schema else table
    sql = text(f"SELECT MAX({column}) AS v FROM {qualified}")
    with engine.connect() as conn:
        return conn.execute(sql).scalar()


def delete_batch_tree(pipeline, root: Node, batch_column: str, value: Any) -> dict[str, int]:
    """Post-order delete a batch parent and all descendant child tables in ClickHouse.

    Each node's rows that belong to this batch are deleted:
      root  -> WHERE batch_column = value
      child -> WHERE child_key IN (SELECT parent_key FROM parent_ch WHERE membership(parent))

    Children are deleted before parents so ancestor rows are still present to
    identify which descendant rows to remove. Returns {node.path: deleted_count}.

    If a node's physical table does not exist yet, there is nothing to pre-delete
    and we skip it (count 0). Physical absence is a normal, expected state here — a
    genuine first load, or a table that has always had 0 rows for the chosen batch
    (dlt does not create empty tables) — so it is NOT treated as an error. The
    pre-delete uses dlt's own naming functions (see el/ch_internal.py), so the
    delete target and dlt's write target stay aligned by construction.
    """
    counts: dict[str, int] = {}

    with open_ch_namer(pipeline) as namer:

        def membership_ch(node: Node) -> str:
            if node.parent is None:
                return f"{namer.column(batch_column)} = {ch_literal(value)}"
            return (
                f"{namer.column(node.child_key)} IN "
                f"(SELECT {namer.column(node.parent_key)} FROM {namer.qualified(node.parent.table_name)} "
                f"WHERE {membership_ch(node.parent)})"
            )

        for node in iter_postorder(root):
            if not namer.exists(node.table_name):
                # No physical table -> nothing to delete (first load, or 0-row table).
                counts[node.path] = 0
                continue

            where = membership_ch(node)
            n = namer.count(node.table_name, where)
            if n:
                namer.delete(node.table_name, where)
            counts[node.path] = n

    return counts

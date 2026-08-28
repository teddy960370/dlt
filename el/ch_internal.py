"""
================================================================================
  DLT-INTERNAL API BOUNDARY  --  RE-CHECK ON EVERY dlt UPGRADE
================================================================================
This module is the SINGLE place that reaches into dlt's non-public internals.
It exists so our manual batch pre-delete targets EXACTLY the same physical
ClickHouse tables/columns that dlt writes into.

Verified against: dlt == 1.28.1  (pinned in requirements.txt)

dlt internals used here (NOT part of dlt's stable public API):
  - pipeline.naming.normalize_table_identifier / normalize_identifier
  - pipeline.sql_client()                      (semi-public: documented for querying)
  - client.make_qualified_table_name()         (physical, quoted "<db>.<prefix><table>")
  - client.make_qualified_table_name_path()    (physical, unquoted [db, table])
  - client.execute_sql()
  - pipeline.last_trace.last_extract_info / last_normalize_info  (row-count metrics)

WHY we need them: dlt's ClickHouse destination stores tables as
"<database>.<dataset-prefix><table>" using its own naming convention + casing.
To DELETE the right rows before an append we must reproduce that exact physical
name. We deliberately call dlt's OWN naming functions (not a re-implementation)
so that write and delete move together if dlt changes them.

FAILURE MODES on upgrade:
  * A method below is renamed / changes signature  -> LOUD AttributeError/TypeError.
  * Naming *behaviour* changes                     -> write & delete change together
                                                      (still aligned), because both go
                                                      through these same functions.

Note: the pre-delete treats a physically-missing table as "nothing to delete"
(a genuine first load, or a table that has always had 0 rows for the batch — dlt
does not create empty tables), NOT as an error. See el/batch.py.

ON UPGRADE: if anything here breaks, fix it HERE and nowhere else. To sanity-check
naming alignment, do a real load and confirm the tables appear in system.tables
under the names these helpers compute.
================================================================================
"""
from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Iterator, Tuple


# --- ClickHouse SQL literal / identifier formatting (our own, CH-specific) --- #

def ch_literal(value: Any) -> str:
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


# --- Physical name resolver bound to an open dlt sql_client (DLT-INTERNAL) --- #

class ChTableNamer:
    """Resolves the physical ClickHouse names dlt uses, and runs count/delete.

    Bound to an open dlt sql_client. Every method here touches dlt internals;
    keep them confined to this class.
    """

    def __init__(self, client, naming):
        self._client = client
        self._naming = naming

    def qualified(self, source_table: str) -> str:
        """Quoted physical name, e.g. `raw_test`.`orders`, as dlt writes it."""
        return self._client.make_qualified_table_name(
            self._naming.normalize_table_identifier(source_table)
        )

    def physical_path(self, source_table: str) -> Tuple[str, str]:
        """Unquoted (database, physical_table) for system.tables lookups."""
        return self._client.make_qualified_table_name_path(
            self._naming.normalize_table_identifier(source_table), quote=False
        )

    def column(self, name: str) -> str:
        """Quoted physical column name as dlt writes it."""
        return _quote_ident(self._naming.normalize_identifier(name))

    def exists(self, source_table: str) -> bool:
        db, phys = self.physical_path(source_table)
        rows = self._client.execute_sql(
            "SELECT count() FROM system.tables "
            f"WHERE database = {ch_literal(db)} AND name = {ch_literal(phys)}"
        )
        return bool(rows and rows[0][0])

    def count(self, source_table: str, where: str) -> int:
        rows = self._client.execute_sql(
            f"SELECT count() FROM {self.qualified(source_table)} WHERE {where}"
        )
        return int(rows[0][0]) if rows and rows[0] else 0

    def delete(self, source_table: str, where: str) -> None:
        self._client.execute_sql(
            f"DELETE FROM {self.qualified(source_table)} WHERE {where}"
        )


@contextmanager
def open_ch_namer(pipeline) -> Iterator[ChTableNamer]:
    """Open a dlt sql_client and yield a ChTableNamer bound to it."""
    naming = pipeline.naming
    with pipeline.sql_client() as client:
        yield ChTableNamer(client, naming)


# --- Row-count metrics from the last run's trace (DLT-INTERNAL) ------------- #

def normalized_table_name(pipeline, source_table: str) -> str:
    return pipeline.naming.normalize_table_identifier(source_table)


def _items_count(step_info, normalized_table: str) -> int:
    metrics = getattr(step_info, "metrics", None)
    if not metrics:
        return 0
    total = 0
    for metrics_list in metrics.values():
        for m in metrics_list:
            wm = m["table_metrics"].get(normalized_table)
            if wm is not None:
                total += wm.items_count
    return total


def row_counts_for(pipeline, normalized_table: str) -> Tuple[int, int]:
    """(selected, inserted) row counts for a table from the last run's trace."""
    trace = getattr(pipeline, "last_trace", None)
    if not trace:
        return 0, 0
    selected = _items_count(trace.last_extract_info, normalized_table)
    inserted = _items_count(trace.last_normalize_info, normalized_table)
    return selected, inserted

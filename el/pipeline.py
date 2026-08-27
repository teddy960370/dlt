"""Orchestrate Extract-Load for one source instance into ClickHouse."""
from __future__ import annotations

import logging
from collections import namedtuple
from datetime import datetime, timezone
from typing import Optional, Sequence

import dlt

from el.batch import delete_batch_tree, fetch_latest_batch_value
from el.ch_internal import normalized_table_name, row_counts_for, sync_schema_from_destination
from el.connections import (
    build_clickhouse_destination,
    build_source_engine,
    ensure_clickhouse_database,
)
from el.results import RunResult, TableResult
from el.settings import load_catalog, load_clickhouse_config, load_source_connection
from el.source import build_batch_tree, build_child_resource, build_resource, iter_preorder

log = logging.getLogger("el")


_LoadItem = namedtuple("_LoadItem", "label mode table_name batch_value resource deleted")


def _collect_batch_items(pipeline, engine, source, table, value) -> list:
    """Pre-delete (post-order) the batch tree, then return one _LoadItem per node.

    Resources are lazy (reflection happens at extract time), so they are collected
    here and loaded together in a single pipeline.run() by the caller.
    """
    root = build_batch_tree(table)
    del_counts = delete_batch_tree(pipeline, root, table.batch_column, value)
    items: list = []
    for node in iter_preorder(root):
        if node.parent is None:
            resource = build_resource(engine, source, table, value)
            mode = "batch"
        else:
            resource = build_child_resource(engine, source, node, table.batch_column, value)
            mode = "batch-child"
        items.append(
            _LoadItem(node.path, mode, node.table_name, str(value), resource, del_counts.get(node.path, 0))
        )
    return items


def run_source(
    source_name: str,
    batch_value: Optional[str] = None,
    only_tables: Optional[Sequence[str]] = None,
) -> RunResult:
    """Run the EL pipeline for one named source instance from sources.yml."""
    started = datetime.now(timezone.utc)
    catalog = load_catalog()
    if source_name not in catalog:
        raise KeyError(
            f"Source '{source_name}' not found in sources.yml. "
            f"Available: {', '.join(catalog) or '(none)'}"
        )
    source = catalog[source_name]

    engine = build_source_engine(load_source_connection(source_name, source.type))

    ch_config = load_clickhouse_config()
    # Each source lands in its own ClickHouse database (schema), e.g. raw_erp.
    ensure_clickhouse_database(ch_config, source.target_schema)
    destination = build_clickhouse_destination(ch_config, database=source.target_schema)

    pipeline = dlt.pipeline(
        pipeline_name=f"el_{source_name.lower()}",
        destination=destination,
        # Empty dataset -> tables stored directly as <target_schema>.<table>, no prefix.
        dataset_name="",
    )
    # Restore dlt's schema from ClickHouse so the pre-delete drift guard can tell a
    # first load apart from a naming mismatch (see el/ch_internal.py, el/batch.py).
    sync_schema_from_destination(pipeline)
    log.info("[%s] target ClickHouse database = %s", source_name, source.target_schema)

    tables = list(source.tables)
    if only_tables:
        wanted = set(only_tables)
        tables = [t for t in tables if t.name in wanted]
        missing = wanted - {t.name for t in tables}
        if missing:
            raise KeyError(f"Tables not in source '{source_name}': {', '.join(sorted(missing))}")

    results: list[TableResult] = []
    try:
        # 1) Pre-delete all batch trees and collect all (lazy) resources to load.
        items: list = []
        for table in tables:
            if table.mode == "batch":
                value = batch_value
                if value is None:
                    value = fetch_latest_batch_value(
                        engine, source.schema, table.name, table.batch_column
                    )
                    log.info("[%s.%s] latest %s = %r", source_name, table.name, table.batch_column, value)
                if value is None:
                    log.warning("[%s.%s] no batch value found; skipping", source_name, table.name)
                    continue
                items.extend(_collect_batch_items(pipeline, engine, source, table, value))
            else:
                resource = build_resource(engine, source, table)
                items.append(_LoadItem(table.name, table.mode, table.name, None, resource, 0))

        # 2) Load everything in a SINGLE pipeline.run() — one extract->normalize->load
        #    cycle instead of one per table, to avoid repeated file-move contention
        #    that triggers Windows WinError 32 during normalize.
        if items:
            pipeline.run([it.resource for it in items])

        # 3) Record per-table counts from the run's trace.
        for it in items:
            selected, inserted = row_counts_for(pipeline, normalized_table_name(pipeline, it.table_name))
            log.info(
                "[%s.%s] mode=%s | select=%d delete=%d insert=%d",
                source_name, it.label, it.mode, selected, it.deleted, inserted,
            )
            results.append(
                TableResult(
                    path=it.label, mode=it.mode, select=selected, delete=it.deleted,
                    insert=inserted, batch_value=it.batch_value,
                )
            )
    finally:
        engine.dispose()

    finished = datetime.now(timezone.utc)
    return RunResult(
        source=source_name,
        status="success",
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=(finished - started).total_seconds(),
        tables=results,
    )

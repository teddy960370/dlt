"""Orchestrate Extract-Load for one source instance into ClickHouse."""
from __future__ import annotations

import logging
from typing import Optional, Sequence

import dlt

from el.batch import delete_batch_tree, fetch_latest_batch_value
from el.connections import (
    build_clickhouse_destination,
    build_source_engine,
    ensure_clickhouse_database,
)
from el.settings import load_catalog, load_clickhouse_config, load_source_connection
from el.source import build_batch_tree, build_child_resource, build_resource, iter_preorder

log = logging.getLogger("el")


def _items_count(step_info, normalized_table: str) -> int:
    """Sum items_count for a normalized table from an extract/normalize StepInfo."""
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


def _log_load(pipeline, source_name, label, mode, deleted, node_table_name) -> None:
    """Log select/delete/insert row counts for one loaded node."""
    norm = pipeline.naming.normalize_table_identifier(node_table_name)
    trace = pipeline.last_trace
    selected = _items_count(trace.last_extract_info, norm) if trace else 0
    inserted = _items_count(trace.last_normalize_info, norm) if trace else 0
    log.info(
        "[%s.%s] mode=%s | select=%d delete=%d insert=%d",
        source_name, label, mode, selected, deleted, inserted,
    )


def _run_batch_tree(pipeline, engine, source, source_name, table, value) -> None:
    """Pre-delete (post-order) then load (pre-order) a batch parent and its children."""
    root = build_batch_tree(table)
    del_counts = delete_batch_tree(pipeline, root, table.batch_column, value)
    for node in iter_preorder(root):
        if node.parent is None:
            resource = build_resource(engine, source, table, value)
            mode = "batch"
        else:
            resource = build_child_resource(engine, source, node, table.batch_column, value)
            mode = "batch-child"
        pipeline.run(resource)
        _log_load(pipeline, source_name, node.path, mode, del_counts.get(node.path, 0), node.table_name)


def run_source(
    source_name: str,
    batch_value: Optional[str] = None,
    only_tables: Optional[Sequence[str]] = None,
) -> None:
    """Run the EL pipeline for one named source instance from sources.yml."""
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
    log.info("[%s] target ClickHouse database = %s", source_name, source.target_schema)

    tables = list(source.tables)
    if only_tables:
        wanted = set(only_tables)
        tables = [t for t in tables if t.name in wanted]
        missing = wanted - {t.name for t in tables}
        if missing:
            raise KeyError(f"Tables not in source '{source_name}': {', '.join(sorted(missing))}")

    try:
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
                _run_batch_tree(pipeline, engine, source, source_name, table, value)
            else:
                resource = build_resource(engine, source, table)
                pipeline.run(resource)
                _log_load(pipeline, source_name, table.name, table.mode, 0, table.name)
    finally:
        engine.dispose()

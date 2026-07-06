"""Orchestrate Extract-Load for one source instance into ClickHouse."""
from __future__ import annotations

import logging
from typing import Optional, Sequence

import dlt

from el.batch import delete_batch, fetch_latest_batch_value
from el.connections import build_clickhouse_destination, build_source_engine
from el.settings import load_catalog, load_clickhouse_config, load_source_connection
from el.source import build_resource

log = logging.getLogger("el")


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
    destination = build_clickhouse_destination(load_clickhouse_config())

    pipeline = dlt.pipeline(
        pipeline_name=f"el_{source_name.lower()}",
        destination=destination,
        dataset_name=source_name.lower(),
    )

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
                existed = delete_batch(pipeline, table.name, table.batch_column, value)
                log.info(
                    "[%s.%s] pre-delete %s=%r (target existed=%s)",
                    source_name, table.name, table.batch_column, value, existed,
                )
                resource = build_resource(engine, source, table, value)
            else:
                resource = build_resource(engine, source, table)

            info = pipeline.run(resource)
            log.info("[%s.%s] mode=%s loaded: %s", source_name, table.name, table.mode, info)
    finally:
        engine.dispose()

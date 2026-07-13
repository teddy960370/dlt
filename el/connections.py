"""Build source database engines and the ClickHouse dlt destination."""
from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

from dlt.destinations import clickhouse

from el.settings import ClickHouseConfig, SourceConnection

_oracle_thick_ready = False


def _ensure_oracle_thick_mode() -> None:
    """Enable python-oracledb thick mode (once per process).

    Required for old Oracle servers (e.g. 11g) that thin mode does not support.
    Loads the Oracle Client library from ORACLE_CLIENT_LIB_DIR if set, otherwise
    relies on the OS library path (PATH). Must be a 64-bit client to match Python.
    """
    global _oracle_thick_ready
    if _oracle_thick_ready:
        return
    import oracledb

    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR") or None
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
    except oracledb.Error as e:
        # Ignore "already initialized"; re-raise anything else (e.g. lib not found).
        if "already been initialized" not in str(e):
            raise
    _oracle_thick_ready = True


def build_source_engine(cfg: SourceConnection) -> Engine:
    """Create a SQLAlchemy engine for a source instance based on its type."""
    if cfg.type == "mssql":
        url = URL.create(
            "mssql+pyodbc",
            username=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            query={"driver": cfg.odbc_driver, "TrustServerCertificate": "yes"},
        )
        return create_engine(url)

    if cfg.type == "oracle":
        _ensure_oracle_thick_mode()
        url = URL.create(
            "oracle+oracledb",
            username=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port=cfg.port,
            query={"service_name": cfg.service_name},
        )
        return create_engine(url)

    raise ValueError(f"Unsupported source type: {cfg.type}")


def build_clickhouse_destination(cfg: ClickHouseConfig, database: Optional[str] = None):
    """Create a dlt ClickHouse destination.

    ``database`` overrides the connect database so each source can land in its own
    ClickHouse database (schema), e.g. "raw_erp". Combined with an empty
    ``dataset_name`` on the pipeline, tables are stored as ``<database>.<table>``.
    """
    return clickhouse(
        credentials={
            "host": cfg.host,
            "port": cfg.port,
            "http_port": cfg.http_port,
            "database": database or cfg.database,
            "username": cfg.username,
            "password": cfg.password,
            "secure": 1 if cfg.secure else 0,
        }
    )


def ensure_clickhouse_database(cfg: ClickHouseConfig, database: str) -> None:
    """Create the target ClickHouse database (schema) if it does not exist.

    Connects to the bootstrap database (CLICKHOUSE_DATABASE, usually 'default')
    to issue CREATE DATABASE IF NOT EXISTS for the per-source target schema.
    """
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.http_port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
        secure=cfg.secure,
    )
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    finally:
        client.close()

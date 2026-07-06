"""Build source database engines and the ClickHouse dlt destination."""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

from dlt.destinations import clickhouse

from el.settings import ClickHouseConfig, SourceConnection


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


def build_clickhouse_destination(cfg: ClickHouseConfig):
    """Create a dlt ClickHouse destination from config."""
    return clickhouse(
        credentials={
            "host": cfg.host,
            "port": cfg.port,
            "http_port": cfg.http_port,
            "database": cfg.database,
            "username": cfg.username,
            "password": cfg.password,
            "secure": 1 if cfg.secure else 0,
        }
    )

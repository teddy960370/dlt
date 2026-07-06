"""Configuration loading.

Single place that reads environment (.env) and the table catalog (sources.yml),
exposing typed config objects to the rest of the package.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCES_YAML = PROJECT_ROOT / "config" / "sources.yml"

# Load .env once at import time; real env vars still take precedence.
load_dotenv(PROJECT_ROOT / ".env", override=False)

VALID_MODES = {"batch", "full_replace", "scd2"}
VALID_TYPES = {"mssql", "oracle"}


@dataclass
class TableConfig:
    name: str
    mode: str
    batch_column: Optional[str] = None
    scd_natural_key: Optional[Union[str, list[str]]] = None


@dataclass
class SourceDefinition:
    name: str
    type: str
    schema: str
    tables: list[TableConfig]


@dataclass
class SourceConnection:
    name: str
    type: str
    host: str
    port: int
    user: str
    password: str
    database: Optional[str] = None       # mssql
    service_name: Optional[str] = None   # oracle
    odbc_driver: Optional[str] = None    # mssql


@dataclass
class ClickHouseConfig:
    host: str
    http_port: int
    port: int
    database: str
    username: str
    password: str
    secure: bool


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return val


def load_catalog(path: Path = SOURCES_YAML) -> dict[str, SourceDefinition]:
    """Parse config/sources.yml into validated SourceDefinition objects."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    catalog: dict[str, SourceDefinition] = {}
    for name, body in (raw.get("sources") or {}).items():
        stype = str(body["type"]).lower()
        if stype not in VALID_TYPES:
            raise ValueError(f"Source '{name}': unsupported type '{stype}' (expected one of {VALID_TYPES})")

        tables: list[TableConfig] = []
        for t in body.get("tables", []) or []:
            mode = t["mode"]
            if mode not in VALID_MODES:
                raise ValueError(
                    f"Source '{name}', table '{t.get('name')}': invalid mode '{mode}' "
                    f"(expected one of {VALID_MODES})"
                )
            tc = TableConfig(
                name=t["name"],
                mode=mode,
                batch_column=t.get("batch_column"),
                scd_natural_key=t.get("scd_natural_key"),
            )
            if mode == "batch" and not tc.batch_column:
                raise ValueError(f"Source '{name}', table '{tc.name}': batch mode requires 'batch_column'")
            if mode == "scd2" and not tc.scd_natural_key:
                raise ValueError(f"Source '{name}', table '{tc.name}': scd2 mode requires 'scd_natural_key'")
            tables.append(tc)

        catalog[name] = SourceDefinition(
            name=name,
            type=stype,
            schema=body["schema"],
            tables=tables,
        )
    return catalog


def load_source_connection(name: str, source_type: str) -> SourceConnection:
    """Read a source instance's connection info from env, prefixed by its name."""
    p = name.upper()
    if source_type == "mssql":
        return SourceConnection(
            name=name,
            type="mssql",
            host=_require(f"{p}_HOST"),
            port=int(os.getenv(f"{p}_PORT", "1433")),
            database=_require(f"{p}_DATABASE"),
            user=_require(f"{p}_USER"),
            password=_require(f"{p}_PASSWORD"),
            odbc_driver=os.getenv(f"{p}_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
        )
    if source_type == "oracle":
        return SourceConnection(
            name=name,
            type="oracle",
            host=_require(f"{p}_HOST"),
            port=int(os.getenv(f"{p}_PORT", "1521")),
            service_name=_require(f"{p}_SERVICE_NAME"),
            user=_require(f"{p}_USER"),
            password=_require(f"{p}_PASSWORD"),
        )
    raise ValueError(f"Unsupported source type: {source_type}")


def load_clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=_require("CLICKHOUSE_HOST"),
        http_port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() in ("1", "true", "yes"),
    )

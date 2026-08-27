"""Structured results returned by a pipeline run (consumed by CLI and the API)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TableResult:
    path: str                       # node path, e.g. "Orders > SaleItems"
    mode: str                       # batch | batch-child | full_replace | scd2
    select: int                     # rows read from source
    delete: int                     # rows pre-deleted in ClickHouse (batch only)
    insert: int                     # rows loaded into ClickHouse
    batch_value: Optional[str] = None


@dataclass
class RunResult:
    source: str
    status: str                     # "success" | "failed"
    started_at: str                 # ISO-8601 UTC
    finished_at: str
    duration_seconds: float
    tables: list[TableResult] = field(default_factory=list)

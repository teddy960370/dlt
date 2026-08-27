"""Per-source concurrency guard around el.run_source.

A run for a given source is exclusive: while one is in progress, another request
for the same source is rejected (SourceBusyError -> HTTP 409). Different sources
run concurrently. Requires a single API worker process (the registry is in-memory).
"""
from __future__ import annotations

import threading
from typing import Optional, Sequence

from el.pipeline import run_source
from el.results import RunResult


class SourceBusyError(Exception):
    """Raised when a source already has a run in progress."""


_lock = threading.Lock()
_running: set[str] = set()


def _acquire(source: str) -> bool:
    with _lock:
        if source in _running:
            return False
        _running.add(source)
        return True


def _release(source: str) -> None:
    with _lock:
        _running.discard(source)


def run(source: str, batch_value: Optional[str] = None, tables: Optional[Sequence[str]] = None) -> RunResult:
    """Run a source exclusively; raise SourceBusyError if it is already running."""
    if not _acquire(source):
        raise SourceBusyError(source)
    try:
        return run_source(source, batch_value=batch_value, only_tables=tables)
    finally:
        _release(source)

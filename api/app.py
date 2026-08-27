"""FastAPI application exposing el pipeline control to an Orchestrator.

Run: uvicorn api.app:app --host $API_HOST --port $API_PORT --workers 1
(single worker is required — the per-source concurrency guard is in-memory).
"""
from __future__ import annotations

import logging
from dataclasses import asdict
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException

from api import runner
from api.auth import require_auth
from api.models import HealthResponse, RunRequest, RunResult, SourceInfo
from el.settings import load_catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = FastAPI(
    title="dlt EL Web API",
    description="Trigger and inspect el (MSSQL/Oracle -> ClickHouse) pipeline runs.",
    version="1.0.0",
)


def _catalog():
    """Load the source catalog, mapping config errors to HTTP 400."""
    try:
        return load_catalog()
    except ValueError as e:
        raise HTTPException(400, f"invalid sources.yml: {e}")


def _source_info(source_def) -> SourceInfo:
    return SourceInfo.model_validate(asdict(source_def))


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/sources", response_model=List[SourceInfo], dependencies=[Depends(require_auth)])
def list_sources() -> List[SourceInfo]:
    return [_source_info(s) for s in _catalog().values()]


@app.get("/sources/{name}", response_model=SourceInfo, dependencies=[Depends(require_auth)])
def get_source(name: str) -> SourceInfo:
    catalog = _catalog()
    if name not in catalog:
        raise HTTPException(404, f"source '{name}' not found")
    return _source_info(catalog[name])


@app.post("/sources/{name}/run", response_model=RunResult, dependencies=[Depends(require_auth)])
def run_source_endpoint(name: str, body: Optional[RunRequest] = None) -> RunResult:
    body = body or RunRequest()
    catalog = _catalog()
    if name not in catalog:
        raise HTTPException(404, f"source '{name}' not found")

    try:
        result = runner.run(name, batch_value=body.batch_value, tables=body.tables)
    except runner.SourceBusyError:
        raise HTTPException(409, f"source '{name}' is already running")
    except KeyError as e:
        # e.g. unknown table names passed in `tables`
        raise HTTPException(400, str(e).strip('"'))
    except Exception as e:
        raise HTTPException(500, f"run failed: {e}")

    return RunResult.model_validate(asdict(result))

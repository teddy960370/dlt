"""Pydantic (v2) request/response models for the API."""
from __future__ import annotations

from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


# --- requests --- #

class RunRequest(BaseModel):
    batch_value: Optional[str] = None
    tables: Optional[List[str]] = None


# --- responses: catalog discovery --- #

class ChildInfo(BaseModel):
    name: str
    child_key: str
    parent_key: str
    children: List["ChildInfo"] = []


class TableInfo(BaseModel):
    name: str
    mode: str
    batch_column: Optional[str] = None
    scd_natural_key: Optional[Union[str, List[str]]] = None
    children: List[ChildInfo] = []


class SourceInfo(BaseModel):
    # allow constructing by field name while serializing "schema_" as "schema"
    model_config = ConfigDict(populate_by_name=True)

    name: str
    type: str
    schema_: str = Field(alias="schema", serialization_alias="schema")
    target_schema: str
    tables: List[TableInfo] = []


# --- responses: run result --- #

class TableResult(BaseModel):
    path: str
    mode: str
    select: int
    delete: int
    insert: int
    batch_value: Optional[str] = None


class RunResult(BaseModel):
    source: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    tables: List[TableResult] = []


class HealthResponse(BaseModel):
    status: str


ChildInfo.model_rebuild()

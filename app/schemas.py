"""Pydantic request/response schemas for the JSON API."""
from __future__ import annotations

from pydantic import BaseModel, Field


class NodeSpec(BaseModel):
    node_key: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=200)
    role: str = Field(pattern="^(primary|candidate)$")


class IncidentCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    primary_timeline: int = Field(ge=1, le=0xFFFFFFFF)
    commit_lsn: str
    nodes: list[NodeSpec] = Field(min_length=1)


class ObservationSpec(BaseModel):
    node_key: str
    timeline: int = Field(ge=1, le=0xFFFFFFFF)
    replay_lsn: str


class ObservationBatch(BaseModel):
    expected_version: int = Field(ge=1)
    observations: list[ObservationSpec] = Field(min_length=1)
    client_request_id: str | None = Field(default=None, max_length=100)


class ArchiveSpec(BaseModel):
    timeline: int = Field(ge=1, le=0xFFFFFFFF)
    start_segment: str
    end_segment: str


class ArchiveBatch(BaseModel):
    expected_version: int = Field(ge=1)
    archives: list[ArchiveSpec] = Field(min_length=1)
    client_request_id: str | None = Field(default=None, max_length=100)


class CheckRequest(BaseModel):
    expected_version: int = Field(ge=1)
    primary_timeline: int | None = None
    commit_lsn: str | None = None
    client_request_id: str | None = Field(default=None, max_length=100)


class PromoteRequest(BaseModel):
    expected_version: int = Field(ge=1)
    # Idempotency key: replaying the same request_id returns the first result.
    request_id: str = Field(min_length=1, max_length=100)
    node_key: str | None = None  # optional explicit choice; must be eligible
    primary_timeline: int | None = None
    commit_lsn: str | None = None


class RecoveryReport(BaseModel):
    node_key: str
    # 0 means the initial primary has never been through a promotion.
    generation: int = Field(ge=0)
    reported_primary_timeline: int = Field(ge=1)
    reported_replay_lsn: str
    request_id: str = Field(min_length=1, max_length=100)

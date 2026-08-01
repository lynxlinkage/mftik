"""HTTP response / request models for the MFT API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DomainStats(BaseModel):
    domain: str
    live: int = 0
    done: int = 0
    healthy: bool | None = None


class StatsResponse(BaseModel):
    domains: list[DomainStats]


class DeployBody(BaseModel):
    """Deployment spec for ``POST /sts/{strategy_id}`` (like deployment.yml)."""

    td: list[int] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    st_paras: dict[str, Any] = Field(default_factory=dict)
    created_by: int | None = None
    timeout: float = 30.0


class TdAttachOut(BaseModel):
    api_id: int
    refcount: int


class DeployResponse(BaseModel):
    session_id: str
    strategy: str
    td: list[TdAttachOut] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    status: str = "live"


class SessionOut(BaseModel):
    session_id: str
    domain: str
    created_by: int
    created_at: float
    finished_at: float | None = None
    status: str
    api_id: int | None = None
    sts_session_id: str | None = None
    strategy: str | None = None
    paused: bool | None = None
    venue: str | None = None


class SessionListResponse(BaseModel):
    sessions: list[SessionOut] = Field(default_factory=list)


class StsControlResponse(BaseModel):
    session_id: str
    status: str
    paused: bool = False
    strategy: str | None = None


class StrategiesResponse(BaseModel):
    strategies: list[str]


class ApiOut(BaseModel):
    id: int
    venue: str
    api_key: str
    type: str


class ApiListResponse(BaseModel):
    apis: list[ApiOut] = Field(default_factory=list)


class AuditOut(BaseModel):
    id: int
    user_id: int
    operation: str
    result: str
    created_at: float | None = None


class AuditListResponse(BaseModel):
    audits: list[AuditOut] = Field(default_factory=list)


class ErrorBody(BaseModel):
    code: str
    message: str

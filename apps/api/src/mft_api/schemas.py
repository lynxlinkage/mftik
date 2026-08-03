"""HTTP response / request models for the MFT API."""

from __future__ import annotations

from typing import Any

from mft.protocol import SymbolInfo
from pydantic import BaseModel, Field


class DomainStats(BaseModel):
    domain: str
    live: int = 0
    done: int = 0
    healthy: bool | None = None


class StatsResponse(BaseModel):
    domains: list[DomainStats]


class StrategyDeployBody(BaseModel):
    """Deploy from a strategy.yml document (live editor / API)."""

    yaml: str = Field(..., description="strategy.yml contents")
    created_by: int | None = None
    timeout: float = 30.0


class TdAttachOut(BaseModel):
    api_id: int
    refcount: int


class DeployResponse(BaseModel):
    id: int
    session_id: str
    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    td: list[TdAttachOut] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    status: str = "live"


class StrategyOut(BaseModel):
    """Deployed strategy.yml row joined to sts_sessions for status."""

    id: int
    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    created_by: int
    created_at: float
    sts_session: str
    status: str | None = None
    paused: bool | None = None


class StrategyListResponse(BaseModel):
    strategies: list[StrategyOut] = Field(default_factory=list)


class StrategyTypesResponse(BaseModel):
    """Registered STS strategy class names for strategy.yml ``sts.type``."""

    types: list[str] = Field(default_factory=list)


class SessionOut(BaseModel):
    session_id: str
    domain: str
    created_by: int
    created_at: float
    finished_at: float | None = None
    status: str
    api_id: int | None = None
    api_name: str | None = None
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


class StrategyYamlResponse(BaseModel):
    """A past deploy rebuilt as strategy.yml.

    ``yaml`` is reconstructed from the stored spec, not the original document:
    comments, key order and formatting are gone. ``unresolved_td`` lists api
    ids whose account name could not be recovered — their ``td`` entries are
    placeholders that will not redeploy.
    """

    id: int
    sts_session: str
    yaml: str
    unresolved_td: list[int] = Field(default_factory=list)


class VenueOut(BaseModel):
    """A venue credentials can be registered against."""

    name: str
    label: str
    api_types: list[str] = Field(default_factory=list)
    requires_passphrase: bool = False
    simulated: bool = False
    symbol_example: str = ""


class VenueListResponse(BaseModel):
    venues: list[VenueOut] = Field(default_factory=list)


class SymVenueListResponse(BaseModel):
    """Venues the symbol plane tracks, and how many instruments it holds."""

    venues: list[str] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class SymSymbolListResponse(BaseModel):
    symbols: list[SymbolInfo] = Field(default_factory=list)


class ApiCreateBody(BaseModel):
    """Create a venue API credential and its 1-1 trading account."""

    name: str = Field(..., min_length=1, max_length=128)
    venue: str = Field(..., min_length=1, max_length=64)
    api_key: str = Field(..., min_length=1, max_length=256)
    api_secret: str = Field(..., min_length=1)
    type: str = "HMAC"
    passphrase: str | None = None
    created_by: int | None = None


class ApiOut(BaseModel):
    id: int
    account_id: int
    name: str
    venue: str
    api_key: str
    type: str
    created_at: float
    created_by: int


class ApiListResponse(BaseModel):
    apis: list[ApiOut] = Field(default_factory=list)


class ApiDeleteResponse(BaseModel):
    id: int
    account_id: int
    deleted: bool = True


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

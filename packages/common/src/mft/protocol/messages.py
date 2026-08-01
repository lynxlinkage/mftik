"""Concrete payload models carried inside ``Envelope[T]``."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mft.protocol.envelope import Envelope


class Heartbeat(BaseModel):
    """Periodic liveness signal from a domain process."""

    model_config = ConfigDict(frozen=True)

    status: str = "ok"


class Log(BaseModel):
    """Session log line streamed to the UI over WebSocket."""

    model_config = ConfigDict(frozen=True, extra="allow")

    level: str = "info"
    message: str


class HealthCheck(BaseModel):
    """API → domain health probe request payload."""

    model_config = ConfigDict(frozen=True)

    note: str | None = None


class HealthStatus(BaseModel):
    """Domain → API health probe reply payload."""

    model_config = ConfigDict(frozen=True)

    status: str = "ok"
    service: str = "td"


class RpcError(BaseModel):
    """Generic RPC error reply payload."""

    model_config = ConfigDict(frozen=True)

    code: str
    message: str


class TdAttachRequest(BaseModel):
    """API → TD: attach trading api_id to an STS session."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    session_id: str
    created_by: int
    timeout: float = 30.0


class TdAttachResult(BaseModel):
    """TD → API: attach succeeded (lease heartbeat observed)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    refcount: int


# Backward-compatible aliases used by older call sites / tests.
CreateSessionRequest = TdAttachRequest
CreateSessionResult = TdAttachResult


class StsCreateSessionRequest(BaseModel):
    """API → STS: create strategy session (API-minted session_id)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    created_by: int
    strategy: str
    td: list[int] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    st_paras: dict[str, Any] = Field(default_factory=dict)


class StsCreateSessionResult(BaseModel):
    """STS → API: strategy session created."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    strategy: str
    td: list[int] = Field(default_factory=list)


class ListSessionsRequest(BaseModel):
    """API → domain: list control-plane sessions."""

    model_config = ConfigDict(frozen=True)

    domain: str | None = "td"
    status: str | None = "live"
    created_by: int | None = None


class SessionInfo(BaseModel):
    """One control-plane session row."""

    model_config = ConfigDict(frozen=True)

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


class ListSessionsResult(BaseModel):
    """Domain → API: session list."""

    model_config = ConfigDict(frozen=True)

    sessions: list[SessionInfo] = Field(default_factory=list)


class StsSessionControlRequest(BaseModel):
    """API → STS: pause / resume / stop a strategy session."""

    model_config = ConfigDict(frozen=True)

    session_id: str


class StsSessionControlResult(BaseModel):
    """STS → API: control-plane action applied."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    status: str
    paused: bool = False
    strategy: str | None = None


class LeaseHeartbeat(BaseModel):
    """STS → TD fencing lease heartbeat on ``sts.{session_id}``."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    token: int


class LeaseAck(BaseModel):
    """TD → STS fencing lease ACK on ``td.{api_id}.{session_id}``."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    session_id: str
    token: int


class Recon(BaseModel):
    """STS → TD: request OMS reconciliation for ``api_id``."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int


class ReconDone(BaseModel):
    """TD → STS: reconciliation finished (OMS also published to ``td.oms.{api_id}``)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int


class StsDetach(BaseModel):
    """STS → TD: strategy session stopping; drop this attach (refcount --)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int


HeartbeatEnvelope = Envelope[Heartbeat]
LogEnvelope = Envelope[Log]
HealthCheckEnvelope = Envelope[HealthCheck]
HealthStatusEnvelope = Envelope[HealthStatus]
RpcErrorEnvelope = Envelope[RpcError]
TdAttachRequestEnvelope = Envelope[TdAttachRequest]
TdAttachResultEnvelope = Envelope[TdAttachResult]
CreateSessionRequestEnvelope = TdAttachRequestEnvelope
CreateSessionResultEnvelope = TdAttachResultEnvelope
StsCreateSessionRequestEnvelope = Envelope[StsCreateSessionRequest]
StsCreateSessionResultEnvelope = Envelope[StsCreateSessionResult]
StsSessionControlRequestEnvelope = Envelope[StsSessionControlRequest]
StsSessionControlResultEnvelope = Envelope[StsSessionControlResult]
ListSessionsRequestEnvelope = Envelope[ListSessionsRequest]
ListSessionsResultEnvelope = Envelope[ListSessionsResult]
LeaseHeartbeatEnvelope = Envelope[LeaseHeartbeat]
LeaseAckEnvelope = Envelope[LeaseAck]
ReconEnvelope = Envelope[Recon]
ReconDoneEnvelope = Envelope[ReconDone]
StsDetachEnvelope = Envelope[StsDetach]

# Envelope.type constants for control-plane RPC
TD_HEALTH = "td.health"
TD_ERROR = "td.error"
TD_SESSION_ATTACH = "td.session.attach"
TD_SESSION_CREATE = TD_SESSION_ATTACH  # alias
TD_SESSION_LIST = "td.session.list"
TD_LEASE_ACK = "td.lease.ack"
TD_RECON_DONE = "td.recon.done"
TD_OMS_VIEW = "td.oms.view"

STS_HEALTH = "sts.health"
STS_ERROR = "sts.error"
STS_SESSION_CREATE = "sts.session.create"
STS_SESSION_LIST = "sts.session.list"
STS_SESSION_PAUSE = "sts.session.pause"
STS_SESSION_RESUME = "sts.session.resume"
STS_SESSION_STOP = "sts.session.stop"
STS_LEASE_HEARTBEAT = "sts.lease.heartbeat"
STS_HEARTBEAT = STS_LEASE_HEARTBEAT  # alias for older names
STS_RECON = "sts.recon"
STS_DETACH = "sts.detach"

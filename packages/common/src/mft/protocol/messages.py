"""Concrete payload models carried inside ``Envelope[T]``."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mft.exchange.models import OrderType, Side
from mft.exchange.oms import OmsView
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
    venue: str | None = None


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
    """STS fencing lease heartbeat on ``sts.td.*`` / ``sts.md.*``."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    token: int


class LeaseAck(BaseModel):
    """TD → STS fencing lease ACK on ``td.{api_id}.{session_id}``."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    session_id: str
    token: int


class MdLeaseAck(BaseModel):
    """MD → STS fencing lease ACK on ``md.{session_id}``."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    token: int


class MdAttachRequest(BaseModel):
    """API → MD: attach STS session with market-data subscriptions."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    created_by: int
    subscriptions: list[str] = Field(default_factory=list)
    timeout: float = 30.0


class MdAttachResult(BaseModel):
    """MD → API: attach succeeded (lease heartbeat observed)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    subscriptions: list[str] = Field(default_factory=list)
    refcounts: dict[str, int] = Field(default_factory=dict)


class MdSubscribe(BaseModel):
    """STS → MD: add a feed subscription on the session stream."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    feed: str


class MdUnsubscribe(BaseModel):
    """STS → MD: drop a feed subscription on the session stream."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    feed: str


class MdDetach(BaseModel):
    """STS → MD: strategy session stopping; drop all MD attaches."""

    model_config = ConfigDict(frozen=True)

    session_id: str


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
    oms: OmsView = Field(default_factory=OmsView)


class StsDetach(BaseModel):
    """STS → TD: strategy session stopping; drop this attach (refcount --)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int


class OrderSubmit(BaseModel):
    """STS → TD: place an order for ``api_id`` (keyed by ``client_order_id``)."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    symbol: str
    side: Side
    type: OrderType
    qty: Decimal
    price: Decimal | None = None
    client_order_id: str


class OrderCancel(BaseModel):
    """STS → TD: cancel an open order by ``client_order_id``."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    client_order_id: str


class OrderReject(BaseModel):
    """TD → STS: submit rejected (publish on ``td.{api_id}.global``)."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    client_order_id: str | None = None
    order_id: str | None = None
    symbol: str | None = None
    reason: str


class CancelReject(BaseModel):
    """TD → STS: cancel rejected (publish on ``td.{api_id}.global``)."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    client_order_id: str | None = None
    order_id: str | None = None
    reason: str


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
OrderSubmitEnvelope = Envelope[OrderSubmit]
OrderCancelEnvelope = Envelope[OrderCancel]
OrderRejectEnvelope = Envelope[OrderReject]
CancelRejectEnvelope = Envelope[CancelReject]
MdLeaseAckEnvelope = Envelope[MdLeaseAck]
MdAttachRequestEnvelope = Envelope[MdAttachRequest]
MdAttachResultEnvelope = Envelope[MdAttachResult]
MdSubscribeEnvelope = Envelope[MdSubscribe]
MdUnsubscribeEnvelope = Envelope[MdUnsubscribe]
MdDetachEnvelope = Envelope[MdDetach]

# Envelope.type constants for control-plane RPC
TD_HEALTH = "td.health"
TD_ERROR = "td.error"
TD_SESSION_ATTACH = "td.session.attach"
TD_SESSION_CREATE = TD_SESSION_ATTACH  # alias
TD_SESSION_LIST = "td.session.list"
TD_LEASE_ACK = "td.lease.ack"
TD_RECON_DONE = "td.recon.done"
TD_OMS_VIEW = "td.oms.view"
TD_ORDER_UPDATE = "td.order.update"
TD_FILL = "td.fill"
TD_ORDER_REJECT = "td.order.reject"
TD_CANCEL_REJECT = "td.cancel.reject"
TD_BALANCE_UPDATE = "td.balance.update"

# Paper engine RPC / streams
PAPER = "paper"
PAPER_ERROR = "paper.error"
PAPER_AUTH = "paper.auth"
PAPER_PLACE_ORDER = "paper.place_order"
PAPER_CANCEL_ORDER = "paper.cancel_order"
PAPER_CANCEL_BY_CLIENT_ORDER_ID = "paper.cancel_by_client_order_id"
PAPER_FETCH_ORDER = "paper.fetch_order"
PAPER_FETCH_OPEN_ORDERS = "paper.fetch_open_orders"
PAPER_FETCH_BALANCES = "paper.fetch_balances"
PAPER_ORDER = "paper.order"
PAPER_FILL = "paper.fill"
PAPER_BALANCE = "paper.balance"
PAPER_FETCH_ORDER_BOOK = "paper.fetch_order_book"
PAPER_FETCH_INSTRUMENTS = "paper.fetch_instruments"
PAPER_FETCH_TICKER = "paper.fetch_ticker"
PAPER_ORDER_BOOK = "paper.orderbook"


class PaperCredentials(BaseModel):
    """Auth material for paper-engine private RPCs."""

    model_config = ConfigDict(frozen=True)

    api_key: str
    api_secret: str
    passphrase: str | None = None


class PaperPlaceOrderRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    credentials: PaperCredentials
    symbol: str
    side: Side
    type: OrderType
    qty: Decimal
    price: Decimal | None = None
    client_order_id: str | None = None


class PaperCancelRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    credentials: PaperCredentials
    order_id: str | None = None
    client_order_id: str | None = None


class PaperFetchOrderRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    credentials: PaperCredentials
    order_id: str


class PaperFetchOpenOrdersRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    credentials: PaperCredentials
    symbol: str | None = None


class PaperFetchBalancesRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    credentials: PaperCredentials


class PaperFetchOrderBookRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    depth: int = 10


class PaperFetchTickerRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str


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
STS_ORDER_SUBMIT = "sts.order.submit"
STS_ORDER_CANCEL = "sts.order.cancel"

MD_HEALTH = "md.health"
MD_ERROR = "md.error"
MD_SESSION_ATTACH = "md.session.attach"
MD_SESSION_LIST = "md.session.list"
MD_LEASE_ACK = "md.lease.ack"
MD_ORDERBOOK = "md.orderbook"
MD_SUBSCRIBE = "md.subscribe"
MD_UNSUBSCRIBE = "md.unsubscribe"
MD_DETACH = "md.detach"

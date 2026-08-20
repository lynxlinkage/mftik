"""Concrete payload models carried inside ``Envelope[T]``."""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange.models import (
    BestQuote,
    Kline,
    OrderBook,
    OrderType,
    Side,
    TimeInForce,
)
from mftik.exchange.oms import OmsView
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol.envelope import Envelope
from mftik.protocol.query_codes import QueryCode
from mftik.protocol.reject_codes import RejectCode


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


class TdBackfill(BaseModel):
    """Anyone → TD: re-read this account's history from the venue.

    A request, not a command: whoever takes it decides how much of the walk to
    do, and a run that stops early is not a failure — the cursors it advanced
    are progress, and the next request resumes from them. That is what lets the
    same message be sent by a schedule, by a strategy detaching, and by a TD on
    its way down, without any of them coordinating.

    ``tickers`` narrows the walk to named instruments. Empty means every
    instrument the account has an order on file for, which is the set that can
    have history worth reading.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    tickers: list[str] = Field(default_factory=list)
    #: Why this was asked for, for the log. ``cron`` / ``detach`` / ``shutdown``.
    reason: str = ""


class TdBackfillResult(BaseModel):
    """TD → caller: what one backfill run managed to confirm."""

    model_config = ConfigDict(frozen=True)

    api_id: int
    ok: bool
    #: Instruments walked to completion this run.
    tickers: list[str] = Field(default_factory=list)
    fills: int = 0
    orders: int = 0
    #: The weakest settlement line across the walks, after this run.
    confirmed_through_ts: float = 0.0
    reason: str = ""


class TdDetachRequest(BaseModel):
    """STS → TD: drop this attach (refcount --), and say so.

    Request-reply rather than a message on the session stream, for the same
    reason order entry is: the sender needs to learn whether it landed. A
    detach published to ``sts.td.{session}`` is read by one subscriber per
    link and acted on only by the one whose api_id it names — so if that
    link's reader is gone, the message is dropped by the sibling that did
    read it, and nothing anywhere records that the attach was never closed.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    reason: str = "sts_stop"


class TdDetachResult(BaseModel):
    """TD → STS: the attach is closed; ``refcount`` is what is left."""

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
    #: ``always`` | ``never`` — see ``StrategySpec.restart``.
    restart: str = "always"
    #: Qualified registry key (``CrossArb``, ``private::Tiny``). Optional so
    #: an old API and a new STS can pass each other during a rolling upgrade.
    type: str | None = None
    #: The submitted ``strategy.yml``. Same upgrade window as ``type``.
    yaml_text: str | None = None


class StsCreateSessionResult(BaseModel):
    """STS → API: strategy session created.

    ``status`` is not always ``live``. A strategy that rejects its
    configuration does so in ``on_start`` / ``on_ready`` — which run before
    this reply is sent — so the session can be over before the caller has
    heard that it began. Saying so here is what lets a deploy stop at that
    point instead of attaching feeds to a session that is already gone and
    learning about it, half a minute later, as a lease timeout.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    strategy: str
    td: list[int] = Field(default_factory=list)
    #: ``live`` | ``failed`` | ``done``.
    status: str = "live"
    #: Why it is not live. The strategy's own words, meant for an operator.
    reason: str | None = None


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
    venue: str | None = None
    #: Why a ``failed`` session ended. Null for live and natural ends.
    reason: str | None = None


class ListSessionsResult(BaseModel):
    """Domain → API: session list."""

    model_config = ConfigDict(frozen=True)

    sessions: list[SessionInfo] = Field(default_factory=list)


class StsSessionControlRequest(BaseModel):
    """API → STS: stop / fail a strategy session."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    #: Why a fail is being recorded. Ignored by stop.
    reason: str | None = None


class StsSessionControlResult(BaseModel):
    """STS → API: control-plane action applied."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    status: str
    strategy: str | None = None
    #: Set when the action left the session in ``failed``.
    reason: str | None = None


class StsEventLogPart(BaseModel):
    """One file of a session's event log — see :mod:`mftik.strategy.eventlog`.

    Rotation splits a long session across several, so a whole log is the parts
    concatenated in the order they are listed, which is oldest first.
    """

    model_config = ConfigDict(frozen=True)

    #: File name as STS will accept it back on a read. Not a path: the reader
    #: never composes one, it echoes what this listing gave it.
    name: str
    size: int
    modified: float | None = None


class StsRegistryReloadRequest(BaseModel):
    """API → STS: re-scan the registry directory and re-register what is there.

    STS imports the registry at boot. Everything that changes it afterwards —
    a strategy added through the API, a peer connected, a tree replaced — is
    invisible to the running process until something says so, and this is
    that something. It takes no arguments: the store on disk is the whole
    input, and a reload that trusted the caller to name what changed would be
    wrong the moment two things changed at once.
    """

    model_config = ConfigDict(frozen=True)


class StsRegistryReloadResult(BaseModel):
    """STS → API: the qualified type keys this process now answers to.

    The list is what came back from the scan, not what was asked for. A tree
    that failed to import, or whose name collides with a bundled strategy, is
    absent — so a caller can tell "added and loadable" from "added" by
    looking for its own key, which is the difference between a deploy that
    will work and one that will 404.

    ``generation`` is the env stamp this process now believes — the copy
    read at boot and on this reload, not a fresh open of ``applied.json``.
    Apply compares it to the generation it just committed.
    """

    model_config = ConfigDict(frozen=True)

    loaded: list[str] = Field(default_factory=list)
    generation: int = 0


class StsRegistryGenerationRequest(BaseModel):
    """API → STS: which env generation this process has already adopted.

    Read-only. It does not re-scan the registry, does not import strategy
    trees, and does not retarget ``sys.path``. Opening Settings asks this;
    a write is what sends :data:`STS_REGISTRY_RELOAD`.
    """

    model_config = ConfigDict(frozen=True)


class StsRegistryGenerationResult(BaseModel):
    """STS → API: the in-memory env generation, or 0 before the first attach."""

    model_config = ConfigDict(frozen=True)

    generation: int = 0


class StsEventLogInfoRequest(BaseModel):
    """API → STS: what event log does this session have, and how big."""

    model_config = ConfigDict(frozen=True)

    session_id: str


class StsEventLogInfo(BaseModel):
    """STS → API: the parts of one session's event log.

    ``available`` False means this STS has no log for the session — either
    logging is off, or the session ran on a different process. The two are
    distinguished by ``enabled``, because "we do not keep these" and "we keep
    these but not that one" call for different answers from an operator.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    available: bool = False
    #: Whether this STS keeps event logs at all (``STS_EVENTLOG_DIR`` set).
    enabled: bool = False
    #: Oldest first — the order a reader wants them concatenated in.
    parts: list[StsEventLogPart] = Field(default_factory=list)
    total_bytes: int = 0
    #: Session still running here, so the newest part is still being written
    #: and a download is a prefix rather than the whole story.
    live: bool = False


class StsEventLogReadRequest(BaseModel):
    """API → STS: one slice of one part.

    Paged rather than streamed because a domain's RPC subject is served in
    turn: a handler that sat on the queue for the length of a file transfer
    would hold every session create and stop behind it. One small read per
    request keeps that loop moving, and lets a download resume instead of
    starting over.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    #: A ``name`` from :class:`StsEventLogInfo`, never a path.
    part: str
    offset: int = 0
    length: int = 262_144


class StsEventLogChunk(BaseModel):
    """STS → API: one slice, gzipped and base64'd.

    Compressed at the source because this crosses the broker that also carries
    order acks, and jsonl gives up roughly a factor of ten. Base64 because the
    envelope is JSON; the two together still cost far less than the raw bytes.

    Each chunk is a self-contained gzip member, so a reader concatenates them
    — across parts as well — and gets one valid ``.gz`` without decompressing
    anything on the way through.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    part: str
    offset: int
    #: base64 of gzip of the slice. Empty at or past end of file.
    data: str = ""
    #: Uncompressed length of this slice, so a caller can advance ``offset``.
    raw_bytes: int = 0
    #: Nothing follows this slice in this part.
    eof: bool = True


class StsSessionStatus(BaseModel):
    """STS → UI: one session's control-plane state, as a full snapshot.

    Deliberately not a delta (``{"event": "stopped"}``): pub/sub drops messages
    whenever nobody is subscribed, and a consumer that has to replay
    transitions to know where it stands ends up permanently wrong after one
    missed line. A snapshot is idempotent — apply the newest one per
    ``session_id`` and the state is right no matter what was missed.

    Every field here is also on :class:`SessionInfo`, so the REST snapshot the
    UI loads first and the events it applies afterwards agree by construction.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    #: live | done | failed | interrupted | ack
    status: str
    strategy: str | None = None
    reason: str | None = None
    created_by: int | None = None
    finished_at: float | None = None


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


class MdDetachRequest(BaseModel):
    """STS → MD: drop this session's attach, and say so.

    Request-reply for the same reason as :class:`TdDetachRequest` — the
    session stream's only reader is the lease loop, so a detach published
    there is lost exactly when it matters most.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    reason: str = "sts_stop"


class MdDetachResult(BaseModel):
    """MD → STS: the attach is closed and its feeds released."""

    model_config = ConfigDict(frozen=True)

    session_id: str


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


class MdFetchRequest(BaseModel):
    """What every market-data query carries, whatever it is asking for.

    The request says where its answer goes. MD's fetch plane has no notion of
    who is asking and holds no attachment to them — it reads ``reply_channel``
    and publishes there, which is what lets anything ask without first
    becoming a market-data session. There is no ``session_id`` for the same
    reason: a caller that is not a strategy has none, and the fetch plane
    would have nothing to do with it if it did.

    ``query_id`` is quoted back by both the ack and the result. The answer is
    asynchronous, so a caller always needs something to correlate on, and the
    reply channel alone will not do it — one channel carries every answer that
    caller asked for, of every kind.
    """

    model_config = ConfigDict(frozen=True)

    #: Pub/sub channel for the result. The caller's to choose, and to be
    #: listening on before it asks.
    reply_channel: str
    query_id: str
    #: The instrument, as a rendered ``UniversalTicker``. It carries the
    #: venue, so there is no separate field for one.
    ticker: str


class MdFetchKlines(MdFetchRequest):
    """Ask for recent candles, oldest first.

    ``interval`` is canonical spelling (:mod:`mftik.exchange.intervals`); the
    venue's own vocabulary never reaches the wire.
    """

    interval: str
    limit: int = 100


class MdFetchOrderBook(MdFetchRequest):
    """Ask for a book snapshot, capped at ``depth`` levels a side.

    A snapshot, not a subscription. The ``orderbook`` feed pushes whole books
    on the venue's own schedule, which is what a strategy watching the book
    wants; this is for one that needs the book *now* and then not again.
    """

    depth: int = 10


class MdFetchBestQuote(MdFetchRequest):
    """Ask for the top of the book with its resting sizes.

    The same read as :class:`MdFetchOrderBook` at ``depth`` 1, in the shape a
    caller that only cares about the touch actually wants — no venue serves a
    separate endpoint for it, and unpacking a book to reach two levels is work
    every such caller would otherwise repeat.
    """


class MdQueryAck(BaseModel):
    """MD → caller: the immediate reply to a query request.

    ``accepted`` means MD took the query and will run it, not that the venue
    answered. The data arrives separately, and can fail after being accepted
    here — the ack is cheap and the venue round trip is not, and holding the
    reply open for the second would stall every query behind it.

    A refusal at this stage is always MD's own, so ``error_code`` is in the
    ``1xx`` band: the venue was never asked.
    """

    model_config = ConfigDict(frozen=True)

    query_id: str
    accepted: bool
    #: Human-readable, for logs and the UI. Free-form; do not branch on it.
    reason: str = ""
    #: Machine-readable — see :mod:`mftik.protocol.query_codes`. Branch on this.
    error_code: int | str = QueryCode.NONE


class MdFetchResult(BaseModel):
    """What every query answer carries, quoting the id that asked for it.

    Sent whether or not the query worked: a caller waiting on ``query_id`` has
    no other way to learn that it never will, and a hook that only fires on
    success leaves it unable to tell failure from delay.

    ``ok`` False means the payload is absent or empty and the reason is in
    ``error_code``. ``ok`` True with nothing in it is a real answer — the venue
    had none to give — and must not be read as a failure.
    """

    model_config = ConfigDict(frozen=True)

    query_id: str
    #: Quoted back from the request — see :class:`MdFetchRequest.ticker`.
    ticker: str
    ok: bool = True
    #: Human-readable, and the venue's own words when it was the venue that
    #: refused. Free-form; do not branch on it.
    reason: str = ""
    #: Machine-readable — see :mod:`mftik.protocol.query_codes`. Branch on this.
    error_code: int | str = QueryCode.NONE


class MdKlinesResult(MdFetchResult):
    """The answer to :class:`MdFetchKlines`.

    An ``ok`` result with no candles means the venue has no history that far
    back for this instrument.
    """

    interval: str
    klines: list[Kline] = Field(default_factory=list)


class MdOrderBookResult(MdFetchResult):
    """The answer to :class:`MdFetchOrderBook`.

    ``book`` is None only when the query failed. An ``ok`` result with an empty
    side is a real book: nothing is resting there.
    """

    book: OrderBook | None = None


class MdBestQuoteResult(MdFetchResult):
    """The answer to :class:`MdFetchBestQuote`.

    ``quote`` is None when the query failed **or** when the book had no resting
    order on one side, which is not an error and not a quote either — a caller
    checking whether its price can rest has nothing to check against, and
    should treat it as "ask again", not as "the book is empty at zero".
    """

    quote: BestQuote | None = None


class Recon(BaseModel):
    """STS → TD: request an async OMS snapshot for ``api_id``.

    TD answers from its current book when clean, or after settling UNKNOWN
    orders. This is not a request to hit the venue on the strategy's behalf.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int


class ReconDone(BaseModel):
    """TD → STS: book snapshot ready (OMS also on ``td.oms.{api_id}``)."""

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
    """STS → TD: place an order for ``api_id`` (keyed by ``client_order_id``).

    The instrument is a **universal ticker**, not a symbol. ``api_id`` pins
    which account the order is for, but not which instrument: on a unified
    venue ``BTCUSDT`` names both the spot pair and the perp, and the two are
    different books at different prices. So STS says which, and TD checks that
    it is one this session's venue trades before doing anything with it —
    ``Binance_Spot_BTCUSDT`` sent to a Bybit session is a strategy bug, and
    the only place it can be caught is where the two meet.

    Resolving it to the venue's own spelling is TD's job, through the symbol
    plane. STS neither knows nor needs to know what Gate calls this pair.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    universal_ticker: str
    side: Side
    type: OrderType
    #: Base size. Required on a limit; on a market, exactly one of this and
    #: ``quote_qty``.
    qty: Decimal | None = None
    #: Quote-currency size for a market order. ``qty`` is always base; this
    #: is the spend (or proceeds) when the venue sizes the order in quote.
    quote_qty: Decimal | None = None
    price: Decimal | None = None
    #: ``None`` leaves it to the adapter's default for this order type.
    tif: TimeInForce | None = None
    #: Close only — ask the venue to refuse this order rather than let it open
    #: or extend a position. Contract markets only: spot has no position to
    #: reduce, and TD refuses a spot order carrying it instead of dropping the
    #: flag, because a strategy sets this to be certain it cannot go the other
    #: way and a silently ignored guarantee is worse than none.
    reduce_only: bool = False
    client_order_id: str


class OrderCancel(BaseModel):
    """STS → TD: cancel an open order by ``client_order_id``."""

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    client_order_id: str


class OrderAck(BaseModel):
    """TD → STS: reply to a submit/cancel request on ``td.order.{api_id}``.

    ``accepted`` means TD took the request, not that the venue accepted it.
    The venue outcome still arrives asynchronously on ``td.{api_id}.global``
    as an order update or a reject.

    A refusal here is always TD's own, so ``error_code`` is always in the
    ``1xx`` band: the request never left the process.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    client_order_id: str
    accepted: bool
    #: Human-readable, for logs and the UI. Free-form; do not branch on it.
    reason: str = ""
    #: Machine-readable — see :mod:`mftik.protocol.reject_codes`. Branch on this.
    error_code: int | str = RejectCode.NONE


class EnsureLeverage(BaseModel):
    """STS → TD: look up (and cache) this account's leverage for a perp.

    Answered on ``td.account.{api_id}`` with :class:`LeverageAck`. Spot
    tickers and venues without a leverage read are refused.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    api_id: int
    universal_ticker: str


class LeverageAck(BaseModel):
    """TD → STS: reply to :class:`EnsureLeverage` on ``td.account.{api_id}``.

    Unlike :class:`OrderAck`, this waits for the venue (or a TD cache hit)
    before answering: ``ok`` means the leverage figure is known and cached
    on TD, not merely that the request was accepted.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    universal_ticker: str
    ok: bool
    leverage: Decimal | None = None
    reason: str = ""
    error_code: int | str = RejectCode.NONE


class OrderReject(BaseModel):
    """TD → STS: submit rejected (publish on ``td.{api_id}.global``).

    ``error_code`` says who refused it and why in terms that hold across
    venues; ``reason`` keeps the venue's own words. See
    :mod:`mftik.protocol.reject_codes`.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    client_order_id: str | None = None
    order_id: str | None = None
    #: The instrument the refused order was for. ``None`` when the request was
    #: too malformed to say — a ticker TD could not parse is exactly that.
    universal_ticker: str | None = None
    #: Human-readable, for logs and the UI. Free-form; do not branch on it.
    reason: str
    #: Machine-readable — see :mod:`mftik.protocol.reject_codes`. Branch on this.
    error_code: int | str = RejectCode.NONE


class CancelReject(BaseModel):
    """TD → STS: cancel rejected (publish on ``td.{api_id}.global``).

    Same fields as :class:`OrderReject`, and the same rule: read
    ``error_code``, show ``reason``.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    client_order_id: str | None = None
    order_id: str | None = None
    #: Human-readable, for logs and the UI. Free-form; do not branch on it.
    reason: str
    #: Machine-readable — see :mod:`mftik.protocol.reject_codes`. Branch on this.
    error_code: int | str = RejectCode.NONE


HeartbeatEnvelope = Envelope[Heartbeat]
LogEnvelope = Envelope[Log]
HealthCheckEnvelope = Envelope[HealthCheck]
HealthStatusEnvelope = Envelope[HealthStatus]
RpcErrorEnvelope = Envelope[RpcError]
TdAttachRequestEnvelope = Envelope[TdAttachRequest]
TdAttachResultEnvelope = Envelope[TdAttachResult]
TdDetachRequestEnvelope = Envelope[TdDetachRequest]
TdDetachResultEnvelope = Envelope[TdDetachResult]
CreateSessionRequestEnvelope = TdAttachRequestEnvelope
CreateSessionResultEnvelope = TdAttachResultEnvelope
StsCreateSessionRequestEnvelope = Envelope[StsCreateSessionRequest]
StsCreateSessionResultEnvelope = Envelope[StsCreateSessionResult]
StsSessionControlRequestEnvelope = Envelope[StsSessionControlRequest]
StsSessionControlResultEnvelope = Envelope[StsSessionControlResult]
StsSessionStatusEnvelope = Envelope[StsSessionStatus]
StsRegistryReloadRequestEnvelope = Envelope[StsRegistryReloadRequest]
StsRegistryReloadResultEnvelope = Envelope[StsRegistryReloadResult]
StsRegistryGenerationRequestEnvelope = Envelope[StsRegistryGenerationRequest]
StsRegistryGenerationResultEnvelope = Envelope[StsRegistryGenerationResult]
StsEventLogInfoRequestEnvelope = Envelope[StsEventLogInfoRequest]
StsEventLogInfoEnvelope = Envelope[StsEventLogInfo]
StsEventLogReadRequestEnvelope = Envelope[StsEventLogReadRequest]
StsEventLogChunkEnvelope = Envelope[StsEventLogChunk]
ListSessionsRequestEnvelope = Envelope[ListSessionsRequest]
ListSessionsResultEnvelope = Envelope[ListSessionsResult]
LeaseHeartbeatEnvelope = Envelope[LeaseHeartbeat]
LeaseAckEnvelope = Envelope[LeaseAck]
ReconEnvelope = Envelope[Recon]
ReconDoneEnvelope = Envelope[ReconDone]
StsDetachEnvelope = Envelope[StsDetach]
OrderSubmitEnvelope = Envelope[OrderSubmit]
OrderCancelEnvelope = Envelope[OrderCancel]
OrderAckEnvelope = Envelope[OrderAck]
EnsureLeverageEnvelope = Envelope[EnsureLeverage]
LeverageAckEnvelope = Envelope[LeverageAck]
OrderRejectEnvelope = Envelope[OrderReject]
CancelRejectEnvelope = Envelope[CancelReject]
MdLeaseAckEnvelope = Envelope[MdLeaseAck]
MdAttachRequestEnvelope = Envelope[MdAttachRequest]
MdAttachResultEnvelope = Envelope[MdAttachResult]
MdDetachRequestEnvelope = Envelope[MdDetachRequest]
MdDetachResultEnvelope = Envelope[MdDetachResult]
MdSubscribeEnvelope = Envelope[MdSubscribe]
MdUnsubscribeEnvelope = Envelope[MdUnsubscribe]
MdDetachEnvelope = Envelope[MdDetach]
MdFetchKlinesEnvelope = Envelope[MdFetchKlines]
MdFetchOrderBookEnvelope = Envelope[MdFetchOrderBook]
MdFetchBestQuoteEnvelope = Envelope[MdFetchBestQuote]
MdQueryAckEnvelope = Envelope[MdQueryAck]
MdKlinesResultEnvelope = Envelope[MdKlinesResult]
MdOrderBookResultEnvelope = Envelope[MdOrderBookResult]
MdBestQuoteResultEnvelope = Envelope[MdBestQuoteResult]

# Envelope.type constants for control-plane RPC
TD_HEALTH = "td.health"
TD_ERROR = "td.error"
TD_SESSION_ATTACH = "td.session.attach"
TD_SESSION_CREATE = TD_SESSION_ATTACH  # alias
TD_SESSION_DETACH = "td.session.detach"
TD_SESSION_LIST = "td.session.list"
TD_LEASE_ACK = "td.lease.ack"
TD_RECON_DONE = "td.recon.done"
TD_OMS_VIEW = "td.oms.view"
TD_LEDGER_VIEW = "td.ledger.view"
TD_ORDER_UPDATE = "td.order.update"
TD_BACKFILL = "td.backfill"
TD_BACKFILL_RESULT = "td.backfill.result"
TD_FILL = "td.fill"
TD_ORDER_ACK = "td.order.ack"
TD_LEVERAGE_ACK = "td.leverage.ack"
TD_ORDER_REJECT = "td.order.reject"
TD_CANCEL_REJECT = "td.cancel.reject"
TD_BALANCE_UPDATE = "td.balance.update"
TD_POSITION_UPDATE = "td.position.update"

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
    universal_ticker: str
    side: Side
    type: OrderType
    qty: Decimal | None = None
    quote_qty: Decimal | None = None
    price: Decimal | None = None
    tif: TimeInForce | None = None
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
STS_SESSION_STOP = "sts.session.stop"
STS_SESSION_FAIL = "sts.session.fail"
STS_SESSION_STATUS = "sts.session.status"
STS_EVENTLOG_INFO = "sts.eventlog.info"
STS_REGISTRY_RELOAD = "sts.registry.reload"
STS_REGISTRY_GENERATION = "sts.registry.generation"
STS_EVENTLOG_READ = "sts.eventlog.read"

#: ``reason`` written when an operator stopped a session from the UI. A fixed
#: sentinel rather than prose because it is matched, not just displayed: it is
#: the only thing separating "someone pulled this" from "the strategy finished"
#: — both of which are ``done``. The frontend compares against this exact
#: string, so changing it changes the wire contract.
STS_REASON_OPERATOR_STOP = "operator_stop"
STS_LEASE_HEARTBEAT = "sts.lease.heartbeat"
STS_HEARTBEAT = STS_LEASE_HEARTBEAT  # alias for older names
STS_RECON = "sts.recon"
STS_DETACH = "sts.detach"
STS_ORDER_SUBMIT = "sts.order.submit"
STS_ORDER_CANCEL = "sts.order.cancel"
STS_ENSURE_LEVERAGE = "sts.ensure_leverage"

MD_HEALTH = "md.health"
MD_ERROR = "md.error"
MD_SESSION_ATTACH = "md.session.attach"
MD_SESSION_DETACH = "md.session.detach"
MD_SESSION_LIST = "md.session.list"
MD_LEASE_ACK = "md.lease.ack"
MD_ORDERBOOK = "md.orderbook"
MD_TICKER = "md.ticker"
MD_TRADE = "md.trade"
MD_AGG_TRADE = "md.aggtrade"
MD_KLINE = "md.kline"
MD_BEST_QUOTE = "md.bestquote"
MD_LIQUIDATION = "md.liquidation"
MD_SUBSCRIBE = "md.subscribe"
MD_UNSUBSCRIBE = "md.unsubscribe"
MD_DETACH = "md.detach"
MD_FETCH_KLINES = "md.fetch.klines"
MD_FETCH_ORDERBOOK = "md.fetch.orderbook"
MD_FETCH_BESTQUOTE = "md.fetch.bestquote"
MD_QUERY_ACK = "md.query.ack"
MD_KLINES_RESULT = "md.klines.result"
MD_ORDERBOOK_RESULT = "md.orderbook.result"
MD_BESTQUOTE_RESULT = "md.bestquote.result"


# --- symbol plane (sym) ----------------------------------------------------


class SymbolFilterInfo(BaseModel):
    """One trading restriction. ``value`` is None when the venue set no bound."""

    model_config = ConfigDict(frozen=True)

    name: str
    value: Decimal | None = None


class SymbolInfo(BaseModel):
    """One instrument as the symbol plane knows it.

    ``universal_ticker`` is the platform's identity for the instrument;
    ``exch_ticker`` is what the venue wants on the wire. Adapters resolve one
    to the other through this rather than guessing.

    The venue, category and symbol are not separate fields — they are the
    three parts of the ticker, reachable through :attr:`ticker`. One field
    cannot disagree with itself, which three of them could.
    """

    model_config = ConfigDict(frozen=True)

    universal_ticker: str
    base: str
    quote: str
    exch_ticker: str
    contract_size: Decimal | None = None
    settlement_asset: str | None = None
    expiry: float | None = None
    is_active: bool = True
    filters: list[SymbolFilterInfo] = Field(default_factory=list)
    updated_at: float | None = None

    @property
    def ticker(self) -> UniversalTicker:
        """The parsed identity, for reaching venue / category / symbol."""
        return UniversalTicker.parse(self.universal_ticker)

    @property
    def venue(self) -> str:
        return self.ticker.venue

    @property
    def category(self) -> Category:
        return self.ticker.category

    @property
    def symbol(self) -> str:
        return self.ticker.symbol

    def filter(self, name: str) -> Decimal | None:
        """Bound for ``name``. None also means "published with no bound" —
        use :meth:`has_filter` when that distinction matters."""
        for row in self.filters:
            if row.name == name:
                return row.value
        return None

    def has_filter(self, name: str) -> bool:
        """Whether the venue publishes this restriction at all."""
        return any(row.name == name for row in self.filters)

    # --- rounding ----------------------------------------------------------
    #
    # TD does not validate orders against these filters, so a strategy must
    # round before it submits. Putting the arithmetic here keeps every
    # strategy from reimplementing it slightly differently.

    @property
    def price_tick(self) -> Decimal | None:
        return self.filter("price_tick")

    @property
    def qty_step(self) -> Decimal | None:
        return self.filter("qty_step")

    def round_price(self, price: Decimal) -> Decimal:
        """Snap ``price`` down to the venue's tick."""
        return _floor_to_step(price, self.price_tick)

    def round_qty(self, qty: Decimal) -> Decimal:
        """Snap ``qty`` down to the venue's lot step.

        Always down: rounding a size up can breach a balance or a risk limit,
        while rounding down only trades slightly less than intended.
        """
        return _floor_to_step(qty, self.qty_step)

    def qty_for_notional(self, notional: Decimal, price: Decimal) -> Decimal:
        """Size in base currency for a target quote-currency spend."""
        if price <= 0:
            raise ValueError(f"price must be positive, got {price}")
        return self.round_qty(notional / price)

    def meets_minimums(self, qty: Decimal, price: Decimal) -> bool:
        """Whether ``qty`` at ``price`` clears ``min_qty`` and ``min_notional``."""
        min_qty = self.filter("min_qty")
        if min_qty is not None and qty < min_qty:
            return False
        min_notional = self.filter("min_notional")
        if min_notional is not None and qty * price < min_notional:
            return False
        return True


class SymListRequest(BaseModel):
    """Query the plane. Omitted filters widen the result.

    The filters stay separate fields rather than one ticker: this is a query,
    and its whole use is to leave a part out — every instrument on ``Gate``,
    every ``Perp`` anywhere. A ticker names exactly one row and cannot express
    that. Pass ``universal_ticker`` when you do want exactly one.

    ``q`` / ``limit`` / ``offset`` page a browse; omit ``limit`` to get the
    whole match (what TD/MD cache loads). ``slim`` keeps only the filters the
    UI table shows — full filter sets are a follow-up by ticker.
    """

    model_config = ConfigDict(frozen=True)

    universal_ticker: str | None = None
    venue: str | None = None
    category: str | None = None
    symbol: str | None = None
    active_only: bool = True
    q: str | None = None
    limit: int | None = Field(default=None, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    slim: bool = False


class SymListResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbols: list[SymbolInfo] = Field(default_factory=list)
    #: Rows matching the filters before ``limit``/``offset``. Equals
    #: ``len(symbols)`` when the caller did not page.
    total: int = 0


class SymVenuesResult(BaseModel):
    """Venues the plane is configured to track, and what it has for each."""

    model_config = ConfigDict(frozen=True)

    venues: list[str] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class SymRefreshRequest(BaseModel):
    """Force a refresh. Omit ``venue`` to refresh every configured source."""

    model_config = ConfigDict(frozen=True)

    venue: str | None = None


class SymRefreshResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    refreshed: dict[str, int] = Field(default_factory=dict)
    deactivated: dict[str, int] = Field(default_factory=dict)
    failed: dict[str, str] = Field(default_factory=dict)


def _floor_to_step(value: Decimal, step: Decimal | None) -> Decimal:
    """Largest multiple of ``step`` not exceeding ``value``.

    The result carries no more decimal places than the step's granularity.
    That is not cosmetic: a ``Decimal`` product inherits the operands' scale,
    so a step stored as ``0.00010000`` — trailing zeros and all, which is how
    Binance publishes it — turns a size of ``0.0078`` into ``0.00780000``.
    Venues that check written precision reject the second one (Binance answers
    ``-1111``) while accepting the first, though they are the same number.
    """
    if step is None or step <= 0:
        return value
    floored = (value / step).to_integral_value(rounding=ROUND_FLOOR) * step
    return _strip(floored)


def _strip(value: Decimal) -> Decimal:
    """Drop trailing zeros without letting the result go exponential.

    ``Decimal.normalize`` alone would turn ``30`` into ``3E+1``, which is the
    same number but not a spelling every venue parses.
    """
    stripped = value.normalize()
    if stripped.as_tuple().exponent > 0:
        return stripped.quantize(Decimal(1))
    return stripped


SYM_HEALTH = "sym.health"
SYM_ERROR = "sym.error"
SYM_LIST = "sym.list"
SYM_VENUES = "sym.venues"
SYM_REFRESH = "sym.refresh"

SymListRequestEnvelope = Envelope[SymListRequest]
SymListResultEnvelope = Envelope[SymListResult]
SymVenuesResultEnvelope = Envelope[SymVenuesResult]
SymRefreshRequestEnvelope = Envelope[SymRefreshRequest]
SymRefreshResultEnvelope = Envelope[SymRefreshResult]

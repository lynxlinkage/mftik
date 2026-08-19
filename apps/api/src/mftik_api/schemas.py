"""HTTP response / request models for the MFTIK API."""

from __future__ import annotations

from typing import Any, Literal

from mftik.protocol import SymbolInfo
from pydantic import BaseModel, Field


class DomainStats(BaseModel):
    domain: str
    live: int = 0
    done: int = 0
    #: Sessions that ended badly. Only ``sts`` records these — td/md rows
    #: follow their strategy session and are only ever live or done.
    failed: int = 0
    #: Sessions STS cut short when it went down. Also sts-only.
    interrupted: int = 0
    #: Failed or interrupted sessions an operator has acknowledged. sts-only.
    ack: int = 0
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
    session_id: str
    type: str
    config: dict[str, Any] = Field(default_factory=dict)
    td: list[TdAttachOut] = Field(default_factory=list)
    md: list[str] = Field(default_factory=list)
    status: str = "live"


class StrategyOut(BaseModel):
    """One STS session as it appears on the strategies list."""

    type: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    created_by: int
    created_at: float
    session_id: str
    status: str | None = None
    #: Why a ``failed`` session ended. Null otherwise.
    reason: str | None = None


class StrategyListResponse(BaseModel):
    strategies: list[StrategyOut] = Field(default_factory=list)
    has_more: bool = False


class StrategyTemplateOut(BaseModel):
    """One deployable strategy and the document it starts from."""

    type: str
    label: str
    description: str
    yaml: str
    source: Literal["bundled", "registry"] = "bundled"


class StrategyTypesResponse(BaseModel):
    """Deployable strategies for the deploy picker.

    ``types`` is the bare list kept for callers that only need the names;
    ``templates`` carries what the UI needs to render the picker and swap the
    editor contents when the selection changes.
    """

    types: list[str] = Field(default_factory=list)
    templates: list[StrategyTemplateOut] = Field(default_factory=list)
    default: str = ""


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
    venue: str | None = None
    reason: str | None = None


class SessionListResponse(BaseModel):
    sessions: list[SessionOut] = Field(default_factory=list)


class StsControlResponse(BaseModel):
    session_id: str
    status: str
    strategy: str | None = None
    reason: str | None = None


class EventLogInfoResponse(BaseModel):
    """Whether a session's event log can be downloaded, and how big it is.

    ``available`` false with ``enabled`` false means this deployment keeps no
    event logs at all; with ``enabled`` true it means the STS that answered has
    none for this session. ``live`` says the session is still running, so a
    download is a prefix and not the whole of it.
    """

    session_id: str
    available: bool = False
    enabled: bool = False
    parts: int = 0
    total_bytes: int = 0
    live: bool = False


class StrategyYamlResponse(BaseModel):
    """The strategy.yml behind a past deploy.

    Normally ``yaml`` is the document as submitted. When ``reconstructed`` is
    true it is not: that deploy predates the text being stored, so this was
    rebuilt from the persisted spec, with comments and formatting gone and
    ``td`` showing accounts' current names. ``unresolved_td`` (reconstructed
    documents only) lists api ids whose account name could not be recovered —
    their ``td`` entries are placeholders that will not redeploy.
    """

    #: Strategy class this was deployed as — the document no longer carries it.
    type: str | None = None
    session_id: str
    yaml: str
    unresolved_td: list[int] = Field(default_factory=list)
    #: False when ``yaml`` is the original text; true when it was rebuilt.
    reconstructed: bool = False


class VenueOut(BaseModel):
    """A venue credentials can be registered against."""

    name: str
    label: str
    #: Markets this venue trades. One entry is a classic account; several is a
    #: unified one, where the category is part of every instrument's identity.
    categories: list[str] = Field(default_factory=list)
    api_types: list[str] = Field(default_factory=list)
    requires_passphrase: bool = False
    simulated: bool = False
    ticker_example: str = ""


class VenueListResponse(BaseModel):
    venues: list[VenueOut] = Field(default_factory=list)


class SymVenueListResponse(BaseModel):
    """Venues the symbol plane tracks, and how many instruments it holds."""

    venues: list[str] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class SymSymbolListResponse(BaseModel):
    symbols: list[SymbolInfo] = Field(default_factory=list)
    #: Match count before limit/offset; equals ``len(symbols)`` when unpaged.
    total: int = 0


class ApiCreateBody(BaseModel):
    """Create a venue API credential and its 1-1 trading account."""

    name: str = Field(..., min_length=1, max_length=128)
    venue: str = Field(..., min_length=1, max_length=64)
    api_key: str = Field(..., min_length=1, max_length=256)
    api_secret: str = Field(..., min_length=1)
    type: str = "HMAC"
    passphrase: str | None = None
    created_by: int | None = None


class ApiRenameBody(BaseModel):
    """Rename the trading account bound to an API credential."""

    name: str = Field(..., min_length=1, max_length=128)


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
    via: str | None = None
    key_kind: str | None = None
    key_id: int | None = None


class AuditListResponse(BaseModel):
    audits: list[AuditOut] = Field(default_factory=list)


class SessionLogOut(BaseModel):
    """One persisted log line. ``id`` is the envelope id (WS-compatible)."""

    id: str
    db_id: int
    ts: float
    source: str
    level: str
    message: str


class SessionLogListResponse(BaseModel):
    logs: list[SessionLogOut] = Field(default_factory=list)
    has_more: bool = False


class BoardSession(BaseModel):
    """One strategy run, as the board shows it.

    Counts and times, and deliberately no PnL. Deriving a result means matching
    executions into positions and valuing whatever is left open, and a number
    produced before that machinery exists would be believed. What is here is
    what the record can already state without arithmetic.
    """

    session_id: str
    strategy: str | None = None
    #: live | done | failed | interrupted
    status: str
    reason: str | None = None
    created_at: float
    finished_at: float | None = None
    #: Seconds from start to finish, or to now while it is still running.
    duration_s: float
    running: bool
    #: Executions recorded for this run. The only count here, and deliberately:
    #: it never decreases, and losing rows makes it under-report, which reads
    #: as a quiet run rather than as a wrong one.
    fills: int = 0
    td_api_ids: list[int] = Field(default_factory=list)
    #: How far this run's record has been confirmed against the venue. Null
    #: when nothing has been walked yet, which is not the same as zero fills —
    #: it is the record declining to claim it knows.
    confirmed_through_ts: float | None = None
    #: Whether every count above sits at or before that line.
    settled: bool = False


class BoardResponse(BaseModel):
    sessions: list[BoardSession] = Field(default_factory=list)


class BoardFill(BaseModel):
    """One execution, as the record holds it.

    Decimals travel as strings. Every one of these is money or size, and JSON
    numbers are doubles — a round trip through one is exactly the silent
    rounding the ``NUMERIC(38,18)`` columns exist to avoid, and it would happen
    on the way *out*, where nothing downstream could tell.
    """

    #: Row id, and the second half of the page cursor.
    id: int
    #: The venue's own trade id.
    fill_id: str
    universal_ticker: str
    side: str
    price: str
    qty: str
    fee: str
    fee_asset: str
    client_order_id: str | None = None
    venue_order_id: str | None = None
    api_id: int
    ts: float
    #: ``stream`` — what TD caught live. ``backfill`` — re-read from the venue.
    source: str
    #: Whether this row sits at or before the run's settlement line.
    settled: bool = False


class BoardFillListResponse(BaseModel):
    fills: list[BoardFill] = Field(default_factory=list)
    has_more: bool = False


class ErrorBody(BaseModel):
    code: str
    message: str


class RegistryAddBody(BaseModel):
    """Register a strategy tree on this node.

    ``files`` is path → UTF-8 text: the ``.py`` tree plus an optional root
    ``strategy.yml``. Default origin is ``private`` (this node only).
    ``public`` is what other nodes pull.
    """

    files: dict[str, str] = Field(..., min_length=1)
    replace: bool = False
    origin: Literal["public", "private"] = "private"


class RegistryStrategyOut(BaseModel):
    """One strategy as stored under ``public/``, ``private/``, or ``pulled/``."""

    name: str
    type: str
    digest: str
    requires_mftik: str
    requires: list[str] = Field(default_factory=list)
    origin: str
    files: list[str] = Field(default_factory=list)


class RegistryAddOut(RegistryStrategyOut):
    """A stored tree, plus whether the running STS picked it up.

    Its own shape rather than a field on ``RegistryStrategyOut`` because it
    answers a question only ``add`` raises. Listing what is on disk has no
    opinion about what STS has imported, and a field that was meaningless on
    every other response would invite being read as one.
    """

    #: STS re-scanned and now answers to this strategy's qualified type. False
    #: means the files are on disk and deploying them will not work yet —
    #: either STS did not answer, or it answered and skipped this tree (a bad
    #: import, or a name that collides with a bundled strategy). Its log says
    #: which.
    loaded: bool = False
    #: Why ``loaded`` is false, in a sentence meant for whoever ran the push.
    load_error: str | None = None


class RegistryRemovedOut(RegistryStrategyOut):
    """The tree that was deleted, and whether STS has stopped answering to it."""

    #: STS re-scanned and no longer resolves this strategy's qualified type.
    #: False means the files are gone but the process still holds the class it
    #: imported, so a deploy naming it would build a session from source that
    #: is no longer on disk. It stops on the next STS restart either way.
    unloaded: bool = False
    #: Why ``unloaded`` is false, for whoever ran the delete.
    unload_error: str | None = None


class RegistryStrategyDetailOut(RegistryStrategyOut):
    """Published tree plus file contents, for another node to pull."""

    contents: dict[str, str] = Field(default_factory=dict)


class RegistryStrategyListResponse(BaseModel):
    strategies: list[RegistryStrategyOut] = Field(default_factory=list)


class RegistryInfoOut(BaseModel):
    protocol: str
    protocol_version: int
    protocol_min: int
    mftik_version: str


class RegistryRemoteBody(BaseModel):
    """Name this node will show in front of pulled types (``name::HelloStrategy``)."""

    name: str
    url: str
    #: A registry key the peer issued us, for a peer that does not publish
    #: openly. Kept with the remote and sent on every pull from it.
    token: str | None = None


class RegistryRemoteOut(BaseModel):
    name: str
    url: str
    count: int = 0
    #: Whether we hold a key for this peer. Never the key: it is a credential
    #: someone else issued, and a list is not a place to hand it back out.
    authenticated: bool = False


class RegistrySyncRow(BaseModel):
    """One strategy as it sits on this node versus on the peer."""

    name: str
    type: str
    local_digest: str | None = None
    remote_digest: str | None = None
    status: str


class RegistryRemoteDetailOut(RegistryRemoteOut):
    """A named peer and the strategies pulled from it."""

    strategies: list[RegistryStrategyOut] = Field(default_factory=list)


class RegistryDiffOut(RegistryRemoteOut):
    reachable: bool = True
    error: str | None = None
    strategies: list[RegistrySyncRow] = Field(default_factory=list)


class RegistryRemotesResponse(BaseModel):
    remotes: list[RegistryRemoteOut] = Field(default_factory=list)


class RegistryConnectOut(BaseModel):
    name: str
    url: str
    pulled: list[RegistryStrategyOut] = Field(default_factory=list)
    #: Qualified type keys from this remote that STS resolves after the pull.
    #: A subset of ``pulled``: a copied tree can still fail to import here, or
    #: collide with a bundled name, and neither is visible from the files.
    loaded: list[str] = Field(default_factory=list)
    #: Set when STS could not be asked at all, so ``loaded`` is empty for a
    #: reason that has nothing to do with the strategies.
    load_error: str | None = None

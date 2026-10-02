"""Protocol v2 payloads: intent, session end, and the new broadcasts.

IF-01 defines these shapes. Constructing one is fine. Nothing here
serves them: a handler that is asked to raises
``NotImplementedError("IF-01")`` until the ticket that owns that
handler. The models are the interface.

**State authority (§3.3).** This package holds one kind of state, and it
is not in this module: the protocol version ``pv`` is a code constant on
the envelope (:mod:`mftik.protocol.version`). Everything below describes
state whose authority sits somewhere else.

* An intent row is written by the API, and by the STS controller when it
  re-puts while healing. MD and TD read it. The owner is released from
  ``procman.report`` (§8.2), not from a lease.
* ``sts.session.status`` describes session status. The STS controller's
  Supervisor is the authority; this is the snapshot it publishes.
* ``md.a.*`` is market data. The connection worker is the authority
  (F21). ``md.w.*`` is that worker's own connection state. ``md.universe.*``
  is the MD controller's selector.
* ``td.account.state.*`` and ``td.account.reset`` are the account
  worker's. ``td.account.trading`` is the controller's desired
  trading-layer bit and the worker's observed bit, on
  ``td.account.{api_id}``. ``td.order.cancel_session`` asks that
  worker to cancel.
* ``procman.report.*`` is the Supervisor's observation of which workers
  are alive, plus the entries the plane's orchestrator adds for a
  worker that should stay listed with no process (R4). It is not
  stored. A gap in the whole report reclaims nothing (F32).

**Invariants.**

* **P-1** ``*.intent.put`` and ``*.intent.delete`` are idempotent. A
  second put of the same owner replaces the desired feeds or accounts.
  It does not refcount. There is no per-session lease.
* **P-2** Every intent carries ``owner = (sts_instance, session_id)``
  (§8.2 rule 1).
* **P-3** ``sts.session.start`` accepts and returns. ``on_start`` has not
  run. Progress after the reply is ``sts.session.status`` (F12, §8.1).
  The request model is :class:`~mftik.protocol.messages.StsCreateSessionRequest`.
* **P-4** ``sts.session.end`` names a session and a reason. The worker
  runs ``on_stop``, then the status becomes terminal (§8.1).
* **P-5** ``md.a.{venue}.{hash}`` is one subject token per part. The hash
  is of the ``atom_id``, because the channel contains ``.`` (§6.1).
* **P-6** A published ``procman.report`` lists workers whose desired
  state is running. The Supervisor contributes slots in ``starting``,
  ``running`` and ``stopping``. The plane's orchestrator adds a
  session that is ``restarting`` and has no live process (R4). An
  owner missing from two consecutive reports is released (§8.2 rule
  3). Each worker carries ``code_ref`` (§4.3), ``incarnation``,
  ``phase`` (a :class:`~mftik.procman.WorkerPhase` value as a string;
  this module does not import procman), ``ready``, and ``rss_bytes``:
  the proportional set size (Pss) of that worker's process tree, not
  the shim and not plain ``VmRSS`` (§4.7). ``strategy_digest`` and
  ``env_generation`` are added by IF-16 (#275); they are not fields of
  this model. ``generation`` increases by one per publication and is
  not durable: a new process starts again. While publication is
  paused, or once it has stopped, the absence is not an observation
  and nothing is reclaimed (P5, F32).
"""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from mftik.protocol.envelope import Envelope
from mftik.protocol.strategy_yml import MdSelect, load_md

# --- owner -----------------------------------------------------------------


class IntentOwner(BaseModel):
    """Who an intent belongs to: ``(sts_instance, session_id)`` (§8.2).

    The MD and TD orchestrators release the intent when this owner is
    absent from two consecutive ``procman.report.sts.{instance}``
    publications. The report lists sessions whose desired state is
    running, so a session in ``restarting`` is still an owner (R4) and
    its intents stay.
    """

    model_config = ConfigDict(frozen=True)

    sts_instance: str
    session_id: str


# --- md intent -------------------------------------------------------------


class MdIntentPut(BaseModel):
    """API or STS controller → MD: ``md.intent.put`` (§8.1, §8.3).

    Idempotent (P-1). Putting the same owner again replaces ``feeds`` and
    ``selects``; it does not add a refcount. MD resolves feeds to atoms
    and answers with :class:`MdIntentPutResult`. Served on ``md`` or
    ``md.{instance}``, the same subjects attach used.

    ``feeds`` is the static list. ``selects`` is the ``select:`` blocks
    from ``strategy.yml`` (§3.3: an intent is feeds and selectors). Both
    may be empty; a put with neither still registers the owner.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    owner: IntentOwner
    feeds: dict[str, list[str]] = Field(default_factory=dict)
    selects: list[MdSelect] = Field(default_factory=list)

    @field_validator("feeds", mode="before")
    @classmethod
    def _feeds(cls, value: Any) -> dict[str, list[str]]:
        return load_md(value)


class MdIntentPutResult(BaseModel):
    """MD → caller: the intent is registered (§8.2).

    ``atoms`` is ``{feed: [atom_id]}``, what the session then subscribes
    to (§6.1). Empty means MD has not resolved the feeds. A handler that
    has not is not this model — it raises — so an empty map is a real
    answer only once resolution is implemented.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    atoms: dict[str, list[str]] = Field(default_factory=dict)


class MdIntentDelete(BaseModel):
    """API or orchestrator → MD: ``md.intent.delete`` (§8.1, §8.3).

    Idempotent. Deleting an owner that is already gone still succeeds.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    owner: IntentOwner
    reason: str = ""


class MdIntentDeleteResult(BaseModel):
    """MD → caller: the intent is released."""

    model_config = ConfigDict(frozen=True)

    session_id: str


class MdIntentPatch(BaseModel):
    """Session worker → MD: ``md.intent.patch`` (§5.1, §8.3).

    How a strategy's ``subscribe`` / ``unsubscribe`` reaches MD during a
    run. It replaces ``md.subscribe`` and ``md.unsubscribe``, which had
    no production sender. ``add`` and ``remove`` are feed keys. A
    selector's membership is not patched here: the controller publishes
    that on ``md.universe.{session_id}``.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    owner: IntentOwner
    add: list[str] = Field(default_factory=list)
    remove: list[str] = Field(default_factory=list)


class MdIntentPatchResult(BaseModel):
    """MD → session worker: the patch is registered."""

    model_config = ConfigDict(frozen=True)

    session_id: str


# --- td intent -------------------------------------------------------------


class TdIntentPut(BaseModel):
    """API or STS controller → TD: ``td.intent.put`` (§8.1, §8.3).

    Idempotent (P-1). ``api_ids`` is the whole set for this owner, not a
    delta: a second put replaces it. The intent decides whether the
    account's trading layer is on (F35). It does not decide whether the
    account worker exists. Served on ``td.{instance}``.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    owner: IntentOwner
    api_ids: list[int] = Field(default_factory=list)


class TdIntentPutResult(BaseModel):
    """TD → caller: the intent is registered."""

    model_config = ConfigDict(frozen=True)

    session_id: str


class TdIntentDelete(BaseModel):
    """API or orchestrator → TD: ``td.intent.delete`` (§8.1, §8.3).

    Idempotent. ``api_ids`` empty releases every account this owner
    holds; a non-empty list releases those accounts and leaves the rest.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    owner: IntentOwner
    api_ids: list[int] = Field(default_factory=list)
    reason: str = ""


class TdIntentDeleteResult(BaseModel):
    """TD → caller: the intent is released."""

    model_config = ConfigDict(frozen=True)

    session_id: str


# --- session end -----------------------------------------------------------


class StsSessionEndRequest(BaseModel):
    """API → STS controller: ``sts.session.end`` (§8.1, #298).

    Was ``sts.session.stop``. The controller serves it on
    ``sts.{instance}``, next to start and list (B4-02). The controller
    stops the worker through its Supervisor; the worker runs ``on_stop``
    on that signal (B4-03). ``sts.ctl.{session_id}`` stays the worker's
    subject and is not where this request is sent. ``reason`` is recorded
    on the terminal status;
    :data:`~mftik.protocol.messages.STS_REASON_OPERATOR_STOP` is the
    sentinel the UI compares against.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    reason: str


class StsSessionEndResult(BaseModel):
    """Controller → API: the session has entered a terminal status.

    ``status`` is that terminal value (``done`` or ``failed``). This
    reply is not an accept-and-return: end waits until the worker has
    exited or the stop has been given up (§8.1).
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    status: str


# --- md broadcasts ---------------------------------------------------------


class MdWorkerState(BaseModel):
    """Connection worker → listeners: ``md.worker.state`` on ``md.w.*``.

    The worker broadcasts this itself, not the controller, so a
    controller rolling does not interrupt it (P1, §5.6). ``version``
    increases on every publication, including the steady one every two
    seconds, so a listener can see that it missed one. Ten seconds of
    silence is ``down`` for the feeds this worker holds, and that is
    only a notification (§5.6).

    Authority: the connection worker. ``state`` is the connection's own
    word for how the socket is; the plan names the field and not a
    closed vocabulary.
    """

    model_config = ConfigDict(frozen=True)

    instance: str
    worker_id: str
    incarnation: int
    state: str
    version: int


class MdAtomState(BaseModel):
    """Connection worker → listeners: one atom's observed state (§6.3).

    A separate event from :class:`MdWorkerState`, on the same
    ``md.w.{instance}.{worker_id}`` subject. ``state`` is ``pending`` or
    ``subscribed``. A venue refusal is ``error`` with ``state`` left as
    the worker last knew it. ``first_msg_at`` and ``last_msg_at`` are
    the worker's clock, seconds.
    """

    model_config = ConfigDict(frozen=True)

    atom_id: str
    state: str
    version: int
    incarnation: int
    first_msg_at: float | None = None
    last_msg_at: float | None = None
    error: str | None = None


class MdUniverseEvent(BaseModel):
    """MD controller → session: ``md.universe`` on ``md.universe.{id}``.

    One selector's transition (§6.4, §8.3): ``name``, ``added``,
    ``removed``, ``current``, ``epoch``. ``added`` and ``removed`` are
    rendered platform tickers, not atom ids, and they are disjoint.
    ``current`` is the front contract of a ``rolling_future`` and null
    for an option chain. ``epoch`` increases and survives a controller
    restart.

    Authority: the MD controller, which persists the selector (§3.3).
    This object is one publication of that state, not a second copy.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    name: str
    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    current: str | None = None
    epoch: int


# --- td broadcasts and cancel_session --------------------------------------


class TdAccountState(BaseModel):
    """Account worker → sessions: ``td.account.state`` (§5.6, §7.1).

    Published on ``td.account.state.{api_id}`` by the worker, not the
    controller. ``state`` is ``ready``, ``degraded``, or ``unavailable``.
    ``version`` increases on every publication, including the steady one
    every two seconds. Ten seconds of silence is ``unavailable``, and
    that is only a notification.

    Authority: the account worker (§3.3).
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    incarnation: int
    state: Literal["ready", "degraded", "unavailable"]
    version: int
    reason: str = ""


class TdAccountTrading(BaseModel):
    """TD controller → account worker: ``td.account.trading`` (F35, §7.2).

    Sent on ``td.account.{api_id}``, once per reconcile pass, for every
    desired account. ``active`` on the request is the desired trading
    layer, level-triggered from intent. ``active`` on the ack is what
    the worker observed after applying it. A fresh worker starts off
    and waits for the first push. Silence after that keeps the last
    value (P5); this message is not how a missing controller closes
    the layer.
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    active: bool


class TdAccountReset(BaseModel):
    """Account worker → sessions: ``td.account.reset`` (§7.1, F13).

    Published on ``td.{api_id}.global`` after a new incarnation has
    rebuilt the ledger from the venue. Ingress treats it as
    ``cause="account_reset"`` and then ``on_resync``. ``TdReady`` is
    false until that recon settles.

    Authority: the account worker's trading layer, which owns the OMS
    and the ledger (§3.3).
    """

    model_config = ConfigDict(frozen=True)

    api_id: int
    incarnation: int


class TdCancelSessionRequest(BaseModel):
    """STS controller → account worker: ``td.order.cancel_session`` (F10).

    Served on ``td.order.{api_id}``. Cancels every resting order whose
    ``client_order_id`` names this session, including orders still
    ``PENDING_NEW`` or ``UNKNOWN`` once ``chase_unknown`` has settled
    them. The reply is success only when every one of them is confirmed.
    On timeout the reply lists the ones that are not (§7.1). Positions
    are not cancelled.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str


class TdCancelSessionResult(BaseModel):
    """Account worker → controller: how ``cancel_session`` finished.

    ``ok`` is true only when nothing in :attr:`unconfirmed` remains.
    ``unconfirmed`` is the ``client_order_id`` values still outstanding
    when the wait ended, and empty on success.
    """

    model_config = ConfigDict(frozen=True)

    session_id: str
    ok: bool
    unconfirmed: list[str] = Field(default_factory=list)


# --- procman report --------------------------------------------------------

#: :class:`~mftik.procman.WorkerPhase` values, as strings. Duplicated here
#: so this module does not import procman. The supervisor publishes
#: ``phase.value``. A test locks this set to the enum.
_WORKER_PHASES = frozenset(
    {
        "stopped",
        "starting",
        "running",
        "stopping",
        "failed",
        "crashed",
        "backoff",
        "fatal",
        "lost",
    }
)


class ProcmanWorker(BaseModel):
    """One worker inside a :class:`ProcmanReport`.

    ``id`` is the worker id (``sts/session/a1b2c3``). ``code_ref`` is the
    platform release the worker was spawned from (§4.3). ``incarnation``
    tells a new process from the previous one. ``phase`` is the
    supervisor's phase value, as a string. ``ready`` is the slot's ready
    bit.

    ``rss_bytes`` is the proportional set size (Pss) of that worker's
    process tree, summed from ``/proc/<pid>/smaps_rollup`` (kB × 1024).
    It is not the shim, and it is not plain ``VmRSS`` (§4.7). ``None``
    when ``smaps_rollup`` cannot be read for the worker pid itself. The
    strategy-tree digest and the extras generation are not here; IF-16
    adds them.
    """

    model_config = ConfigDict(frozen=True)

    id: str
    code_ref: str
    rss_bytes: int | None
    phase: str
    ready: bool
    incarnation: int

    @field_validator("rss_bytes")
    @classmethod
    def _rss(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("rss_bytes must be >= 0")
        return value

    @field_validator("phase")
    @classmethod
    def _phase(cls, value: str) -> str:
        if value not in _WORKER_PHASES:
            raise ValueError(f"unknown phase {value!r}")
        return value

    @field_validator("incarnation")
    @classmethod
    def _incarnation(cls, value: int) -> int:
        if value < 0:
            raise ValueError("incarnation must be >= 0")
        return value


class ProcmanReport(BaseModel):
    """Supervisor → listeners: ``procman.report`` (§8.2, §4.7).

    Published on ``procman.report.{plane}.{instance}``, inside
    :data:`ProcmanReportEnvelope` so ``pv`` is set. ``generation``
    increases by one each time this supervisor publishes, so a consumer
    can tell two successive reports apart. It is not durable: a new
    process starts again. It is the report's own counter, not an extras
    generation and not a session generation.

    ``workers`` is the set a listener treats as alive. The Supervisor
    contributes slots in ``starting``, ``running`` and ``stopping``, and
    nothing else: ``failed``, ``fatal``, ``lost``, ``crashed`` and
    ``backoff`` are absent, so two reports that omit an owner release
    that owner's intents (§8.2 rule 3). The plane's orchestrator may
    append entries whose desired state is running but that have no live
    process, including an STS session in ``restarting`` (R4). Putting
    those on the report belongs to the orchestrator, not the Supervisor.

    Authority: the Supervisor for the live slots (§3.3). The report is
    not stored. While publication is paused — before ``start`` has
    finished reconciling, and after ``close`` — and once publications
    have stopped entirely, the absence is not an observation. Consumers
    reclaim nothing (P5, F32). An empty ``workers`` list is an
    observation that nothing is live; pause is the absence of a report,
    not an empty one.

    The shape lives here so procman publishes this model instead of
    declaring a second one.
    """

    model_config = ConfigDict(frozen=True)

    generation: int
    workers: list[ProcmanWorker] = Field(default_factory=list)

    @field_validator("generation")
    @classmethod
    def _generation(cls, value: int) -> int:
        if value < 0:
            raise ValueError("generation must be >= 0")
        return value

    @model_validator(mode="after")
    def _unique_workers(self) -> Self:
        ids = [worker.id for worker in self.workers]
        if len(ids) != len(set(ids)):
            raise ValueError("report lists a worker id twice")
        return self


MdIntentPutEnvelope = Envelope[MdIntentPut]
MdIntentPutResultEnvelope = Envelope[MdIntentPutResult]
MdIntentDeleteEnvelope = Envelope[MdIntentDelete]
MdIntentDeleteResultEnvelope = Envelope[MdIntentDeleteResult]
MdIntentPatchEnvelope = Envelope[MdIntentPatch]
MdIntentPatchResultEnvelope = Envelope[MdIntentPatchResult]
TdIntentPutEnvelope = Envelope[TdIntentPut]
TdIntentPutResultEnvelope = Envelope[TdIntentPutResult]
TdIntentDeleteEnvelope = Envelope[TdIntentDelete]
TdIntentDeleteResultEnvelope = Envelope[TdIntentDeleteResult]
StsSessionEndRequestEnvelope = Envelope[StsSessionEndRequest]
StsSessionEndResultEnvelope = Envelope[StsSessionEndResult]
MdWorkerStateEnvelope = Envelope[MdWorkerState]
MdAtomStateEnvelope = Envelope[MdAtomState]
MdUniverseEventEnvelope = Envelope[MdUniverseEvent]
TdAccountStateEnvelope = Envelope[TdAccountState]
TdAccountTradingEnvelope = Envelope[TdAccountTrading]
TdAccountResetEnvelope = Envelope[TdAccountReset]
TdCancelSessionRequestEnvelope = Envelope[TdCancelSessionRequest]
TdCancelSessionResultEnvelope = Envelope[TdCancelSessionResult]
ProcmanReportEnvelope = Envelope[ProcmanReport]

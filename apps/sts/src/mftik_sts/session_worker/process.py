"""Run one session: ingress on the main thread, strategy on the other.

``python -m mftik_sts.session_worker <request.json>`` is the process the
controller spawns. Phases walk 0 to 6 once. The ingress subscribes
``sts.ctl.{session_id}`` and beats the shim from phase 0, so a long hook
on the strategy thread does not stop the beat. Market data is subscribed
in phase 1. TD is subscribed only after ``on_start`` returns. ``on_ready``
is called once. Delivery starts in phase 4. Phase 5 keeps delivering
until ``on_stop`` returns. Phase 6 writes the last status, drains NATS,
and exits.

The worker does not send ``md.intent.*`` or ``td.intent.*`` (the API owns
intents) and it does not open a database (the row is the controller's).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import signal
import sys
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.clock import Clock, SystemClock
from mftik.exchange.atoms import JoinPolicy
from mftik.exchange.oms import LedgerView, OmsView
from mftik.procman.heartbeat import heartbeat_loop, status_fd
from mftik.protocol import (
    DEFAULT_READY_TIMEOUT_S,
    DEFAULT_START_TIMEOUT_S,
    MD_FEED_END,
    STS_SESSION_END,
    STS_SESSION_FAIL,
    STS_SESSION_FORCE_STOP,
    STS_SESSION_STATUS,
    TD_LEDGER_VIEW,
    TD_OMS_VIEW,
    Envelope,
    StsCreateSessionRequest,
    StsSessionStatus,
    StsStatusProgress,
    TdLedgerViewRequest,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
    load_md,
    load_td,
    md_feeds_of,
    parse_strategy_yml,
    publish_sts_log,
    td_api_ids_of,
)
from mftik.protocol.version import PROTOCOL_VERSION, reject_if_pv_mismatch
from mftik.strategy import Ready, Strategy
from mftik.strategy.eventlog import EventLog
from mftik.symbols import SymbolClient

from mftik_sts.exit_codes import STRATEGY_EXCEPTION
from mftik_sts.session_worker.availability import (
    Availability,
    MdUpdate,
    Resync,
    TdUpdate,
    notice_text,
    read_oms_view,
    schedule_effects,
)
from mftik_sts.session_worker.budget import ON_READY_LIMIT_S, ON_STOP_LIMIT_S
from mftik_sts.session_worker.delivery import kind_of_topic
from mftik_sts.session_worker.dispatch import dispatch_md, dispatch_notice, dispatch_td
from mftik_sts.session_worker.events import Inbound, StreamKind
from mftik_sts.session_worker.ingress import Ingress
from mftik_sts.session_worker.limits import HEARTBEAT_PERIOD_S, TEMP_BUFFER_CAPACITY
from mftik_sts.session_worker.pending import PendingTable
from mftik_sts.session_worker.phase import Phase
from mftik_sts.session_worker.readiness import FeedReady, resolve_feeds
from mftik_sts.session_worker.runner import StrategyRunner

logger = logging.getLogger(__name__)

_SOURCE = "sts.session_worker"
_POLL_S = 0.05


class _Progress:
    """Lines the status snapshot is built from. Written from both threads."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.reason: str | None = None
        self.failed = False
        self.terminal: str | None = None
        self.on_start_at: float | None = None
        self.td_waiting: int | None = None
        self.td_ready = 0
        self.td_total = 0

    def fail(self, reason: str) -> None:
        with self._lock:
            if self.failed:
                return
            self.failed = True
            self.reason = reason

    def note_reason(self, reason: str) -> None:
        with self._lock:
            if self.reason is None:
                self.reason = reason

    def snapshot_bits(self) -> tuple[bool, str | None, str | None]:
        with self._lock:
            return self.failed, self.reason, self.terminal


class _Hooks:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._name: str | None = None
        self._started: float | None = None
        self._deadline: float | None = None

    def arm(self, name: str, *, started: float, deadline: float) -> None:
        with self._lock:
            self._name = name
            self._started = started
            self._deadline = deadline

    def clear(self) -> None:
        with self._lock:
            self._name = None
            self._started = None
            self._deadline = None

    def breached(self, now: float) -> str | None:
        with self._lock:
            if self._deadline is not None and now > self._deadline and self._name:
                return self._name
            return None

    def elapsed(self, now: float) -> tuple[str | None, float | None]:
        with self._lock:
            if self._name is None or self._started is None:
                return None, None
            return self._name, now - self._started


class WorkerSession:
    """What the strategy is bound to. Satisfies :class:`SessionView`.

    ``order_phase`` is how the SDK gates order entry without importing
    this package. It is ``on_start`` until ``on_ready`` is called, then
    ``ready`` for that call, ``running`` after it returns, and
    ``stopping`` while ``on_stop`` runs.
    """

    def __init__(
        self,
        request: StsCreateSessionRequest,
        *,
        broker: Any,
        event_log: EventLog,
        symbols: SymbolClient,
    ) -> None:
        self.session_id = request.session_id
        self.type = request.type
        self.broker = broker
        self.symbols = symbols
        self.event_log = event_log
        self.td = load_td(request.td)
        self.md = load_md(request.md)
        self.md_ids = md_feeds_of(self.md)
        self.st_paras = dict(request.st_paras or {})
        self.order_phase = "boot"
        self.strategy: Strategy | None = None
        self.availability: Availability | None = None
        self._exit = threading.Event()
        self.exit_reason: str | None = None
        self.exit_failed = False
        self._wake: Callable[[], None] | None = None

    @property
    def td_api_ids(self) -> list[int]:
        return td_api_ids_of(self.td)

    @property
    def exit_requested(self) -> bool:
        return self._exit.is_set()

    def td_account(self, name: str):  # type: ignore[no-untyped-def]
        try:
            return self.td[name]
        except KeyError:
            raise KeyError(
                f"session {self.session_id} has no td account named {name!r}"
            ) from None

    def td_sole(self) -> int:
        if len(self.td) != 1:
            raise RuntimeError(
                f"session {self.session_id} needs exactly one td account, "
                f"got {list(self.td)}"
            )
        return next(iter(self.td.values())).api_id

    def feed_state(self, feed: str) -> str | None:
        """``md.state``. ``None`` until a broadcast decides the feed."""
        tracker = self.availability
        if tracker is None:
            return None
        return tracker.md_state(feed)

    def account_state(self, api_id: int) -> str | None:
        """``td.state``. ``None`` until a broadcast decides the account."""
        tracker = self.availability
        if tracker is None:
            return None
        return tracker.td_state(api_id)

    def request_exit(
        self, reason: str = "strategy_exit", *, failed: bool = False
    ) -> None:
        if self._exit.is_set():
            return
        self.exit_reason = reason
        self.exit_failed = failed
        self._exit.set()
        if self._wake is not None:
            self._wake()


class CrossThreadBroker:
    """The strategy's broker. Requests are answered on the ingress.

    ``request`` registers the future before it publishes, and the publish
    flushes. The timeout is enforced by :class:`PendingTable` on the
    ingress clock, not by a timer on this loop.
    """

    def __init__(
        self,
        send: Broker,
        pending: PendingTable,
        inbox_for: Callable[[str], str],
        clock: Clock,
    ) -> None:
        self._send = send
        self._pending = pending
        self._inbox_for = inbox_for
        self._clock = clock
        self.config = send.config

    async def request(
        self,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float | None = None,
    ) -> UntypedEnvelope:
        wait = self.config.request_timeout if timeout is None else timeout
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        request_id = envelope.id
        self._pending.register(
            request_id,
            loop=loop,
            future=future,
            deadline=self._clock.monotonic() + float(wait),
            subject=subject,
            timeout=float(wait),
        )
        try:
            await self._send.publish_with_reply(
                subject, envelope, reply=self._inbox_for(request_id)
            )
        except Exception as exc:
            self._pending.cancel(request_id, exc)
            raise
        raw = await future
        return UntypedEnvelope.from_json(raw)

    async def publish(self, topic: str, envelope: Envelope[Any]) -> None:
        await self._send.publish(topic, envelope)
        await self._send.flush()

    async def publish_log(
        self,
        topic: str,
        envelope: Envelope[Any],
        *,
        maxlen: int | None = None,
        ttl_seconds: int = 86_400,
    ) -> None:
        del maxlen, ttl_seconds
        await self.publish(topic, envelope)


#: Same strings as :mod:`mftik_sts.hostdisk.identity`. Inlined so this
#: process does not import :mod:`mftik_sts.hostdisk`, whose package init
#: loads the controller and, through it, :mod:`mftik_db` (B5-09).
STRATEGY_DIGEST_ENV = "MFTIK_STRATEGY_DIGEST"
ENV_GENERATION_ENV = "MFTIK_ENV_GENERATION"


def load_strategy(name: str | None) -> Strategy:
    """Resolve ``name``.

    When ``MFTIK_STRATEGY_DIGEST`` is set, load ``registry/trees/<digest>``
    through :func:`mftik_sts.pinned_strategy.load_pinned`. That puts the
    pinned generation's ``site-packages`` on ``sys.path`` and does not
    call :func:`mftik_sts.runtime_env.refresh`. This module does not
    import the registry itself. With no digest, fall back to
    :func:`mftik_sts.impl.resolve` and
    :func:`mftik_sts.impl.load_local_registry`, which is how a built-in
    strategy and a tree planted only in the name layout still start.
    """
    digest = os.environ.get(STRATEGY_DIGEST_ENV, "").strip()
    if digest:
        from mftik_sts.pinned_strategy import load_pinned

        return load_pinned(digest)
    from mftik_sts.impl import load_local_registry, resolve

    try:
        return resolve(name)
    except KeyError:
        load_local_registry()
        return resolve(name)


def _arm_pdeathsig() -> None:
    """SIGTERM when the parent dies (S2). Linux only.

    The shim is the parent. A missing parent is the same stop as EPIPE
    on the status pipe: phase 5, not a crash inside the worker.
    """
    if sys.platform != "linux":
        return
    import ctypes

    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM) != 0:
        return
    if os.getppid() == 1:
        os.kill(os.getpid(), signal.SIGTERM)


def _kind_of(env_type: str) -> StreamKind | None:
    if env_type == MD_FEED_END:
        return StreamKind.FEED_END
    if env_type.startswith("md."):
        try:
            return kind_of_topic(env_type[3:])
        except ValueError:
            return None
    return StreamKind.TD


def delivery_overrides_of(request: StsCreateSessionRequest) -> dict[str, str]:
    """The ``md_delivery`` map from the submitted ``strategy.yml``.

    Empty when the request did not carry the document. Parsing stays
    IF-07's. A must-deliver kind is not a feed, so the map cannot name
    one.
    """
    if not request.yaml_text:
        return {}
    return dict(parse_strategy_yml(request.yaml_text).md_delivery)


def drop_status(ingress: Ingress) -> tuple[int, dict[str, str]]:
    """Total drops, and the per-feed counts for the status snapshot.

    ``progress.dropped`` is the total. Per-feed counts ride in
    ``conditions`` as ``dropped.<feed>`` because
    :class:`~mftik.protocol.messages.StsStatusProgress` has one integer.
    A feed that has not dropped is absent. Replacement is not a count.
    """
    total = ingress.delivery.dropped
    fields = {
        f"dropped.{feed}": str(count)
        for feed, count in ingress.delivery.dropped_by_feed.items()
        if count
    }
    return total, fields


def _inbound(
    raw: str,
    *,
    feed: str,
    kind: StreamKind,
    recv_ts: float,
    clock: Clock,
) -> Inbound | None:
    try:
        header = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(header, dict):
        return None
    event_id = header.get("id")
    if not isinstance(event_id, str) or not event_id:
        event_id = uuid.uuid4().hex
    seq = header.get("seq")
    if type(seq) is not int:
        seq = None
    bar_open, closed = _kline_header(header, kind)
    return Inbound(
        kind=kind,
        feed=feed,
        recv_ts=recv_ts,
        body=raw.encode(),
        event_id=event_id,
        seq=seq,
        bar_open=bar_open,
        closed=closed,
        clock=clock.now,
    )


def _kline_header(
    header: dict[str, Any], kind: StreamKind
) -> tuple[float | None, bool | None]:
    """``open_time`` and ``closed`` from the envelope payload.

    The ingress already parsed the JSON to read ``type`` and ``seq``.
    These two keys are the conflation header. This does not build a
    :class:`~mftik.exchange.models.Kline`.
    """
    if kind is not StreamKind.KLINE:
        return None, None
    payload = header.get("payload")
    if not isinstance(payload, dict):
        return None, None
    opened = payload.get("open_time")
    bar_open: float | None = None
    if not isinstance(opened, bool) and isinstance(opened, int | float):
        bar_open = float(opened)
    closed_bit = payload.get("closed")
    closed = closed_bit if type(closed_bit) is bool else None
    return bar_open, closed


async def amain(
    request: StsCreateSessionRequest,
    *,
    clock: Clock | None = None,
    install_signals: bool = False,
    capacity: int = TEMP_BUFFER_CAPACITY,
) -> int:
    """Walk phases 0–6. Return the process exit code.

    ``install_signals`` is for the process entry. An in-process caller
    leaves the host's handlers alone.
    """
    clock = clock if clock is not None else SystemClock()
    try:
        overrides = delivery_overrides_of(request)
    except Exception:
        logger.exception(
            "strategy.yml delivery overrides session=%s", request.session_id
        )
        return 1
    ingress = Ingress(
        request,
        capacity=capacity,
        delivery_overrides=overrides,
        start_timeout_s=DEFAULT_START_TIMEOUT_S,
        clock=clock,
    )
    ingress.start()
    progress = _Progress()
    hooks = _Hooks()
    pending = PendingTable()
    stop = threading.Event()
    cancel_hooks = threading.Event()
    ingress_stop = asyncio.Event()
    finished = asyncio.Event()
    loop = asyncio.get_running_loop()
    box: dict[str, int] = {"code": 0}
    strategy_loop: list[asyncio.AbstractEventLoop | None] = [None]
    wake_strategy: list[Callable[[], None]] = []

    def kick() -> None:
        callback = wake_strategy[0] if wake_strategy else None
        if callback is not None:
            callback()

    ingress.set_queued_callback(kick)

    def request_stop(reason: str | None = None) -> None:
        # Does not stop the ingress pumps. Phase 5 still needs the inbox
        # and the market subscriptions, and the shim heartbeat keeps
        # going until the strategy thread has finished.
        if reason:
            progress.note_reason(reason)
        stop.set()
        if strategy_loop[0] is not None:
            strategy_loop[0].call_soon_threadsafe(_noop)
        kick()

    def _td_overflow(reason: str) -> None:
        # ``fail`` is what marks the exit non-zero. ``request_stop`` is
        # what leaves the hook and runs phase 5. A TD event was not dropped.
        progress.fail(reason)
        request_stop(reason)

    ingress.set_failure_callback(_td_overflow)

    token = uuid.uuid4().hex
    inbox_wild = f"_INBOX.{token}.*"

    def inbox_for(request_id: str) -> str:
        return f"_INBOX.{token}.{request_id}"

    ingress_broker = Broker()
    try:
        await ingress_broker.connect()
    except Exception:
        logger.exception(
            "session worker ingress connect failed session=%s",
            request.session_id,
        )
        ingress.abort()
        return ingress.exit_code or 1

    feeds_box: list[FeedReady | None] = [None]
    availability_box: list[Availability | None] = [None]
    ingress_flags = {"down": False}

    hb_stop = asyncio.Event()

    def _hb_ready() -> bool:
        return ingress.phase in (Phase.RUNNING, Phase.STOPPING)

    async def _heartbeat() -> None:
        await heartbeat_loop(
            clock,
            ready=_hb_ready,
            period_s=HEARTBEAT_PERIOD_S,
            stop=hb_stop,
            fd=status_fd(),
        )
        # ``hb_stop`` is also set once the strategy thread has finished.
        # That return is shutdown, not a missing shim.
        if not stop.is_set() and not finished.is_set():
            request_stop("shim_gone")

    async def _watch() -> None:
        assert clock is not None
        reported: str | None = None
        while not ingress_stop.is_set():
            now = clock.monotonic()
            pending.expire(now)
            tracker = availability_box[0]
            if tracker is not None:
                _emit(tracker.tick(now))
            name = hooks.breached(now)
            if name is not None and name != reported:
                reported = name
                progress.fail(f"{name} exceeded its limit")
                cancel_hooks.set()
                await _log(progress.snapshot_bits()[1] or name, level="error")
                request_stop()
            sleep = asyncio.create_task(clock.sleep(_POLL_S))
            waiter = asyncio.create_task(ingress_stop.wait())
            _done, pending_tasks = await asyncio.wait(
                {sleep, waiter}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending_tasks:
                task.cancel()

    async def _publish_status(*, status: str | None = None) -> None:
        assert clock is not None
        failed, reason, terminal = progress.snapshot_bits()
        phase = ingress.phase
        if status is None:
            if terminal is not None:
                status = terminal
            elif phase in (None, Phase.BOOT, Phase.LOAD, Phase.ON_START, Phase.READY):
                status = "starting"
            elif phase is Phase.RUNNING:
                status = "running"
            elif phase is Phase.STOPPING:
                status = "stopping"
            elif failed:
                status = "failed"
            else:
                status = "done"
        hook, elapsed = hooks.elapsed(clock.monotonic())
        conditions = _conditions(
            status=status,
            phase=phase,
            clock=clock,
            progress=progress,
            feeds=feeds_box[0],
            availability=availability_box[0],
        )
        dropped, drop_fields = drop_status(ingress)
        conditions.update(drop_fields)
        snapshot = StsSessionStatus(
            session_id=request.session_id,
            status=status,
            strategy=request.strategy,
            reason=reason,
            created_by=request.created_by,
            type=request.type,
            conditions=conditions,
            progress=StsStatusProgress(
                hook=hook, elapsed_s=elapsed, dropped=dropped
            ),
        )
        envelope = Envelope[StsSessionStatus].wrap(
            snapshot,
            type=STS_SESSION_STATUS,
            source=_SOURCE,
            session_id=request.session_id,
        )
        try:
            await asyncio.wait_for(
                ingress_broker.publish(Topics.sts_status(request.session_id), envelope),
                timeout=1.0,
            )
        except Exception:
            logger.exception(
                "session status publish failed session=%s", request.session_id
            )

    async def _log(message: str, *, level: str = "info") -> None:
        try:
            await asyncio.wait_for(
                publish_sts_log(
                    ingress_broker,
                    request.session_id,
                    message,
                    source=_SOURCE,
                    level=level,
                    type=request.type,
                ),
                timeout=1.0,
            )
        except Exception:
            logger.exception("session log failed session=%s", request.session_id)

    async def _on_ctl(message: UntypedEnvelope) -> Envelope[Any] | None:
        if message.pv != PROTOCOL_VERSION:
            return None
        if message.type in (STS_SESSION_FAIL, STS_SESSION_FORCE_STOP):
            # No worker type was added for these. The controller still
            # serves them. Answering here would invent a result.
            return None
        if message.type == STS_SESSION_END:
            request_stop("sts.session.end")
            failed, reason, _terminal = progress.snapshot_bits()
            snapshot = StsSessionStatus(
                session_id=request.session_id,
                status="failed" if failed else "stopping",
                strategy=request.strategy,
                reason=reason,
                created_by=request.created_by,
                type=request.type,
                conditions={"phase": "failed" if failed else "stopping"},
            )
            return Envelope[StsSessionStatus].wrap(
                snapshot,
                type=STS_SESSION_STATUS,
                source=_SOURCE,
                session_id=request.session_id,
            )
        if message.type == STS_SESSION_STATUS:
            failed, reason, terminal = progress.snapshot_bits()
            phase = ingress.phase
            if terminal is not None:
                word = terminal
            elif phase is Phase.RUNNING:
                word = "running"
            elif phase is Phase.STOPPING:
                word = "stopping"
            elif failed:
                word = "failed"
            else:
                word = "starting"
            conditions = _conditions(
                status=word,
                phase=phase,
                clock=clock,
                progress=progress,
                feeds=feeds_box[0],
                availability=availability_box[0],
            )
            dropped, drop_fields = drop_status(ingress)
            conditions.update(drop_fields)
            snapshot = StsSessionStatus(
                session_id=request.session_id,
                status=word,
                strategy=request.strategy,
                reason=reason,
                created_by=request.created_by,
                type=request.type,
                conditions=conditions,
                progress=StsStatusProgress(dropped=dropped),
            )
            return Envelope[StsSessionStatus].wrap(
                snapshot,
                type=STS_SESSION_STATUS,
                source=_SOURCE,
                session_id=request.session_id,
            )
        return None

    from mftik.broker.handler import serve

    async def _inbox(ready: asyncio.Event) -> None:
        assert clock is not None
        async for subject, raw in ingress_broker.iter_core(
            inbox_wild, stop=ingress_stop, ready=ready
        ):
            if not _pv_ok(raw):
                continue
            request_id = subject.rsplit(".", 1)[-1]
            event = _inbound(
                raw,
                feed="rpc",
                kind=StreamKind.RPC_REPLY,
                recv_ts=clock.now(),
                clock=clock,
            )
            if event is not None:
                ingress.log_only(event)
            pending.complete(request_id, raw, now=clock.monotonic())

    async def _pump_raw(
        topics: list[str],
        *,
        ready: asyncio.Event,
        on_message: Callable[[str, str], None],
    ) -> None:
        if not topics:
            ready.set()
            return
        # A wildcard is a pattern (``md.w.*.*``). Fan-out topics refuse
        # ``*``; patterns are the subscription that allows one per segment.
        if any("*" in topic or ">" in topic for topic in topics):
            stream = ingress_broker.iter_patterns(
                topics, stop=ingress_stop, ready=ready
            )
        else:
            stream = ingress_broker.iter_raw(
                topics, stop=ingress_stop, ready=ready
            )
        async for topic, raw in stream:
            on_message(topic, raw)

    def _notice_event(effect: MdUpdate | TdUpdate) -> Inbound:
        assert clock is not None
        if isinstance(effect, MdUpdate):
            payload: dict[str, Any] = {
                "feed": effect.feed,
                "state": effect.state,
                "reason": effect.reason,
                "token": effect.token,
            }
            kind = StreamKind.MD_NOTICE
            feed = effect.feed
        else:
            payload = {
                "api_id": effect.api_id,
                "state": effect.state,
                "reason": effect.reason,
                "token": effect.token,
            }
            kind = StreamKind.TD_NOTICE
            feed = f"td.{effect.api_id}"
        return Inbound(
            kind=kind,
            feed=feed,
            recv_ts=clock.now(),
            body=json.dumps(payload).encode(),
            event_id=uuid.uuid4().hex,
            clock=clock.now,
        )

    def _emit(effects: list[Any]) -> None:
        """Queue notices. A settled resync read is a task, not this call."""

        def _offer(effect: MdUpdate | TdUpdate) -> None:
            ingress.offer(_notice_event(effect))

        async def _warn(effect: MdUpdate | TdUpdate) -> None:
            await _log(notice_text(effect), level="warning")

        schedule_effects(
            effects,
            offer=_offer,
            deliver=_deliver_resync,
            log=_warn,
        )

    async def _deliver_resync(effect: Resync) -> None:
        assert clock is not None
        view = await read_oms_view(
            ingress_broker,
            api_id=effect.api_id,
            session_id=request.session_id,
        )
        if view is None:
            logger.warning(
                "on_resync skipped api_id=%s cause=%s",
                effect.api_id,
                effect.cause,
            )
        else:
            payload = {
                "api_id": effect.api_id,
                "cause": effect.cause,
                "view": view.model_dump(mode="json"),
            }
            ingress.offer(
                Inbound(
                    kind=StreamKind.RESYNC,
                    feed=f"td.{effect.api_id}",
                    recv_ts=clock.now(),
                    body=json.dumps(payload).encode(),
                    event_id=uuid.uuid4().hex,
                    clock=clock.now,
                )
            )
        if effect.then_td is not None:
            ingress.offer(_notice_event(effect.then_td))
            await _log(notice_text(effect.then_td), level="warning")

    def bind_availability(tracker: Availability) -> None:
        availability_box[0] = tracker
        if not ingress_flags["down"]:
            return

        def _down() -> None:
            _emit(tracker.ingress_disconnected())

        loop.call_soon_threadsafe(_down)

    async def _nats_down() -> None:
        # Closing the broker at teardown also drops the socket. That is
        # not an ingress reconnect the strategy still has to hear.
        if ingress_stop.is_set() or finished.is_set():
            return
        ingress_flags["down"] = True
        tracker = availability_box[0]
        if tracker is not None:
            _emit(tracker.ingress_disconnected())

    async def _nats_up() -> None:
        if ingress_stop.is_set() or finished.is_set():
            return
        ingress_flags["down"] = False
        tracker = availability_box[0]
        if tracker is None or clock is None:
            return
        _emit(tracker.ingress_reconnected(clock.monotonic()))

    ingress_broker.set_reconnect_handlers(
        disconnected=_nats_down,
        reconnected=_nats_up,
    )

    inbox_ready = asyncio.Event()
    tasks: list[asyncio.Task[Any]] = [
        asyncio.create_task(_heartbeat(), name="sts-heartbeat"),
        asyncio.create_task(_watch(), name="sts-watch"),
        asyncio.create_task(_inbox(inbox_ready), name="sts-inbox"),
        asyncio.create_task(
            serve(
                ingress_broker,
                Topics.sts_control(request.session_id),
                _on_ctl,
                stop=ingress_stop,
            ),
            name="sts-ctl",
        ),
        asyncio.create_task(
            _status_until_stopped(_publish_status, ingress_stop, clock),
            name="sts-status",
        ),
    ]

    def start_pump(
        topics: list[str], on_message: Callable[[str, str], None]
    ) -> concurrent.futures.Future[None]:
        """Start a fan-out pump on the ingress loop. Done when the SUB is flushed."""
        done: concurrent.futures.Future[None] = concurrent.futures.Future()

        def _start() -> None:
            ready = asyncio.Event()
            pump = asyncio.create_task(
                _pump_raw(topics, ready=ready, on_message=on_message),
                name="sts-pump",
            )
            tasks.append(pump)

            async def _wait_ready() -> None:
                await ready.wait()

            waiter = asyncio.create_task(_wait_ready())

            def _finish(task: asyncio.Task[None]) -> None:
                if done.done():
                    return
                if task.cancelled():
                    done.cancel()
                    return
                exc = task.exception()
                if exc is not None:
                    done.set_exception(exc)
                else:
                    done.set_result(None)

            waiter.add_done_callback(_finish)

            def _pump_ended(task: asyncio.Task[None]) -> None:
                if task.cancelled():
                    return
                exc = task.exception()
                if exc is None:
                    return
                if not done.done():
                    done.set_exception(exc)
                if not ready.is_set():
                    ready.set()

            pump.add_done_callback(_pump_ended)

        loop.call_soon_threadsafe(_start)
        return done

    await inbox_ready.wait()

    if install_signals:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, request_stop)

    def _thread() -> None:
        try:
            box["code"] = asyncio.run(
                _strategy(
                    request,
                    ingress=ingress,
                    clock=clock,
                    pending=pending,
                    inbox_for=inbox_for,
                    progress=progress,
                    hooks=hooks,
                    stop=stop,
                    cancel_hooks=cancel_hooks,
                    feeds_box=feeds_box,
                    strategy_loop=strategy_loop,
                    wake_strategy=wake_strategy,
                    start_pump=start_pump,
                    log=_log,
                    emit=_emit,
                    bind_availability=bind_availability,
                )
            )
        except Exception:
            logger.exception("strategy thread failed session=%s", request.session_id)
            progress.fail("strategy thread failed")
            box["code"] = 1
        finally:
            loop.call_soon_threadsafe(finished.set)
            loop.call_soon_threadsafe(ingress_stop.set)

    thread = threading.Thread(target=_thread, name="mftik-strategy", daemon=True)
    thread.start()
    await finished.wait()
    hb_stop.set()
    ingress_stop.set()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    code = box["code"]
    failed, reason, _terminal = progress.snapshot_bits()
    if failed or code:
        code = code or 1
        progress.terminal = "failed"
        if reason:
            logger.error(
                "session %s failed: %s", request.session_id, reason
            )
            await _log(reason, level="error")
    else:
        progress.terminal = "done"
    await _publish_status(status=progress.terminal)
    ingress.set_exit_code(code)
    runner = ingress._runner  # noqa: SLF001
    if runner is not None and getattr(runner, "alive", False):
        kill = getattr(runner, "kill", None)
        if kill is not None:
            kill()
    try:
        ingress.close()
    except Exception:
        logger.exception("ingress close failed session=%s", request.session_id)
    pending.cancel_all(RequestTimeoutError("shutdown", "shutdown", 0.0))
    try:
        await ingress_broker.close()
    except Exception:
        logger.exception("ingress broker close failed session=%s", request.session_id)
    return 0 if ingress.exit_code == 0 else (ingress.exit_code or code)


async def _status_until_stopped(
    publish: Callable[..., Any],
    stop: asyncio.Event,
    clock: Clock,
) -> None:
    while not stop.is_set():
        await publish()
        sleep = asyncio.create_task(clock.sleep(HEARTBEAT_PERIOD_S))
        waiter = asyncio.create_task(stop.wait())
        _done, pending = await asyncio.wait(
            {sleep, waiter}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()


def _conditions(
    *,
    status: str,
    phase: Phase | None,
    clock: Clock,
    progress: _Progress,
    feeds: FeedReady | None,
    availability: Availability | None = None,
) -> dict[str, str]:
    out = {"phase": status}
    if phase is Phase.ON_START and progress.on_start_at is not None:
        elapsed = clock.monotonic() - progress.on_start_at
        out["on_start"] = f"running {elapsed:.0f}s"
    if phase in (Phase.RUNNING, Phase.STOPPING) and availability is not None:
        ready, total = availability.md_ready_counts()
        if total:
            out["MdReady"] = f"{ready}/{total}"
        td_ready, td_total = availability.td_ready_counts()
        if td_total:
            out["TdReady"] = f"{td_ready}/{td_total}"
        return out
    if feeds is not None:
        ready, total = feeds.counts()
        if total:
            out["MdReady"] = f"{ready}/{total}"
    if progress.td_waiting is not None:
        out["TdReady"] = f"waiting for account {progress.td_waiting}"
    elif progress.td_total:
        out["TdReady"] = f"{progress.td_ready}/{progress.td_total}"
    return out


def _noop() -> None:
    return None


async def _strategy(
    request: StsCreateSessionRequest,
    *,
    ingress: Ingress,
    clock: Clock,
    pending: PendingTable,
    inbox_for: Callable[[str], str],
    progress: _Progress,
    hooks: _Hooks,
    stop: threading.Event,
    cancel_hooks: threading.Event,
    feeds_box: list[FeedReady | None],
    strategy_loop: list[asyncio.AbstractEventLoop | None],
    wake_strategy: list[Callable[[], None]],
    start_pump: Callable[
        [list[str], Callable[[str, str], None]], concurrent.futures.Future[None]
    ],
    log: Callable[..., Any],
    emit: Callable[[list[Any]], None],
    bind_availability: Callable[[Availability], None],
) -> int:
    loop = asyncio.get_running_loop()
    strategy_loop[0] = loop
    wake = asyncio.Event()

    def _wake() -> None:
        loop.call_soon_threadsafe(wake.set)

    wake_strategy.append(_wake)
    send = Broker()
    event_log: EventLog | None = None
    runner: StrategyRunner | None = None
    strategy: Strategy | None = None
    code = 1
    try:
        await send.connect()
        broker = CrossThreadBroker(send, pending, inbox_for, clock)
        event_log = EventLog.from_env(request.session_id)
        await event_log.start()
        session = WorkerSession(
            request,
            broker=broker,
            event_log=event_log,
            symbols=SymbolClient(broker),
        )
        session._wake = _wake  # noqa: SLF001
        try:
            strategy = load_strategy(request.strategy)
        except KeyError as exc:
            progress.fail(f"unknown strategy: {exc}")
            await log(
                progress.snapshot_bits()[1] or "unknown strategy", level="error"
            )
            return 1
        session.strategy = strategy
        strategy.bind(session)
        strategy.paras = type(strategy).on_initialized(session.st_paras)
        runner = StrategyRunner(ingress, strategy)
        runner.start()
        session.order_phase = "load"
        code = await _run(
            request,
            ingress=ingress,
            runner=runner,
            strategy=strategy,
            session=session,
            clock=clock,
            progress=progress,
            hooks=hooks,
            stop=stop,
            cancel_hooks=cancel_hooks,
            wake=wake,
            feeds_box=feeds_box,
            start_pump=start_pump,
            log=log,
            broker=broker,
            emit=emit,
            bind_availability=bind_availability,
        )
    finally:
        if runner is not None and runner.alive:
            runner.finish()
        if strategy is not None:
            try:
                strategy.timer.close()
            except Exception:
                logger.exception(
                    "timer close failed session=%s", request.session_id
                )
        if event_log is not None:
            await event_log.close()
        await send.close()
    return code


async def _run(
    request: StsCreateSessionRequest,
    *,
    ingress: Ingress,
    runner: StrategyRunner,
    strategy: Strategy,
    session: WorkerSession,
    clock: Clock,
    progress: _Progress,
    hooks: _Hooks,
    stop: threading.Event,
    cancel_hooks: threading.Event,
    wake: asyncio.Event,
    feeds_box: list[FeedReady | None],
    start_pump: Callable[
        [list[str], Callable[[str, str], None]], concurrent.futures.Future[None]
    ],
    log: Callable[..., Any],
    broker: CrossThreadBroker,
    emit: Callable[[list[Any]], None],
    bind_availability: Callable[[Availability], None],
) -> int:
    resolved, missing = resolve_feeds(list(session.md_ids))
    tracker = Availability(
        feeds={
            feed.feed: frozenset(atom.atom_id for atom in feed.atoms)
            for feed in resolved
        },
        accounts=session.td_api_ids,
    )
    session.availability = tracker
    bind_availability(tracker)
    feeds = FeedReady(resolved, missing)
    feeds_box[0] = feeds
    topics: list[str] = []
    seen: set[str] = set()
    silent: list[str] = []
    subject_feed: dict[str, str] = {}
    for feed in resolved:
        for atom in feed.atoms:
            subject_feed[atom.subject] = feed.feed
            if atom.policy is JoinPolicy.SILENT:
                silent.append(atom.atom_id)
            if atom.subject not in seen:
                seen.add(atom.subject)
                topics.append(atom.subject)

    def _on_md(topic: str, raw: str) -> None:
        if topic.startswith("md.w."):
            if _pv_ok(raw):
                emit(tracker.apply_md(topic, raw, clock.monotonic()))
            return
        if not _pv_ok(raw):
            return
        feed = feeds.note_event(topic) or subject_feed.get(topic) or topic
        kind = _kind_from_raw(raw)
        if kind is None:
            return
        event = _inbound(
            raw, feed=feed, kind=kind, recv_ts=clock.now(), clock=clock
        )
        if event is not None:
            ingress.offer(event)

    if topics:
        await asyncio.wrap_future(start_pump(topics, _on_md))
        await asyncio.wrap_future(
            start_pump([Topics.md_worker_pattern()], _on_md)
        )
        for atom_id in silent:
            feeds.note_subscribed(atom_id)

    session.order_phase = "on_start"
    runner.begin_on_start()
    progress.on_start_at = clock.monotonic()
    held = await _call_hook(
        strategy.on_start(),
        name="on_start",
        limit_s=DEFAULT_START_TIMEOUT_S,
        hooks=hooks,
        clock=clock,
        cancel_hooks=cancel_hooks,
        stop=stop,
        session=session,
        cancel_on_stop=True,
    )
    if not held or stop.is_set() or session.exit_requested or progress.failed:
        return await _stop(
            strategy=strategy,
            session=session,
            ingress=ingress,
            runner=runner,
            hooks=hooks,
            clock=clock,
            progress=progress,
            cancel_hooks=cancel_hooks,
            wake=wake,
            log=log,
        )

    runner.end_on_start()
    api_ids = list(session.td_api_ids)
    progress.td_total = len(api_ids)
    if api_ids:
        progress.td_waiting = api_ids[0]
        td_topics = [Topics.td_global(api_id) for api_id in api_ids]

        def _on_td(topic: str, raw: str) -> None:
            if not _pv_ok(raw):
                return
            effects = tracker.apply_td_global(raw, clock.monotonic())
            if effects is not None:
                emit(effects)
                return
            api_id = _api_id_from_td_topic(topic)
            event = _inbound(
                raw,
                feed=f"td.{api_id}",
                kind=StreamKind.TD,
                recv_ts=clock.now(),
                clock=clock,
            )
            if event is not None:
                ingress.offer(event)

        await asyncio.wrap_future(start_pump(td_topics, _on_td))

        def _on_td_state(_topic: str, raw: str) -> None:
            if not _pv_ok(raw):
                return
            emit(tracker.apply_td_state(raw, clock.monotonic()))

        state_topics = [Topics.td_account_state(api_id) for api_id in api_ids]
        await asyncio.wrap_future(start_pump(state_topics, _on_td_state))

    deadline = clock.monotonic() + DEFAULT_READY_TIMEOUT_S
    td_ok = await _td_ready(
        session, api_ids, deadline=deadline, clock=clock, progress=progress
    )
    while feeds.unresolved_pending() and clock.monotonic() < deadline and td_ok:
        if stop.is_set() or session.exit_requested or progress.failed:
            break
        try:
            await asyncio.wait_for(wake.wait(), timeout=_POLL_S)
        except TimeoutError:
            pass
        wake.clear()
    if not td_ok and not progress.failed:
        waiting = progress.td_waiting
        progress.fail(
            "TdReady not met within ready_timeout_s"
            + (f" (account {waiting})" if waiting is not None else "")
        )
        await log(progress.snapshot_bits()[1] or "TdReady", level="error")
    if stop.is_set() or session.exit_requested or progress.failed or not td_ok:
        return await _stop(
            strategy=strategy,
            session=session,
            ingress=ingress,
            runner=runner,
            hooks=hooks,
            clock=clock,
            progress=progress,
            cancel_hooks=cancel_hooks,
            wake=wake,
            log=log,
        )
    progress.td_waiting = None
    progress.td_ready = progress.td_total
    if feeds.missing_feeds():
        await log(
            "MdReady incomplete: " + ", ".join(feeds.missing_feeds()),
            level="warning",
        )

    session.order_phase = "ready"
    runner.begin_on_ready()
    ready_report = Ready(missing_feeds=feeds.missing_feeds())
    held = await _call_hook(
        strategy.on_ready(ready_report),
        name="on_ready",
        limit_s=ON_READY_LIMIT_S,
        hooks=hooks,
        clock=clock,
        cancel_hooks=cancel_hooks,
        stop=stop,
        session=session,
        cancel_on_stop=True,
    )
    if not held or progress.failed:
        return await _stop(
            strategy=strategy,
            session=session,
            ingress=ingress,
            runner=runner,
            hooks=hooks,
            clock=clock,
            progress=progress,
            cancel_hooks=cancel_hooks,
            wake=wake,
            log=log,
        )
    if session.exit_requested or stop.is_set():
        return await _stop(
            strategy=strategy,
            session=session,
            ingress=ingress,
            runner=runner,
            hooks=hooks,
            clock=clock,
            progress=progress,
            cancel_hooks=cancel_hooks,
            wake=wake,
            log=log,
        )

    runner.end_on_ready()
    session.order_phase = "running"
    while not stop.is_set() and not session.exit_requested and not progress.failed:
        event = ingress.pull()
        if event is None:
            try:
                await asyncio.wait_for(wake.wait(), timeout=_POLL_S)
            except TimeoutError:
                pass
            wake.clear()
            continue
        try:
            await _dispatch(strategy, session.event_log, event, clock=clock)
        except Exception as exc:
            progress.fail(f"strategy_exception: {exc}")
            logger.exception("strategy hook failed session=%s", request.session_id)
            break
    if session.exit_failed:
        progress.fail(session.exit_reason or "strategy_exit")
    elif session.exit_reason:
        progress.note_reason(session.exit_reason)
    return await _stop(
        strategy=strategy,
        session=session,
        ingress=ingress,
        runner=runner,
        hooks=hooks,
        clock=clock,
        progress=progress,
        cancel_hooks=cancel_hooks,
        wake=wake,
        log=log,
    )


async def _stop(
    *,
    strategy: Strategy,
    session: WorkerSession,
    ingress: Ingress,
    runner: StrategyRunner,
    hooks: _Hooks,
    clock: Clock,
    progress: _Progress,
    cancel_hooks: threading.Event,
    wake: asyncio.Event,
    log: Callable[..., Any],
) -> int:
    # A lifecycle deadline sets this so the hook that overran is cancelled.
    # ``on_stop`` gets its own deadline from the watch, not that flag.
    cancel_hooks.clear()
    if session.exit_failed:
        progress.fail(session.exit_reason or "strategy_exit")
    session.order_phase = "stopping"
    if ingress.phase is not Phase.STOPPING:
        ingress.stop()
    if runner.alive:
        runner.begin_on_stop()
    started = clock.monotonic()
    hooks.arm("on_stop", started=started, deadline=started + ON_STOP_LIMIT_S)
    task = asyncio.create_task(strategy.on_stop())
    try:
        while not task.done():
            event = ingress.pull()
            if event is not None:
                try:
                    await _dispatch(
                        strategy, session.event_log, event, clock=clock
                    )
                except Exception:
                    logger.exception("dispatch during on_stop failed")
                continue
            if hooks.breached(clock.monotonic()) or cancel_hooks.is_set():
                progress.fail("on_stop exceeded its limit")
                task.cancel()
                break
            try:
                await asyncio.wait_for(wake.wait(), timeout=_POLL_S)
            except TimeoutError:
                pass
            wake.clear()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            progress.fail(f"on_stop failed: {exc}")
            logger.exception("on_stop failed")
    finally:
        hooks.clear()
        if runner.alive:
            runner.finish()
    failed, reason, _terminal = progress.snapshot_bits()
    if failed and reason:
        await log(reason, level="error")
    # A hook that raised after ``on_ready`` returned. ``on_stop`` has
    # already been attempted above. The controller maps this code to
    # class A. Every other failure stays 1 (class C). The blocked-hook
    # code is B5-04 and is not produced here.
    if failed and reason is not None and reason.startswith("strategy_exception:"):
        return STRATEGY_EXCEPTION
    return 1 if failed else 0


async def _call_hook(
    coro: Any,
    *,
    name: str,
    limit_s: float,
    hooks: _Hooks,
    clock: Clock,
    cancel_hooks: threading.Event,
    stop: threading.Event,
    session: WorkerSession,
    cancel_on_stop: bool,
) -> bool:
    """Run ``coro``. False means it was cancelled or it raised.

    A raise is recorded on ``cancel_hooks``'s sibling progress by the
    caller only when this returns False and the task's exception was a
    strategy error. Cancellation is a stop, not by itself a failure.
    """
    started = clock.monotonic()
    hooks.arm(name, started=started, deadline=started + limit_s)
    task = asyncio.create_task(coro)
    raised = False
    try:
        while not task.done():
            if cancel_hooks.is_set() or (
                cancel_on_stop and (stop.is_set() or session.exit_requested)
            ):
                task.cancel()
                break
            await asyncio.wait({task}, timeout=_POLL_S)
        try:
            await task
        except asyncio.CancelledError:
            return False
        except Exception as exc:
            raised = True
            logger.exception("%s failed", name)
            session.request_exit(f"{name} failed: {exc}", failed=True)
            return False
        return not raised
    finally:
        hooks.clear()


def _pv_ok(raw: str) -> bool:
    """False when ``pv`` is wrong, or the frame is not a JSON object.

    A bad frame must not take down the pump. The mismatch itself is
    dropped; this process does not invent an error type for it.
    """
    try:
        return reject_if_pv_mismatch(raw) is None
    except ValueError:
        return False


def _kind_from_raw(raw: str) -> StreamKind | None:
    try:
        header = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(header, dict):
        return None
    env_type = header.get("type")
    if not isinstance(env_type, str):
        return None
    return _kind_of(env_type)


def _api_id_from_td_topic(topic: str) -> int:
    # ``td.{api_id}.global``
    parts = topic.split(".")
    if len(parts) >= 2 and parts[0] == "td":
        return int(parts[1])
    raise ValueError(f"not a td global topic: {topic}")


async def _td_ready(
    session: WorkerSession,
    api_ids: list[int],
    *,
    deadline: float,
    clock: Clock,
    progress: _Progress,
) -> bool:
    if not api_ids:
        return True

    async def _one(api_id: int, type_name: str, model: type, payload: Any) -> bool:
        progress.td_waiting = api_id
        remaining = deadline - clock.monotonic()
        if remaining <= 0:
            return False
        envelope = Envelope.wrap(
            payload,
            type=type_name,
            source=_SOURCE,
            session_id=session.session_id,
        )
        try:
            reply = await session.broker.request(
                Topics.td_account(api_id), envelope, timeout=remaining
            )
        except RequestTimeoutError:
            return False
        try:
            model.model_validate(reply.payload)
        except Exception:
            logger.exception(
                "td view unreadable api_id=%s type=%s", api_id, type_name
            )
            return False
        return True

    tasks = []
    for api_id in api_ids:
        tasks.append(
            _one(
                api_id,
                TD_OMS_VIEW,
                OmsView,
                TdOmsViewRequest(api_id=api_id, settled=False),
            )
        )
        tasks.append(
            _one(
                api_id,
                TD_LEDGER_VIEW,
                LedgerView,
                TdLedgerViewRequest(api_id=api_id),
            )
        )
    results = await asyncio.gather(*tasks)
    return all(results)


async def _dispatch(
    strategy: Strategy,
    event_log: EventLog,
    event: Inbound,
    *,
    clock: Clock,
) -> None:
    if event.kind in (
        StreamKind.MD_NOTICE,
        StreamKind.TD_NOTICE,
        StreamKind.RESYNC,
    ):
        await dispatch_notice(strategy, event_log, event, swallow=False)
        return
    env = UntypedEnvelope.from_json(event.body.decode())
    if event.kind is StreamKind.TD:
        api_text = event.feed.split(".", 1)[1]
        await dispatch_td(
            strategy,
            event_log,
            int(api_text),
            env,
            swallow=False,
            delivery=event,
            clock=clock.now,
        )
        return
    await dispatch_md(
        strategy,
        event_log,
        env,
        swallow=False,
        delivery=event,
        clock=clock.now,
    )


def main(argv: list[str] | None = None) -> int:
    """Process entry. ``argv`` is the request path, without ``sys.argv[0]``."""
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        sys.stderr.write(
            "usage: python -m mftik_sts.session_worker <request.json>\n"
        )
        return 2
    try:
        raw = Path(args[0]).read_text(encoding="utf-8")
        request = StsCreateSessionRequest.model_validate_json(raw)
    except Exception as exc:
        sys.stderr.write(f"session request: {exc}\n")
        return 2
    _arm_pdeathsig()
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass
    return asyncio.run(amain(request, install_signals=True))

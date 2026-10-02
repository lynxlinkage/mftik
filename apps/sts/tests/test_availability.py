"""§5.6 availability: one test per situation, plus the order and buffer rules.

Time is a :class:`~mftik.clock.FakeClock`. Nothing here sleeps or opens
NATS. Publishers are stand-ins: the broadcasts are envelopes this file
builds, which is what the ingress will receive once B8-06 and B6-06
publish them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from decimal import Decimal
from types import SimpleNamespace

import pytest
from mftik.clock import FakeClock
from mftik.exchange.models import FeedEnd, OrderType, Side
from mftik.exchange.oms import OmsView
from mftik.protocol import (
    MD_ATOM_STATE,
    MD_FEED_END,
    MD_WORKER_STATE,
    TD_ACCOUNT_RESET,
    TD_ACCOUNT_STATE,
    TD_ERROR,
    TD_OMS_VIEW,
    TD_ORDER_ACK,
    Envelope,
    MdAtomState,
    MdWorkerState,
    OrderAck,
    RejectCode,
    StsCreateSessionRequest,
    TdAccountReset,
    TdAccountState,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik.strategy.eventlog import EventLog
from mftik.strategy.oms import SETTLED_VIEW_TIMEOUT_S
from mftik_sts.session_worker.availability import (
    REASON_INGRESS,
    REASON_SILENCE,
    SILENCE_S,
    Availability,
    MdUpdate,
    Resync,
    TdUpdate,
    read_oms_view,
    schedule_effects,
)
from mftik_sts.session_worker.delivery import MUST_DELIVER
from mftik_sts.session_worker.dispatch import dispatch_md, dispatch_notice
from mftik_sts.session_worker.events import Inbound, StreamKind
from mftik_sts.session_worker.ingress import Ingress
from mftik_sts.session_worker.phase import Phase
from mftik_sts.session_worker.process import _conditions, _Progress
from mftik_sts.session_worker.runner import StrategyRunner

_FEED = "orderbook.Paper_Spot_BTCUSDT"
_ATOM = "paper:ob:btc"
_SUBJECT = "md.w.paper.w1"
_LOG = "mftik_sts.session_worker.availability"


def _tracker(
    feeds: dict[str, frozenset[str]] | None = None,
    accounts: tuple[int, ...] = (7,),
) -> Availability:
    return Availability(
        feeds=feeds if feeds is not None else {_FEED: frozenset({_ATOM})},
        accounts=accounts,
    )


def _worker(state: str, *, version: int, incarnation: int = 1) -> str:
    return Envelope.wrap(
        MdWorkerState(
            instance="paper",
            worker_id="w1",
            incarnation=incarnation,
            state=state,
            version=version,
        ),
        type=MD_WORKER_STATE,
        source="md",
    ).to_json()


def _atom(
    atom_id: str = _ATOM,
    *,
    version: int,
    incarnation: int = 1,
    state: str = "subscribed",
    error: str | None = None,
) -> str:
    return Envelope.wrap(
        MdAtomState(
            atom_id=atom_id,
            state=state,
            version=version,
            incarnation=incarnation,
            error=error,
        ),
        type=MD_ATOM_STATE,
        source="md",
    ).to_json()


def _account(
    state: str,
    *,
    version: int,
    incarnation: int = 1,
    api_id: int = 7,
    reason: str = "",
) -> str:
    return Envelope.wrap(
        TdAccountState(
            api_id=api_id,
            incarnation=incarnation,
            state=state,  # type: ignore[arg-type]
            version=version,
            reason=reason,
        ),
        type=TD_ACCOUNT_STATE,
        source="td",
    ).to_json()


def _reset(incarnation: int, *, api_id: int = 7) -> str:
    return Envelope.wrap(
        TdAccountReset(api_id=api_id, incarnation=incarnation),
        type=TD_ACCOUNT_RESET,
        source="td",
    ).to_json()


def _kinds(effects: list[object]) -> list[str]:
    out: list[str] = []
    for effect in effects:
        if isinstance(effect, Resync):
            out.append(f"resync:{effect.cause}")
        elif isinstance(effect, MdUpdate):
            out.append(f"md:{effect.state}:{effect.reason}")
        elif isinstance(effect, TdUpdate):
            word = "later" if effect.deferred else "now"
            out.append(f"td:{effect.state}:{effect.reason}:{word}")
    return out


class _Sees(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.ended: list[FeedEnd] = []
        self.md_updates: list[tuple[str, str, str]] = []
        self.td_updates: list[tuple[int, str, str]] = []
        self.resyncs: list[tuple[int, str]] = []

    async def on_feed_end(self, event: FeedEnd) -> None:
        self.ended.append(event)

    async def on_md_update(self, feed: str, state: str, reason: str) -> None:
        self.md_updates.append((feed, state, reason))

    async def on_td_update(self, api_id: int, state: str, reason: str) -> None:
        self.td_updates.append((api_id, state, reason))

    async def on_resync(self, api_id: int, cause: str, view: OmsView) -> None:
        self.resyncs.append((api_id, cause))


def test_md_worker_crash_and_reconnect_notifies_without_failing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Exchange disconnect and a worker restart are notifications only."""
    clock = FakeClock()
    tracker = _tracker()
    with caplog.at_level(logging.WARNING, logger=_LOG):
        live = tracker.apply_md(_SUBJECT, _atom(version=1), clock.monotonic())
        live += tracker.apply_md(
            _SUBJECT, _worker("live", version=1), clock.monotonic()
        )
    assert _kinds(live) == ["md:live:live"]
    assert tracker.md_state(_FEED) == "live"

    down = tracker.apply_md(
        _SUBJECT,
        _atom(version=2, error="venue socket closed"),
        clock.monotonic(),
    )
    assert _kinds(down) == ["md:down:venue socket closed"]
    assert tracker.md_state(_FEED) == "down"

    back = tracker.apply_md(_SUBJECT, _atom(version=3), clock.monotonic())
    assert _kinds(back) == ["md:live:live"]

    crashed = tracker.apply_md(
        _SUBJECT, _worker("down", version=2), clock.monotonic()
    )
    assert _kinds(crashed) == ["md:down:down"]
    restored = tracker.apply_md(
        _SUBJECT, _worker("live", version=3), clock.monotonic()
    )
    assert _kinds(restored) == ["md:live:live"]
    assert tracker.md_state(_FEED) == "live"


async def test_feed_end_is_a_hook_and_does_not_restore_availability() -> None:
    """A terminal feed is ``on_feed_end``. It is not an availability broadcast."""
    clock = FakeClock()
    tracker = _tracker()
    tracker.apply_md(_SUBJECT, _atom(version=1, error="closed"), clock.monotonic())
    assert tracker.md_state(_FEED) == "down"
    end = Envelope.wrap(
        FeedEnd(
            universal_ticker="Paper_Spot_BTCUSDT",
            topic="orderbook",
            state="expired",
            code="expired",
            reason="listed settlement",
        ),
        type=MD_FEED_END,
        source="md",
    ).to_json()
    assert tracker.apply_md(_SUBJECT, end, clock.monotonic()) == []
    assert tracker.md_state(_FEED) == "down"

    strategy = _Sees()
    await dispatch_md(
        strategy,
        EventLog("abc123", directory=None),
        UntypedEnvelope.from_json(end),
        swallow=False,
    )
    assert len(strategy.ended) == 1
    assert strategy.ended[0].code == "expired"
    assert strategy.md_updates == []


def test_td_worker_restart_is_unavailable_then_resync_then_ready() -> None:
    """New incarnation while still unavailable waits for the reset.

    Announcing ready from a broadcast that says unavailable would let the
    strategy trade. ``TdAccountReset`` (or a later ready) is what finishes
    the sequence: unavailable, then on_resync, then ready.
    """
    clock = FakeClock()
    tracker = _tracker()
    first = tracker.apply_td_state(
        _account("ready", version=1, incarnation=1), clock.monotonic()
    )
    assert _kinds(first) == ["td:ready:ready:now"]
    assert tracker.td_state(7) == "ready"

    restart = tracker.apply_td_state(
        _account("unavailable", version=2, incarnation=2, reason="worker restart"),
        clock.monotonic(),
    )
    assert _kinds(restart) == ["td:unavailable:worker restart:now"]
    assert tracker.td_state(7) == "unavailable"

    finished = tracker.apply_td_global(_reset(2), clock.monotonic())
    assert finished is not None
    assert _kinds(finished) == ["resync:account_reset"]
    resync = finished[0]
    assert isinstance(resync, Resync)
    assert resync.then_td is not None
    assert resync.then_td.deferred
    assert tracker.td_state(7) == "unavailable"
    assert tracker.commit_td(7, resync.then_td.state, resync.then_td.token)
    assert tracker.td_state(7) == "ready"

    assert tracker.apply_td_global(_reset(2), clock.monotonic()) == []


def test_a_new_incarnation_that_is_already_ready_resyncs_immediately() -> None:
    """The unavailable publication was missed. The snapshot is the full state."""
    clock = FakeClock()
    tracker = _tracker()
    tracker.apply_td_state(
        _account("ready", version=1, incarnation=1), clock.monotonic()
    )
    effects = tracker.apply_td_state(
        _account("ready", version=3, incarnation=2), clock.monotonic()
    )
    assert _kinds(effects) == [
        "td:unavailable:ready:now",
        "resync:account_reset",
    ]
    resync = effects[1]
    assert isinstance(resync, Resync)
    assert resync.then_td is not None
    assert tracker.td_state(7) == "unavailable"
    tracker.commit_td(7, "ready", resync.then_td.token)
    assert tracker.td_state(7) == "ready"


def test_private_connection_loss_is_degraded_then_ready_without_resync() -> None:
    clock = FakeClock()
    tracker = _tracker()
    tracker.apply_td_state(
        _account("ready", version=1, incarnation=1), clock.monotonic()
    )
    degraded = tracker.apply_td_state(
        _account("degraded", version=2, reason="private stream dropped"),
        clock.monotonic(),
    )
    assert _kinds(degraded) == ["td:degraded:private stream dropped:now"]
    assert all(not isinstance(effect, Resync) for effect in degraded)
    ready = tracker.apply_td_state(
        _account("ready", version=3, reason="private stream restored"),
        clock.monotonic(),
    )
    assert _kinds(ready) == ["td:ready:private stream restored:now"]
    assert tracker.td_state(7) == "ready"


def test_a_controller_restart_broadcast_changes_nothing() -> None:
    clock = FakeClock()
    tracker = _tracker()
    tracker.apply_md(_SUBJECT, _atom(version=1), clock.monotonic())
    assert tracker.md_state(_FEED) == "live"
    foreign = Envelope.wrap(
        {"status": "rolling"},
        type="md.controller.status",
        source="md",
    ).to_json()
    assert tracker.apply_md(_SUBJECT, foreign, clock.monotonic()) == []
    assert tracker.md_state(_FEED) == "live"
    assert (
        tracker.apply_td_state(
            Envelope.wrap(
                {"status": "rolling"},
                type="td.controller.status",
                source="td",
            ).to_json(),
            clock.monotonic(),
        )
        == []
    )
    assert tracker.td_state(7) is None


def test_ingress_reconnect_takes_feeds_down_then_live_and_resyncs() -> None:
    """Accounts are not marked unavailable: the send connection is not this one.

    While the ingress is down the silence timer does not also fire, and
    reconnect restarts the clocks so the gap we could not hear is not a
    false ``broadcast_silent``.
    """
    clock = FakeClock()
    feeds = {
        "orderbook.Paper_Spot_BTCUSDT": frozenset({"a"}),
        "ticker.Paper_Spot_BTCUSDT": frozenset({"b"}),
    }
    tracker = _tracker(feeds, accounts=(7, 8))
    tracker.apply_md("md.w.paper.w1", _atom("a", version=1), clock.monotonic())
    down = tracker.ingress_disconnected()
    assert _kinds(down) == [
        f"md:down:{REASON_INGRESS}",
        f"md:down:{REASON_INGRESS}",
    ]
    assert tracker.ingress_disconnected() == []
    clock.advance(SILENCE_S + 5)
    assert tracker.tick(clock.monotonic()) == []
    assert tracker.td_state(7) is None

    clock.advance(1)
    up = tracker.ingress_reconnected(clock.monotonic())
    assert [effect.cause for effect in up if isinstance(effect, Resync)] == [
        "reconnect",
        "reconnect",
    ]
    assert tracker.td_state(7) is None
    assert tracker.td_state(8) is None
    lives = [effect for effect in up if isinstance(effect, MdUpdate)]
    assert {effect.state for effect in lives} == {"live"}
    assert {effect.reason for effect in lives} == {REASON_INGRESS}
    assert tracker.ingress_reconnected(clock.monotonic()) == []
    assert tracker.tick(clock.monotonic()) == []


def test_host_loss_is_silence_then_recovery_without_resync() -> None:
    """Ten seconds after the first broadcast, not before.

    Recovery of the same incarnation restores the broadcast and does not
    call ``on_resync``. F13 names reconnect and account_reset only. §5.6's
    host-loss row also says the account receives ``on_resync``; this test
    follows F13.
    """
    clock = FakeClock()
    tracker = _tracker()
    clock.advance(100)
    assert tracker.tick(clock.monotonic()) == []
    assert tracker.md_state(_FEED) is None
    assert tracker.td_state(7) is None

    tracker.apply_md(_SUBJECT, _atom(version=1), clock.monotonic())
    tracker.apply_td_state(
        _account("ready", version=1, incarnation=1), clock.monotonic()
    )
    clock.advance(9)
    assert tracker.tick(clock.monotonic()) == []
    assert tracker.md_state(_FEED) == "live"
    assert tracker.td_state(7) == "ready"

    clock.advance(1)
    silent = tracker.tick(clock.monotonic())
    assert _kinds(silent) == [
        f"md:down:{REASON_SILENCE}",
        f"td:unavailable:{REASON_SILENCE}:now",
    ]
    assert tracker.tick(clock.monotonic()) == []

    clock.advance(1)
    recovered = tracker.apply_md(_SUBJECT, _atom(version=2), clock.monotonic())
    recovered += tracker.apply_td_state(
        _account("ready", version=2, incarnation=1), clock.monotonic()
    )
    assert _kinds(recovered) == ["md:live:live", "td:ready:ready:now"]
    assert all(not isinstance(effect, Resync) for effect in recovered)


def test_silence_is_not_refreshed_by_a_stale_version() -> None:
    clock = FakeClock()
    tracker = _tracker()
    tracker.apply_md(_SUBJECT, _atom(version=2), clock.monotonic())
    clock.advance(9)
    assert (
        tracker.apply_md(_SUBJECT, _atom(version=1), clock.monotonic()) == []
    )
    clock.advance(1)
    assert _kinds(tracker.tick(clock.monotonic())) == [f"md:down:{REASON_SILENCE}"]


def test_a_version_skip_is_the_snapshot_and_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    tracker = _tracker()
    tracker.apply_md(_SUBJECT, _worker("live", version=1), clock.monotonic())
    tracker.apply_md(_SUBJECT, _atom(version=1), clock.monotonic())
    with caplog.at_level(logging.WARNING, logger=_LOG):
        effects = tracker.apply_md(
            _SUBJECT, _worker("down", version=4), clock.monotonic()
        )
    assert _kinds(effects) == ["md:down:down"]
    assert any("md broadcast gap" in record.message for record in caplog.records)
    assert tracker.md_state(_FEED) == "down"


def test_composite_feed_is_down_when_any_atom_is(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = FakeClock()
    feed = "composite.Paper_Spot_BTCUSDT"
    tracker = _tracker({feed: frozenset({"leg-a", "leg-b"})}, accounts=())
    subject = "md.w.paper.legs"
    tracker.apply_md(subject, _atom("leg-a", version=1), clock.monotonic())
    pending = tracker.apply_md(
        subject, _atom("leg-b", version=1, state="pending"), clock.monotonic()
    )
    assert _kinds(pending) == ["md:down:pending"]
    both = tracker.apply_md(
        subject, _atom("leg-b", version=2, state="subscribed"), clock.monotonic()
    )
    assert _kinds(both) == ["md:live:live"]
    clock.advance(SILENCE_S)
    assert _kinds(tracker.tick(clock.monotonic())) == [f"md:down:{REASON_SILENCE}"]


def test_one_warning_per_transition(caplog: pytest.LogCaptureFixture) -> None:
    clock = FakeClock()
    tracker = _tracker()
    with caplog.at_level(logging.WARNING, logger=_LOG):
        tracker.apply_td_state(
            _account("ready", version=1), clock.monotonic()
        )
        tracker.apply_td_state(
            _account("ready", version=2), clock.monotonic()
        )
    texts = [
        record.message
        for record in caplog.records
        if record.message.startswith("td availability")
    ]
    assert texts == ["td availability api_id=7 state=ready reason=ready"]


def test_must_deliver_includes_availability_notices() -> None:
    assert StreamKind.MD_NOTICE in MUST_DELIVER
    assert StreamKind.TD_NOTICE in MUST_DELIVER
    assert StreamKind.RESYNC in MUST_DELIVER


def test_notices_survive_a_market_data_flood() -> None:
    """Notices share the must-deliver queue. A latest book does not push one out.

    Two notices fit in a queue of 2 beside any number of books on one
    feed: the books conflate to the newest. A third notice overflows
    that queue and fails the session. It is not dropped, and it is not
    pulled.
    """
    ingress = Ingress(
        StsCreateSessionRequest(session_id="abc123", created_by=1, strategy="noop"),
        capacity=2,
    )
    ingress.start()
    runner = StrategyRunner(ingress, Strategy())
    runner.start()
    runner.begin_on_start()
    runner.end_on_start()
    runner.begin_on_ready()
    runner.end_on_ready()
    reasons: list[str] = []
    ingress.set_failure_callback(reasons.append)

    def book(event_id: str) -> Inbound:
        return Inbound(
            kind=StreamKind.ORDERBOOK,
            feed=_FEED,
            recv_ts=0.0,
            body=b"{}",
            event_id=event_id,
        )

    def notice(kind: StreamKind, event_id: str) -> Inbound:
        return Inbound(
            kind=kind,
            feed=_FEED,
            recv_ts=0.0,
            body=b"{}",
            event_id=event_id,
        )

    ingress.offer(book("a"))
    ingress.offer(book("b"))
    ingress.offer(notice(StreamKind.MD_NOTICE, "md"))
    ingress.offer(notice(StreamKind.TD_NOTICE, "td"))
    ingress.offer(book("c"))
    ingress.offer(notice(StreamKind.RESYNC, "resync"))
    pulled: list[str] = []
    while True:
        event = ingress.pull()
        if event is None:
            break
        pulled.append(event.event_id)
    assert pulled == ["md", "c", "td"]
    assert "resync" not in pulled
    assert reasons == ["resync_overflow"]
    assert ingress.delivery.dropped == 0
    assert ingress.delivery.mark("a") is not None
    assert ingress.delivery.mark("resync") is None
    runner.finish()
    ingress.close()


def test_md_and_td_state_read_the_session() -> None:
    strategy = Strategy()
    session = SimpleNamespace(
        session_id="abc123",
        type="t",
        feed_state=lambda feed: "down" if feed == _FEED else None,
        account_state=lambda api_id: "degraded" if api_id == 7 else None,
    )
    strategy.bind(session)  # type: ignore[arg-type]
    assert strategy.md.state(_FEED) == "down"
    assert strategy.md.state("missing") is None
    assert strategy.td.state(7) == "degraded"
    assert strategy.td.state(9) is None
    bare = Strategy()
    assert bare.md.state(_FEED) is None
    assert bare.td.state(7) is None


class _Broker:
    def __init__(
        self,
        *,
        fail: bool = False,
        view: bool = False,
        error: dict[str, str] | None = None,
    ) -> None:
        self.calls: list[str] = []
        self.sent: list[tuple[object, float | None]] = []
        self.fail = fail
        self.view = view
        self.error = error

    async def request(
        self, subject: str, envelope: object, timeout: float | None = None
    ):
        self.calls.append(subject)
        self.sent.append((envelope, timeout))
        if self.fail:
            raise RuntimeError("td is gone")
        if self.error is not None:
            raw = Envelope.wrap(self.error, type=TD_ERROR, source="td").to_json()
            return UntypedEnvelope.from_json(raw)
        if self.view:
            raw = Envelope.wrap(OmsView(), type=TD_OMS_VIEW, source="td").to_json()
            return UntypedEnvelope.from_json(raw)
        ack = OrderAck(api_id=7, client_order_id="ignored", accepted=True)
        raw = Envelope.wrap(ack, type=TD_ORDER_ACK, source="td").to_json()
        return UntypedEnvelope.from_json(raw)


def _trading(state: str | None) -> tuple[Strategy, _Broker, dict[str, str | None]]:
    broker = _Broker()
    held: dict[str, str | None] = {"state": state}
    session = SimpleNamespace(
        session_id="abc123",
        type="t",
        event_log=EventLog("abc123", directory=None),
        broker=broker,
        order_phase="running",
        account_state=lambda api_id: held["state"] if api_id == 7 else None,
        strategy=None,
    )
    strategy = Strategy()
    strategy.bind(session)  # type: ignore[arg-type]
    session.strategy = strategy
    return strategy, broker, held


async def test_unavailable_refuses_submit_and_cancel_locally() -> None:
    strategy, broker, _held = _trading("unavailable")
    strategy.oms._inflight.add("resting")  # noqa: SLF001
    accepted = await strategy.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("1"),
    )
    assert accepted is False
    assert strategy.oms.last_reject_reason == "td_unavailable"
    assert strategy.oms.last_reject_code == RejectCode.TD_UNAVAILABLE
    minted = strategy.oms.last_client_order_id
    assert minted
    assert minted not in strategy.oms._inflight  # noqa: SLF001
    cancelled = await strategy.oms.cancel_order(7, "resting")
    assert cancelled is False
    assert strategy.oms.last_reject_code == RejectCode.TD_UNAVAILABLE
    assert strategy.oms.last_reject_reason == "td_unavailable"
    assert broker.calls == []
    assert strategy.oms.last_client_order_id == minted


async def test_degraded_still_sends() -> None:
    strategy, broker, _held = _trading("degraded")
    accepted = await strategy.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("1"),
    )
    assert accepted is True
    assert broker.calls
    assert strategy.oms.last_reject_code == RejectCode.NONE


async def test_unknown_account_is_not_refused_locally() -> None:
    strategy, broker, _held = _trading(None)
    accepted = await strategy.oms.submit_order(
        7,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("1"),
    )
    assert accepted is True
    assert broker.calls


async def test_a_stale_notice_does_not_rewind_the_hook() -> None:
    clock = FakeClock()
    tracker = _tracker()
    down = tracker.apply_md(
        _SUBJECT, _atom(version=1, error="closed"), clock.monotonic()
    )
    live = tracker.apply_md(_SUBJECT, _atom(version=2), clock.monotonic())
    strategy = _Sees()
    session = SimpleNamespace(session_id="abc123", type="t", availability=tracker)
    strategy.bind(session)  # type: ignore[arg-type]
    log = EventLog("abc123", directory=None)
    newer = live[0]
    older = down[0]
    assert isinstance(newer, MdUpdate) and isinstance(older, MdUpdate)
    await dispatch_notice(strategy, log, _md_event(newer), swallow=False)
    await dispatch_notice(strategy, log, _md_event(older), swallow=False)
    assert strategy.md_updates == [(_FEED, "live", "live")]
    assert tracker.md_state(_FEED) == "live"


def _md_event(effect: MdUpdate) -> Inbound:
    body = json.dumps(
        {
            "feed": effect.feed,
            "state": effect.state,
            "reason": effect.reason,
            "token": effect.token,
        }
    ).encode()
    return Inbound(
        kind=StreamKind.MD_NOTICE,
        feed=effect.feed,
        recv_ts=0.0,
        body=body,
        event_id="n",
    )


async def test_read_oms_view_failure_returns_none(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger=_LOG):
        missing = await read_oms_view(object(), api_id=7, session_id="abc123")
        failed = await read_oms_view(
            _Broker(fail=True), api_id=7, session_id="abc123"
        )
    assert missing is None
    assert failed is None
    assert any("on_resync" in record.message for record in caplog.records)
    broker = _Broker(view=True)
    got = await read_oms_view(broker, api_id=7, session_id="abc123")
    assert got == OmsView()
    envelope, timeout = broker.sent[0]
    assert getattr(envelope.payload, "settled", None) is True
    assert timeout == SETTLED_VIEW_TIMEOUT_S


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("107", "venue is not connected"),
        ("invalid_payload", "settled read refused"),
    ],
)
async def test_td_error_is_not_an_empty_oms_view(
    caplog: pytest.LogCaptureFixture, code: str, message: str
) -> None:
    broker = _Broker(error={"code": code, "message": message})
    with caplog.at_level(logging.WARNING, logger=_LOG):
        got = await read_oms_view(broker, api_id=7, session_id="abc123")
    assert got is None
    assert code in caplog.text
    assert message in caplog.text
    envelope, timeout = broker.sent[0]
    assert getattr(envelope.payload, "settled", None) is True
    assert timeout == SETTLED_VIEW_TIMEOUT_S


async def test_the_settled_wait_does_not_block_other_notices() -> None:
    """The 35s settled read is a task on the ingress loop.

    Notices queued with it, and a notice offered while it is still
    parked, reach a strategy thread before that read returns.
    """
    release = asyncio.Event()
    started = asyncio.Event()
    request_ident: list[int] = []

    class _Slow:
        async def request(
            self, subject: str, envelope: object, timeout: float | None = None
        ):
            del subject
            request_ident.append(threading.get_ident())
            assert timeout == SETTLED_VIEW_TIMEOUT_S
            payload = getattr(envelope, "payload", None)
            assert getattr(payload, "settled", None) is True
            started.set()
            await release.wait()
            raw = Envelope.wrap(OmsView(), type=TD_OMS_VIEW, source="td").to_json()
            return UntypedEnvelope.from_json(raw)

    queued: list[str] = []

    async def deliver(effect: Resync) -> None:
        await read_oms_view(_Slow(), api_id=effect.api_id, session_id="abc123")

    async def warn(effect: MdUpdate | TdUpdate) -> None:
        del effect

    schedule_effects(
        [
            Resync(7, "account_reset", None),
            MdUpdate(_FEED, "down", REASON_SILENCE, 1),
        ],
        offer=lambda effect: queued.append(effect.state),
        deliver=deliver,
        log=warn,
    )
    assert queued == ["down"]
    assert not started.is_set()

    await asyncio.sleep(0)
    assert started.is_set()
    schedule_effects(
        [MdUpdate(_FEED, "live", REASON_INGRESS, 2)],
        offer=lambda effect: queued.append(effect.state),
        deliver=deliver,
        log=warn,
    )
    assert queued == ["down", "live"]

    pulled: list[str] = []
    strategy_ident: list[int] = []

    def _strategy() -> None:
        strategy_ident.append(threading.get_ident())
        pulled.extend(queued)

    worker = threading.Thread(target=_strategy)
    worker.start()
    worker.join()
    assert pulled == ["down", "live"]
    assert strategy_ident[0] != request_ident[0]
    assert request_ident[0] == threading.get_ident()

    release.set()
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current]
    await asyncio.gather(*pending)


def test_running_conditions_follow_live_availability() -> None:
    clock = FakeClock()
    tracker = _tracker()
    progress = _Progress()
    progress.td_total = 1
    progress.td_ready = 1
    running = _conditions(
        status="running",
        phase=Phase.RUNNING,
        clock=clock,
        progress=progress,
        feeds=None,
        availability=tracker,
    )
    assert running["MdReady"] == "1/1"
    assert running["TdReady"] == "1/1"
    tracker.apply_md(_SUBJECT, _atom(version=1, error="closed"), clock.monotonic())
    tracker.apply_td_state(
        _account("degraded", version=1, reason="private"), clock.monotonic()
    )
    later = _conditions(
        status="running",
        phase=Phase.RUNNING,
        clock=clock,
        progress=progress,
        feeds=None,
        availability=tracker,
    )
    assert later["MdReady"] == "0/1"
    assert later["TdReady"] == "0/1"
    startup = _conditions(
        status="starting",
        phase=Phase.READY,
        clock=clock,
        progress=progress,
        feeds=None,
        availability=tracker,
    )
    assert "MdReady" not in startup
    assert startup["TdReady"] == "1/1"

"""Delivery table, beyond the rows the IF-05 contract already names.

One case per notice row, the shared must-deliver FIFO, a ``strategy.yml``
override that cannot move that row, ``FakeClock`` for ``age``, per-feed
drop counts on the status snapshot, and ``seq`` taken from the MD
envelope. Nothing here sleeps or opens a socket.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
from mftik.clock import FakeClock
from mftik.exchange.models import Kline, Side, Trade
from mftik.protocol import (
    MD_KLINE,
    MD_TRADE,
    Envelope,
    StsCreateSessionRequest,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik.strategy.delivered import bind_delivery
from mftik.strategy.eventlog import EventLog
from mftik_sts.session_worker.delivery import Delivery
from mftik_sts.session_worker.dispatch import dispatch_md
from mftik_sts.session_worker.errors import SessionFailed
from mftik_sts.session_worker.events import Inbound, LogMark, StreamKind
from mftik_sts.session_worker.ingress import Ingress
from mftik_sts.session_worker.limits import (
    ALL_QUEUE_CAPACITY,
    DROP_WARN_INTERVAL_S,
    MARK_RETENTION,
    MUST_DELIVER_CAPACITY,
    TEMP_BUFFER_CAPACITY,
    WARNING_RETENTION,
)
from mftik_sts.session_worker.process import (
    _inbound,
    delivery_overrides_of,
    drop_status,
)

_NOTICES = (StreamKind.MD_NOTICE, StreamKind.TD_NOTICE, StreamKind.RESYNC)
_TICKER = "ticker.Deribit_Perp_BTCUSD"
_TRADE = "trade.Deribit_Perp_BTCUSD"


def _event(
    kind: StreamKind,
    feed: str,
    event_id: str,
    *,
    seq: int | None = None,
    recv_ts: float = 0.0,
    bar_open: float | None = None,
) -> Inbound:
    return Inbound(
        kind=kind,
        feed=feed,
        recv_ts=recv_ts,
        body=b"",
        event_id=event_id,
        seq=seq,
        bar_open=bar_open,
    )


def _drain(lane: Delivery) -> list[Inbound]:
    taken: list[Inbound] = []
    while len(taken) <= 8:
        event = lane.take()
        if event is None:
            return taken
        taken.append(event)
    raise AssertionError("take() did not empty")


@pytest.mark.parametrize("kind", _NOTICES, ids=lambda kind: kind.value)
def test_an_availability_notice_overflow_fails_and_drops_nothing(
    kind: StreamKind,
) -> None:
    """Notices are the must-deliver row B5-05 added. Same rule as TD."""
    lane = Delivery(all_capacity=1, must_capacity=1)
    lane.accept(_event(kind, kind.value, "kept", recv_ts=1))
    with pytest.raises(SessionFailed) as caught:
        lane.accept(_event(kind, kind.value, "extra", recv_ts=2))
    assert caught.value.reason == f"{kind.value}_overflow"
    assert lane.failed is True
    assert lane.dropped == 0
    assert lane.mark("extra") is None
    assert [event.event_id for event in _drain(lane)] == ["kept"]


def test_must_deliver_kinds_share_one_fifo_ahead_of_each_feed() -> None:
    """The shared FIFO is drained before any market-data feed.

    A fill and the RPC reply offered after it stay in that order, and
    both come out before the trades that were already queued. Market
    data does not take a turn between them.
    """
    trade = "trade.Paper_Spot_BTCUSDT"
    lane = Delivery(all_capacity=8, must_capacity=8)
    lane.accept(_event(StreamKind.TRADE, trade, "t1", seq=1, recv_ts=1))
    lane.accept(_event(StreamKind.TD, "td.7", "fill", recv_ts=2))
    lane.accept(_event(StreamKind.RPC_REPLY, "rpc", "ack", recv_ts=3))
    lane.accept(_event(StreamKind.TRADE, trade, "t2", seq=2, recv_ts=4))
    assert [event.event_id for event in _drain(lane)] == [
        "fill",
        "ack",
        "t1",
        "t2",
    ]


def test_a_resync_stays_ahead_of_the_deferred_ready() -> None:
    """Offered in that order, both notices come out before the print."""
    trade = "trade.Paper_Spot_BTCUSDT"
    lane = Delivery(all_capacity=8, must_capacity=8)
    lane.accept(_event(StreamKind.TRADE, trade, "t", seq=1))
    lane.accept(_event(StreamKind.RESYNC, "td.7", "resync"))
    lane.accept(_event(StreamKind.TD_NOTICE, "td.7", "ready"))
    assert [event.event_id for event in _drain(lane)] == ["resync", "ready", "t"]


def test_a_saturated_market_does_not_hold_a_fill_or_overflow_it() -> None:
    """Several ``all`` feeds stay full. The next ``take`` is still the fill.

    Market data drops its oldest print. Must-deliver does not share
    those turns, so a strategy that keeps taking drains every fill
    before another book, and the must-deliver queue never fills.
    """
    clock = FakeClock()
    feeds = (
        "trade.Paper_Spot_BTCUSDT",
        "trade.Paper_Spot_ETHUSDT",
        "trade.Paper_Spot_SOLUSDT",
    )
    lane = Delivery(all_capacity=4, must_capacity=4, clock=clock)
    for feed in feeds:
        for seq in range(lane.all_capacity):
            lane.accept(
                _event(
                    StreamKind.TRADE,
                    feed,
                    f"{feed}-{seq}",
                    seq=seq,
                    recv_ts=clock.now(),
                )
            )
    for burst in range(lane.all_capacity * 3):
        clock.advance(0.05)
        for feed in feeds:
            lane.accept(
                _event(
                    StreamKind.TRADE,
                    feed,
                    f"{feed}-more-{burst}",
                    seq=100 + burst,
                    recv_ts=clock.now(),
                )
            )
        fill = f"fill-{burst}"
        lane.accept(_event(StreamKind.TD, "td.7", fill, recv_ts=clock.now()))
        got = lane.take()
        assert got is not None
        assert got.event_id == fill
        assert got.kind is StreamKind.TD
        assert lane.failed is False
    assert lane.dropped > 0
    assert lane.fail_reason is None
    rest: list[Inbound] = []
    while True:
        event = lane.take()
        if event is None:
            break
        rest.append(event)
    assert len(rest) == lane.all_capacity * len(feeds)
    assert all(event.kind is not StreamKind.TD for event in rest)


def test_strategy_yml_overrides_apply_and_cannot_move_must_deliver() -> None:
    """``delivery:`` on a feed replaces that feed's default only.

    A ticker asked to be ``all`` keeps both prints. A trade asked to be
    ``latest`` conflates, and that replacement is not a drop. Putting
    ``latest`` on the TD feed key does not make a fill droppable.
    """
    text = (
        "md:\n"
        "  - feed: ticker.Deribit_Perp_BTCUSD\n"
        "    delivery: all\n"
        "  - feed: trade.Deribit_Perp_BTCUSD\n"
        "    delivery: latest\n"
    )
    request = StsCreateSessionRequest(
        session_id="abc123",
        created_by=1,
        strategy="noop",
        yaml_text=text,
    )
    overrides = delivery_overrides_of(request)
    assert overrides == {_TICKER: "all", _TRADE: "latest"}
    assert delivery_overrides_of(
        StsCreateSessionRequest(session_id="abc123", created_by=1, strategy="noop")
    ) == {}

    lane = Delivery(
        all_capacity=2,
        must_capacity=2,
        overrides={**overrides, "td.7": "latest"},
    )
    lane.accept(_event(StreamKind.TICKER, _TICKER, "a", seq=1, recv_ts=1))
    lane.accept(_event(StreamKind.TICKER, _TICKER, "b", seq=2, recv_ts=2))
    lane.accept(_event(StreamKind.TRADE, _TRADE, "old", seq=1, recv_ts=3))
    lane.accept(_event(StreamKind.TRADE, _TRADE, "new", seq=9, recv_ts=4))
    lane.accept(_event(StreamKind.TD, "td.7", "fill", recv_ts=5))
    lane.accept(_event(StreamKind.TD, "td.7", "second", recv_ts=6))
    with pytest.raises(SessionFailed) as caught:
        lane.accept(_event(StreamKind.TD, "td.7", "third", recv_ts=7))
    assert caught.value.reason == "td_overflow"
    assert lane.mark("old") is LogMark.SUPERSEDED
    assert lane.mark("a") is None
    assert lane.mark("third") is None
    assert lane.dropped == 0
    assert lane.dropped_by_feed == {}
    taken = {event.event_id for event in _drain(lane)}
    assert taken == {"a", "b", "new", "fill", "second"}


def test_age_is_the_ingress_clock_read_on_the_strategy_thread() -> None:
    """``age`` subtracts when it is read. Moving the clock moves it."""
    clock = FakeClock()
    event = Inbound(
        kind=StreamKind.TRADE,
        feed=_TRADE,
        recv_ts=clock.now(),
        body=b"",
        event_id="e",
        seq=4,
        clock=clock.now,
    )
    assert event.age == 0.0
    clock.advance(2.5)
    assert event.age == 2.5

    trade = Trade(
        universal_ticker="Deribit_Perp_BTCUSD",
        price=Decimal("1"),
        qty=Decimal("1"),
        side=Side.BUY,
    )
    other = Trade(
        universal_ticker="Deribit_Perp_BTCUSD",
        price=Decimal("1"),
        qty=Decimal("1"),
        side=Side.BUY,
        trade_id=trade.trade_id,
        ts=trade.ts,
    )
    assert trade == other
    bind_delivery(trade, seq=4, recv_ts=clock.now(), clock=clock.now)
    clock.advance(1.5)
    assert trade.seq == 4
    assert trade.recv_ts == 2.5
    assert trade.age == 1.5
    assert other.seq is None
    assert other.age is None
    assert trade == other
    assert "seq" not in trade.model_dump()
    assert "recv_ts" not in trade.model_dump()


def test_drops_are_counted_per_feed_and_published_on_the_snapshot() -> None:
    btc = "trade.Paper_Spot_BTCUSDT"
    eth = "trade.Paper_Spot_ETHUSDT"
    ingress = Ingress(
        StsCreateSessionRequest(session_id="abc123", created_by=1, strategy="noop"),
        all_capacity=1, must_capacity=1,
    )
    lane = ingress.delivery
    lane.accept(_event(StreamKind.TRADE, btc, "b1", seq=1))
    lane.accept(_event(StreamKind.TRADE, btc, "b2", seq=2))
    lane.accept(_event(StreamKind.TRADE, eth, "e1", seq=1))
    lane.accept(_event(StreamKind.TRADE, eth, "e2", seq=2))
    lane.accept(_event(StreamKind.TICKER, "ticker.Paper_Spot_BTCUSDT", "t1", seq=1))
    lane.accept(_event(StreamKind.TICKER, "ticker.Paper_Spot_BTCUSDT", "t2", seq=5))
    assert lane.dropped == 2
    assert lane.dropped_by_feed == {btc: 1, eth: 1}
    assert lane.mark("t1") is LogMark.SUPERSEDED
    total, fields = drop_status(ingress)
    assert total == 2
    assert fields == {f"dropped.{btc}": "1", f"dropped.{eth}": "1"}


def test_drop_warnings_are_rate_limited_per_feed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Every drop is counted. The log line for one feed waits out the window."""
    clock = FakeClock()
    feed = "trade.Paper_Spot_BTCUSDT"
    lane = Delivery(all_capacity=1, must_capacity=1, clock=clock)

    def one(seq: int) -> None:
        lane.accept(_event(StreamKind.TRADE, feed, f"e{seq}", seq=seq, recv_ts=seq))

    with caplog.at_level(logging.WARNING, logger="mftik_sts.session_worker.delivery"):
        one(1)
        one(2)
        one(3)
        clock.advance(DROP_WARN_INTERVAL_S)
        one(4)
    logged = [
        record.message
        for record in caplog.records
        if "dropped oldest" in record.message
    ]
    assert len(lane.warnings()) == 3
    assert len(logged) == 2
    assert lane.dropped_by_feed == {feed: 3}


def test_marks_and_warnings_forget_the_oldest() -> None:
    """A long session does not keep one note per event.

    The drop count still names every loss. The mark dict and the
    warning deque keep only the recent window.
    """
    clock = FakeClock()
    feed = "trade.Paper_Spot_BTCUSDT"
    lane = Delivery(all_capacity=1, must_capacity=1, clock=clock)
    total = MARK_RETENTION + 2
    for seq in range(total):
        clock.advance(0.001)
        lane.accept(
            _event(StreamKind.TRADE, feed, f"e{seq}", seq=seq, recv_ts=clock.now())
        )
    drops = total - 1
    assert lane.dropped == drops
    assert lane.mark("e0") is None
    assert lane.mark(f"e{drops - 1}") is LogMark.DROPPED
    assert lane.mark(f"e{total - 1}") is None
    lines = lane.warnings()
    assert len(lines) == WARNING_RETENTION
    assert lines[0].endswith("seq=1 count=2")
    assert lines[-1].endswith(f"seq={drops - 1} count={drops}")


def test_seq_and_bar_open_come_from_the_envelope() -> None:
    """``seq`` is the connection worker's envelope field. Not a local counter.

    ``open_time`` and ``closed`` are read off the payload the ingress
    already parsed. A closed bar is then delivered before a later bar
    that was accepted first.
    """
    clock = FakeClock()
    trade = Trade(
        universal_ticker="Paper_Spot_BTCUSDT",
        price=Decimal("1"),
        qty=Decimal("1"),
        side=Side.BUY,
    )
    raw = Envelope.wrap(trade, type=MD_TRADE, source="md.paper", seq=7).to_json()
    event = _inbound(
        raw,
        feed="trade.Paper_Spot_BTCUSDT",
        kind=StreamKind.TRADE,
        recv_ts=clock.now(),
        clock=clock,
    )
    assert event is not None
    assert event.seq == 7
    assert event.bar_open is None
    clock.advance(4)
    assert event.age == 4

    def bar(open_time: float, *, closed: bool, seq: int) -> Inbound:
        body = Kline(
            universal_ticker="Paper_Spot_BTCUSDT",
            interval="1m",
            open_time=open_time,
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            closed=closed,
        )
        framed = Envelope.wrap(
            body, type=MD_KLINE, source="md.paper", seq=seq
        ).to_json()
        parsed = _inbound(
            framed,
            feed="kline_1m.Paper_Spot_BTCUSDT",
            kind=StreamKind.KLINE,
            recv_ts=float(seq),
            clock=clock,
        )
        assert parsed is not None
        return parsed

    lane = Delivery(all_capacity=4, must_capacity=4)
    later = bar(160, closed=False, seq=4)
    close = bar(100, closed=True, seq=3)
    assert later.bar_open == 160
    assert close.closed is True
    assert close.seq == 3
    lane.accept(later)
    lane.accept(close)
    drained = [
        event.event_id
        for event in _drain(lane)
        if event.feed.startswith("kline")
    ]
    assert drained == [close.event_id, later.event_id]


async def test_dispatch_stamps_seq_and_age_on_the_hook_argument() -> None:
    clock = FakeClock()
    seen: list[Trade] = []

    class Spy(Strategy):
        async def on_trade(self, trade: Trade) -> None:
            seen.append(trade)

    trade = Trade(
        universal_ticker="Paper_Spot_BTCUSDT",
        price=Decimal("1"),
        qty=Decimal("1"),
        side=Side.BUY,
    )
    envelope = Envelope.wrap(trade, type=MD_TRADE, source="md.paper", seq=7)
    raw = envelope.to_json()
    event = _inbound(
        raw,
        feed="trade.Paper_Spot_BTCUSDT",
        kind=StreamKind.TRADE,
        recv_ts=clock.now(),
        clock=clock,
    )
    assert event is not None
    clock.advance(3)
    await dispatch_md(
        Spy(),
        EventLog("abc123", directory=None),
        UntypedEnvelope.from_json(raw),
        swallow=False,
        delivery=event,
        clock=clock.now,
    )
    assert len(seen) == 1
    assert seen[0].seq == 7
    assert seen[0].recv_ts == 0.0
    assert seen[0].age == 3


def test_the_queue_bounds_are_independent() -> None:
    """#296. An ``all`` feed and the must-deliver FIFO do not share a bound.

    1024 ``all`` events do not fail the session. Must-deliver fails only
    past 8192. An ``all`` overflow never evicts a must-deliver event.
    ``MARK_RETENTION`` stays its own cap.
    """
    assert ALL_QUEUE_CAPACITY == 1024
    assert MUST_DELIVER_CAPACITY == 8192
    assert TEMP_BUFFER_CAPACITY == ALL_QUEUE_CAPACITY
    assert MARK_RETENTION == 1024

    feed = "trade.Paper_Spot_BTCUSDT"
    market = Delivery(
        all_capacity=ALL_QUEUE_CAPACITY,
        must_capacity=MUST_DELIVER_CAPACITY,
    )
    # The bound is the queue length. One event, accepted that many
    # times, is the same depth as that many distinct prints, and it
    # stays inside the unit call budget.
    queued = _event(StreamKind.TRADE, feed, "queued", seq=0)
    for _ in range(ALL_QUEUE_CAPACITY):
        market.accept(queued)
    assert market.failed is False
    assert market.dropped == 0
    market.accept(_event(StreamKind.TRADE, feed, "extra", seq=ALL_QUEUE_CAPACITY))
    assert market.failed is False
    assert market.dropped == 1

    must = Delivery(all_capacity=1, must_capacity=MUST_DELIVER_CAPACITY)
    held = _event(StreamKind.TD, "td.7", "t0")
    for _ in range(MUST_DELIVER_CAPACITY):
        must.accept(held)
    assert must.failed is False
    with pytest.raises(SessionFailed) as caught:
        must.accept(_event(StreamKind.TD, "td.7", "overflow"))
    assert caught.value.reason == "td_overflow"
    assert must.failed is True
    assert must.mark("t0") is None
    assert must.mark("overflow") is None

    mixed = Delivery(all_capacity=1, must_capacity=2)
    mixed.accept(_event(StreamKind.TD, "td.7", "fill"))
    mixed.accept(_event(StreamKind.TRADE, feed, "old", seq=1))
    mixed.accept(_event(StreamKind.TRADE, feed, "new", seq=2))
    assert mixed.mark("old") is LogMark.DROPPED
    assert mixed.mark("fill") is None
    assert mixed.failed is False
    taken = mixed.take()
    assert taken is not None
    assert taken.event_id == "fill"
    assert mixed.mark("fill") is LogMark.DELIVERED

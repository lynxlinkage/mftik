"""What the session worker will do, written down before it does it (IF-05).

I1–I4 run. The rest are ``xfail(strict=True)``. The surface file is
where the null answers are pinned, so a stub that starts returning a
real queue fails there until this marker is taken off in the same
change. ``strict`` is what makes the marker have to come off: an
``xfail`` that passes is a failure.

The ticket that owns each remaining test is named in its ``reason``.

* The delivery table, including a TD overflow that fails the session
  — B5-01.
* The log line carrying the same mark — B5-02.
* Every row of the F15 table — B5-04.

Nothing here opens a socket or sleeps. Time is a number the test
passes in.
"""

from __future__ import annotations

import threading

import pytest
from mftik.protocol import (
    DEFAULT_START_TIMEOUT_S,
    DELIVERY_ALL,
    StsCreateSessionRequest,
)
from mftik.strategy import Strategy
from mftik_sts.session_worker import (
    HOOK_HARD_S,
    HOOK_WARN_S,
    ON_READY_LIMIT_S,
    ON_STOP_LIMIT_S,
    Delivery,
    Disposition,
    Inbound,
    Ingress,
    IngressEnded,
    IngressNotMainThread,
    IngressNotStarted,
    LogMark,
    Measure,
    Phase,
    SessionFailed,
    SignalHandlersReserved,
    StrategyRunner,
    StrategyStillRunning,
    StreamKind,
    assess_hook,
    refuse_strategy_signal_handler,
)

_B5_DELIVERY = "B5-01 applies the delivery table"
_B5_LOG = "B5-02 writes delivered / superseded / dropped onto the event log"
_B5_BUDGET = "B5-04 classifies hook time (F15)"

LATEST = (
    StreamKind.TICKER,
    StreamKind.BESTQUOTE,
    StreamKind.GREEKS,
    StreamKind.FUNDING,
    StreamKind.OPEN_INTEREST,
    StreamKind.ORDERBOOK,
)
MARKET_ALL = (StreamKind.TRADE, StreamKind.AGGTRADE, StreamKind.LIQUIDATION)
MUST_DELIVER = (StreamKind.TD, StreamKind.FEED_END, StreamKind.RPC_REPLY)


def _spec() -> StsCreateSessionRequest:
    return StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )


def _ingress(capacity: int = 4) -> Ingress:
    return Ingress(_spec(), capacity=capacity)


def _event(
    kind: StreamKind,
    feed: str,
    event_id: str,
    *,
    seq: int | None = None,
    body: bytes = b"",
    recv_ts: float = 0.0,
    bar_open: float | None = None,
    closed: bool | None = None,
    clock=None,
) -> Inbound:
    return Inbound(
        kind=kind,
        feed=feed,
        recv_ts=recv_ts,
        body=body,
        event_id=event_id,
        seq=seq,
        bar_open=bar_open,
        closed=closed,
        clock=clock,
    )


def _drain(lane: Delivery) -> list[Inbound]:
    taken: list[Inbound] = []
    while len(taken) <= 8:
        event = lane.take()
        if event is None:
            return taken
        taken.append(event)
    raise AssertionError("take() did not empty")


# --- I1 to I4 --------------------------------------------------------------


def test_i1_the_strategy_thread_cannot_start_first() -> None:
    """I1. The ingress is already up before the strategy thread exists,
    so the receive side is there for the whole of ``on_start``."""
    runner = StrategyRunner(_ingress(), Strategy())
    with pytest.raises(IngressNotStarted):
        runner.start()


def test_i1_close_is_refused_while_the_strategy_thread_is_alive() -> None:
    """I1, the other end. ``close`` is phase 6, and phase 6 is after
    the strategy thread has finished."""
    ingress = _ingress()
    runner = StrategyRunner(ingress, Strategy())
    ingress.start()
    runner.start()
    assert runner.alive is True
    with pytest.raises(StrategyStillRunning):
        ingress.close()


def test_i1_phases_run_in_order_and_delivery_waits_for_on_ready() -> None:
    """Phases 0 to 6, and the two holds around them.

    Nothing is delivered during ``on_start`` or ``on_ready``. The one
    event received then is what ``pull`` returns once ``on_ready`` has
    returned. During ``stopping``, ``pull`` still returns — ``on_stop``
    is where a cancel's ack comes back (I1). The ingress's phase stays
    ``stopping`` after the strategy thread finishes, and only ``close``
    moves it to teardown.
    """
    ingress = _ingress()
    runner = StrategyRunner(ingress, Strategy())
    seen: list[Phase] = []

    ingress.start()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    assert ingress.thread is threading.main_thread()

    runner.start()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    assert runner.alive is True
    assert runner.thread is not None
    assert runner.thread is not threading.main_thread()

    runner.begin_on_start()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    held = _event(
        StreamKind.TICKER,
        "ticker.Paper_Spot_BTCUSDT",
        "held",
        seq=1,
        body=b"held",
        recv_ts=1.0,
    )
    ingress.offer(held)
    assert ingress.pull() is None

    runner.end_on_start()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    assert ingress.pull() is None

    runner.begin_on_ready()
    assert ingress.phase is Phase.READY
    assert ingress.pull() is None
    runner.end_on_ready()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    delivered = ingress.pull()
    assert delivered is not None
    assert delivered.event_id == "held"
    assert delivered.body == b"held"

    ingress.stop()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    runner.begin_on_stop()
    assert runner.alive is True
    ack = _event(
        StreamKind.RPC_REPLY, "rpc", "ack", body=b"ack", recv_ts=2.0
    )
    ingress.offer(ack)
    pulled = ingress.pull()
    assert pulled is not None
    assert pulled.kind is StreamKind.RPC_REPLY
    assert pulled.body == b"ack"

    runner.finish()
    assert runner.alive is False
    assert ingress.phase is Phase.STOPPING
    ingress.close()
    seen.append(ingress.phase)  # type: ignore[arg-type]
    assert ingress.exit_code == 0
    assert seen == [
        Phase.BOOT,
        Phase.LOAD,
        Phase.ON_START,
        Phase.READY,
        Phase.RUNNING,
        Phase.STOPPING,
        Phase.TEARDOWN,
    ]


def test_i2_a_dead_ingress_is_not_restarted_in_process() -> None:
    """I2. The ingress is the process. It does not come back as a
    second walk of the same object, and the session id does not change
    under it."""
    ingress = _ingress()
    runner = StrategyRunner(ingress, Strategy())
    ingress.start()
    runner.start()
    ingress.abort()
    assert ingress.exit_code not in (None, 0)
    assert runner.alive is False
    assert ingress.phase is not Phase.BOOT
    assert ingress.spec.session_id == "abc123"
    with pytest.raises(IngressEnded):
        ingress.start()
    with pytest.raises(IngressEnded):
        runner.start()


def test_i3_start_off_the_main_thread_is_refused() -> None:
    """I3. Signal handlers run on the main thread, so the ingress is
    that thread and no other."""
    ingress = _ingress()
    box: list[BaseException] = []

    def run() -> None:
        try:
            ingress.start()
        except Exception as exc:
            box.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    assert len(box) == 1
    assert isinstance(box[0], IngressNotMainThread)


def test_i3_a_strategy_cannot_install_a_signal_handler() -> None:
    """I3, the SDK half. Refused on whatever thread asks."""
    with pytest.raises(SignalHandlersReserved):
        refuse_strategy_signal_handler()


def test_i4_the_ingress_does_not_decode_or_run_the_hook() -> None:
    """I4. ``offer`` queues the bytes it was given. The hook runs on
    the strategy thread, after ``pull``, and the file write is the
    writer thread's. This test does not sleep to look for a file:
    the queued :class:`~mftik_sts.session_worker.events.LogRecord` is
    the handoff."""
    seen: list[object] = []

    class Spy(Strategy):
        async def on_ticker(self, ticker) -> None:  # type: ignore[no-untyped-def]
            seen.append(ticker)

    ingress = _ingress()
    runner = StrategyRunner(ingress, Spy())
    ingress.start()
    runner.start()
    runner.begin_on_start()
    runner.end_on_start()
    runner.begin_on_ready()
    runner.end_on_ready()
    assert ingress.phase is Phase.RUNNING

    body = b'{"last":"1"}'
    ingress.offer(
        _event(
            StreamKind.TICKER,
            "ticker.Paper_Spot_BTCUSDT",
            "t",
            seq=1,
            body=body,
            recv_ts=1.0,
        )
    )
    assert seen == []
    pulled = ingress.pull()
    assert pulled is not None
    assert pulled.body == body
    assert type(pulled.body) is bytes
    assert seen == []
    records = ingress.log_records()
    assert [record.body for record in records] == [body]
    assert records[0].recv_ts == 1.0
    assert records[0].event_seq == 1


# --- F15 -------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=_B5_BUDGET)
@pytest.mark.parametrize("hook", ["on_ticker", "on_order_update", "on_timer"])
def test_a_general_hook_past_one_second_warns_and_does_not_kill(hook: str) -> None:
    """The warning row. Equal to 1s is not past it (超過). A warning
    does not kill and does not relaunch. The report is the progress
    line: which hook, how long, plus the drop count it was handed."""
    under = assess_hook(hook, HOOK_WARN_S)
    assert under.disposition is Disposition.OK
    assert under.measure is Measure.BLOCKED
    assert under.ends_session is False

    elapsed = HOOK_WARN_S + 0.001
    over = assess_hook(hook, elapsed)
    assert over.hook == hook
    assert over.disposition is Disposition.WARN
    assert over.measure is Measure.BLOCKED
    assert over.limit_s == HOOK_WARN_S
    assert over.elapsed_s == pytest.approx(elapsed)
    assert over.ends_session is False
    assert over.restarts is False
    assert over.as_progress(dropped=2).hook == hook
    assert over.as_progress(dropped=2).elapsed_s == pytest.approx(elapsed)
    assert over.as_progress(dropped=2).dropped == 2


@pytest.mark.xfail(strict=True, reason=_B5_BUDGET)
@pytest.mark.parametrize("hook", ["on_ticker", "on_order_update", "on_timer"])
def test_a_general_hook_past_thirty_seconds_is_a_class_b_crash(hook: str) -> None:
    """The hard row. Exactly 30s is still the warning — it has passed
    1s and it has not passed 30s. Past 30s the session is killed,
    cleaned up and failed, and it is not relaunched."""
    at_hard = assess_hook(hook, HOOK_HARD_S)
    assert at_hard.disposition is Disposition.WARN
    assert at_hard.ends_session is False

    over = assess_hook(hook, HOOK_HARD_S + 0.001)
    assert over.disposition is Disposition.CRASH_B
    assert over.measure is Measure.BLOCKED
    assert over.limit_s == HOOK_HARD_S
    assert over.ends_session is True
    assert over.restarts is False


@pytest.mark.xfail(strict=True, reason=_B5_BUDGET)
def test_on_start_is_wall_time_against_its_own_budget() -> None:
    """``on_start`` is not a general hook. 31s of it is not a class-B
    crash — F3 is a long warm-up, and the only line is
    ``start_timeout_s``. Past that line the session fails init and
    does not restart. The default budget is 60s; a session may set
    its own, under the parser's cap."""
    long_warm = assess_hook("on_start", HOOK_HARD_S + 1)
    assert long_warm.disposition is Disposition.OK
    assert long_warm.measure is Measure.WALL
    assert long_warm.ends_session is False

    at_budget = assess_hook("on_start", DEFAULT_START_TIMEOUT_S)
    assert at_budget.disposition is Disposition.OK
    over = assess_hook("on_start", DEFAULT_START_TIMEOUT_S + 0.001)
    assert over.disposition is Disposition.INIT_FAILED
    assert over.measure is Measure.WALL
    assert over.limit_s == DEFAULT_START_TIMEOUT_S
    assert over.ends_session is True
    assert over.restarts is False

    assert assess_hook("on_start", 10, start_timeout_s=10).disposition is Disposition.OK
    custom = assess_hook("on_start", 10.001, start_timeout_s=10)
    assert custom.disposition is Disposition.INIT_FAILED
    assert custom.limit_s == 10


@pytest.mark.xfail(strict=True, reason=_B5_BUDGET)
def test_on_ready_past_ten_seconds_is_an_init_failure() -> None:
    """Wall time, 10s. Two seconds is not a warning — the 1s line is
    the general row only. Past 10s is init failed, no restart."""
    short = assess_hook("on_ready", 2)
    assert short.disposition is Disposition.OK
    assert short.measure is Measure.WALL
    assert short.ends_session is False
    assert assess_hook("on_ready", ON_READY_LIMIT_S).disposition is Disposition.OK

    over = assess_hook("on_ready", ON_READY_LIMIT_S + 0.001)
    assert over.disposition is Disposition.INIT_FAILED
    assert over.measure is Measure.WALL
    assert over.limit_s == ON_READY_LIMIT_S
    assert over.ends_session is True
    assert over.restarts is False


@pytest.mark.xfail(strict=True, reason=_B5_BUDGET)
def test_on_stop_past_ten_seconds_is_not_waited_for() -> None:
    """Wall time, 10s, the same budget ``on_stop`` already has. Past it
    the ingress stops waiting, the platform cleans up, then the process
    is killed. It is not a warning and it does not relaunch."""
    assert assess_hook("on_stop", 2).disposition is Disposition.OK
    assert assess_hook("on_stop", 2).measure is Measure.WALL
    assert assess_hook("on_stop", ON_STOP_LIMIT_S).disposition is Disposition.OK

    over = assess_hook("on_stop", ON_STOP_LIMIT_S + 0.001)
    assert over.disposition is Disposition.STOP_EXPIRED
    assert over.measure is Measure.WALL
    assert over.limit_s == ON_STOP_LIMIT_S
    assert over.ends_session is True
    assert over.restarts is False


@pytest.mark.xfail(strict=True, reason=_B5_BUDGET)
def test_the_ingress_publishes_the_hook_it_is_judging() -> None:
    """The report is not only a return value. The ingress's progress,
    which is what ``sts.status`` carries, is that report."""
    ingress = _ingress()
    runner = StrategyRunner(ingress, Strategy())
    report = runner.note_hook("on_ticker", HOOK_WARN_S + 0.5)
    assert report.disposition is Disposition.WARN
    assert report.hook == "on_ticker"
    progress = ingress.progress()
    assert progress is not None
    assert progress.hook == "on_ticker"
    assert progress.elapsed_s == pytest.approx(HOOK_WARN_S + 0.5)
    assert progress.dropped == ingress.delivery.dropped


# --- delivery --------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
@pytest.mark.parametrize("kind", LATEST, ids=lambda kind: kind.value)
def test_latest_keeps_the_newest_body_and_does_not_count_a_drop(
    kind: StreamKind,
) -> None:
    """``latest``: one slot per feed, replaced before anyone decodes.

    The bytes ``take`` returns are the newest payload, still bytes.
    The older event is ``superseded``, not ``dropped`` — a hole in
    ``seq`` (1 then 5) is the replacement, and F23 says it is not
    recorded as a loss. ``age`` is ``recv_ts`` against the clock bound
    on the event.
    """
    feed = f"{kind.value}.Paper_Spot_BTCUSDT"
    lane = Delivery(capacity=4)
    lane.accept(
        _event(
            kind, feed, "old", seq=1, body=b"old", recv_ts=1.0, clock=lambda: 10.0
        )
    )
    lane.accept(
        _event(
            kind, feed, "new", seq=5, body=b"new", recv_ts=2.0, clock=lambda: 10.0
        )
    )
    assert lane.mark("old") is LogMark.SUPERSEDED
    assert lane.dropped == 0
    assert lane.warnings() == ()
    assert lane.failed is False
    got = lane.take()
    assert got is not None
    assert got.event_id == "new"
    assert got.body == b"new"
    assert got.seq == 5
    assert got.recv_ts == 2.0
    assert got.age == 8.0
    assert lane.mark("new") is LogMark.DELIVERED
    assert lane.take() is None


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
def test_latest_conflates_per_feed() -> None:
    """Two tickers do not share a slot. Only the feed that got a second
    print loses its first one."""
    btc = "ticker.Paper_Spot_BTCUSDT"
    eth = "ticker.Paper_Spot_ETHUSDT"
    lane = Delivery(capacity=4)
    lane.accept(_event(StreamKind.TICKER, btc, "a1", seq=1, body=b"a1", recv_ts=1))
    lane.accept(_event(StreamKind.TICKER, eth, "b1", seq=1, body=b"b1", recv_ts=2))
    lane.accept(_event(StreamKind.TICKER, btc, "a2", seq=2, body=b"a2", recv_ts=3))
    assert lane.mark("a1") is LogMark.SUPERSEDED
    assert lane.mark("b1") is None
    assert {event.event_id for event in _drain(lane)} == {"a2", "b1"}


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
def test_kline_keeps_the_latest_per_bar_and_does_not_drop_a_closed_one() -> None:
    """The key is ``(feed, bar_open)``.

    A later update of bar 100 replaces the earlier ones. Bar 160
    arriving *before* bar 100's close does not drop that close: it is a
    different key, and ``take`` yields bars in increasing ``bar_open``.
    The same open on a different feed is a different bar.
    """
    feed = "kline_1m.Paper_Spot_BTCUSDT"
    other = "kline_1m.Paper_Spot_ETHUSDT"
    lane = Delivery(capacity=4)
    lane.accept(
        _event(
            StreamKind.KLINE, feed, "next",
            seq=4, body=b"next", recv_ts=1, bar_open=160, closed=False,
        )
    )
    lane.accept(
        _event(
            StreamKind.KLINE, feed, "open",
            seq=1, body=b"open", recv_ts=2, bar_open=100, closed=False,
        )
    )
    lane.accept(
        _event(
            StreamKind.KLINE, feed, "mid",
            seq=2, body=b"mid", recv_ts=3, bar_open=100, closed=False,
        )
    )
    lane.accept(
        _event(
            StreamKind.KLINE, feed, "close",
            seq=3, body=b"close", recv_ts=4, bar_open=100, closed=True,
        )
    )
    lane.accept(
        _event(
            StreamKind.KLINE, other, "eth",
            seq=1, body=b"eth", recv_ts=5, bar_open=100, closed=True,
        )
    )
    assert lane.mark("open") is LogMark.SUPERSEDED
    assert lane.mark("mid") is LogMark.SUPERSEDED
    assert lane.dropped == 0
    taken = _drain(lane)
    assert [event.event_id for event in taken if event.feed == feed] == [
        "close",
        "next",
    ]
    assert "eth" in {event.event_id for event in taken}
    close = next(event for event in taken if event.event_id == "close")
    assert close.closed is True
    assert close.body == b"close"
    assert close.seq == 3


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
@pytest.mark.parametrize("kind", MARKET_ALL, ids=lambda kind: kind.value)
def test_all_drops_the_oldest_and_the_seq_hole_is_the_loss(kind: StreamKind) -> None:
    """``all``: a bounded queue per feed. Overflow drops the oldest,
    writes a warning, and adds to the drop count.

    The strategy already took seq 1. The queue then holds 2 and 3, and
    4 pushes 2 out. What the strategy sees next is 3, then 4 — the hole
    is seq 2, and it is the strategy's to notice (F25). Nothing here
    records a gap (F23). The dropped event is ``dropped``, not
    ``superseded``, and the session has not failed.
    """
    feed = f"{kind.value}.Paper_Spot_BTCUSDT"
    lane = Delivery(capacity=2)

    def one(seq: int) -> Inbound:
        return _event(
            kind, feed, f"e{seq}", seq=seq, body=bytes([seq]), recv_ts=float(seq)
        )

    lane.accept(one(1))
    first = lane.take()
    assert first is not None and first.seq == 1
    assert lane.mark("e1") is LogMark.DELIVERED

    lane.accept(one(2))
    lane.accept(one(3))
    lane.accept(one(4))
    assert lane.mark("e2") is LogMark.DROPPED
    assert lane.dropped == 1
    assert lane.warnings()
    assert lane.failed is False
    assert [event.seq for event in _drain(lane)] == [3, 4]
    assert lane.mark("e3") is LogMark.DELIVERED
    assert lane.mark("e4") is LogMark.DELIVERED


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
def test_an_all_queue_is_per_feed() -> None:
    """A full BTC queue does not drop the ETH print sitting next to it."""
    lane = Delivery(capacity=1)
    lane.accept(
        _event(StreamKind.TRADE, "trade.Paper_Spot_BTCUSDT", "btc", seq=1, recv_ts=1)
    )
    lane.accept(
        _event(StreamKind.TRADE, "trade.Paper_Spot_ETHUSDT", "eth", seq=1, recv_ts=2)
    )
    assert lane.dropped == 0
    assert lane.mark("btc") is None
    assert lane.mark("eth") is None
    assert {event.event_id for event in _drain(lane)} == {"btc", "eth"}


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
def test_a_feed_override_to_all_does_not_conflate() -> None:
    """``delivery: all`` on a ticker feed replaces the ``latest`` default.
    Both prints are delivered, in arrival order, and neither is
    superseded."""
    feed = "ticker.Paper_Spot_BTCUSDT"
    lane = Delivery(capacity=4, overrides={feed: DELIVERY_ALL})
    lane.accept(_event(StreamKind.TICKER, feed, "a", seq=1, body=b"a", recv_ts=1))
    lane.accept(_event(StreamKind.TICKER, feed, "b", seq=2, body=b"b", recv_ts=2))
    assert lane.mark("a") is None
    assert [event.event_id for event in _drain(lane)] == ["a", "b"]
    assert lane.dropped == 0


@pytest.mark.xfail(strict=True, reason=_B5_DELIVERY)
@pytest.mark.parametrize("kind", MUST_DELIVER, ids=lambda kind: kind.value)
def test_a_must_deliver_overflow_fails_the_session_and_drops_nothing(
    kind: StreamKind,
) -> None:
    """TD events, ``feed_end`` and RPC replies are ``all`` and are not
    dropped. Past ``capacity`` the session fails. The event that did
    not fit was never accepted, so it has no mark. The ones that did
    fit are still taken, in order, and the drop count stays 0.

    The TD case is the row the ticket names. The other two are the
    same sentence in the table.
    """
    lane = Delivery(capacity=2)

    def one(seq: int) -> Inbound:
        return _event(
            kind, kind.value, f"e{seq}", body=bytes([seq]), recv_ts=float(seq)
        )

    lane.accept(one(1))
    lane.accept(one(2))
    with pytest.raises(SessionFailed) as caught:
        lane.accept(one(3))
    assert caught.value.reason == f"{kind.value}_overflow"
    assert lane.failed is True
    assert lane.fail_reason == f"{kind.value}_overflow"
    assert lane.dropped == 0
    assert lane.mark("e3") is None
    assert lane.mark("e1") is not LogMark.DROPPED
    with pytest.raises(SessionFailed):
        lane.accept(one(4))
    assert [event.event_id for event in _drain(lane)] == ["e1", "e2"]


@pytest.mark.xfail(strict=True, reason=_B5_LOG)
def test_a_superseded_event_is_marked_on_its_log_record() -> None:
    """The mark is not only an in-memory query. The line the writer
    will append carries it: ``superseded`` once the newer print
    arrives, ``delivered`` once the strategy thread takes what remains.
    Logged at receive, so the body and ``recv_ts`` are the ones the
    ingress saw, not a decode."""
    feed = "ticker.Paper_Spot_BTCUSDT"
    lane = Delivery(capacity=4)
    lane.accept(_event(StreamKind.TICKER, feed, "old", seq=1, body=b"old", recv_ts=1))
    lane.accept(_event(StreamKind.TICKER, feed, "new", seq=2, body=b"new", recv_ts=2))
    by_id = {record.event_id: record for record in lane.log_records()}
    assert by_id["old"].mark is LogMark.SUPERSEDED
    assert by_id["old"].body == b"old"
    assert by_id["old"].recv_ts == 1
    assert by_id["old"].event_seq == 1
    assert by_id["new"].mark is None
    lane.take()
    by_id = {record.event_id: record for record in lane.log_records()}
    assert by_id["new"].mark is LogMark.DELIVERED

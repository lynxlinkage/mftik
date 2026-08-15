"""Session event log — what lands in the jsonl, and what happens under load."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange.models import Order, OrderStatus, OrderType, Side, Ticker
from mft.exchange.oms import LedgerEntry
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    MD_TICKER,
    TD_LEASE_ACK,
    TD_LEVERAGE_ACK,
    TD_ORDER_ACK,
    LeaseAck,
    LeverageAck,
    LeverageAckEnvelope,
    OrderAck,
    OrderAckEnvelope,
    SymbolFilterInfo,
    SymbolInfo,
    Topics,
    UntypedEnvelope,
)
from mft_sts import tape as tape_module
from mft_sts.eventlog import DIR_ENV, EventLog
from mft_sts.session.session import StsSession
from mft_sts.strategy import Strategy


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-eventlog"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


def _read(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _events(records: list[dict], kind: str) -> list[dict]:
    return [r for r in records if r["kind"] == kind]


def _ticker_payload() -> dict:
    return Ticker(
        universal_ticker="Paper_Spot_BTCUSDT",
        bid=Decimal("100"),
        ask=Decimal("101"),
        last=Decimal("100.5"),
    ).model_dump(mode="json")


# --- the writer -----------------------------------------------------------


async def test_unset_dir_leaves_no_log(monkeypatch, tmp_path: Path) -> None:
    """The default is off, and off has to be free of side effects."""
    monkeypatch.delenv(DIR_ENV, raising=False)
    log = EventLog.from_env("s1")

    assert not log.enabled
    assert log.path is None
    await log.start()
    log.record("md", "md.ticker", payload={"a": 1})
    await log.close()

    assert list(tmp_path.iterdir()) == []


async def test_records_are_one_json_object_per_line(tmp_path: Path) -> None:
    log = EventLog("s1", directory=tmp_path)
    await log.start()
    log.record("md", "md.ticker", payload={"bid": "100"})
    log.record("order", "sts.order.submit", dir="out", cid="7")
    await log.close()

    records = _read(tmp_path / "s1.jsonl")
    assert [r["event"] for r in records] == [
        "md.ticker",
        "sts.order.submit",
        "closed",
    ]
    assert [r["seq"] for r in records] == [1, 2, 3]
    assert records[0]["dir"] == "in"
    assert records[0]["payload"] == {"bid": "100"}
    assert records[1]["dir"] == "out"
    assert records[1]["cid"] == "7"
    # A None field is absent rather than present-and-null.
    assert "api_id" not in records[0]
    assert all(r["session"] == "s1" for r in records)


async def test_decimals_and_models_survive_serialization(
    tmp_path: Path,
) -> None:
    """Prices must come back as they went out — not as binary floats."""
    log = EventLog("s1", directory=tmp_path)
    await log.start()
    log.record(
        "order",
        "sts.order.submit",
        dir="out",
        price=Decimal("0.1"),
        side=Side.BUY,
        payload=Ticker(
            universal_ticker="Paper_Spot_BTCUSDT",
            bid=Decimal("100"),
            ask=Decimal("101"),
            last=Decimal("100.5"),
        ),
    )
    await log.close()

    record = _read(tmp_path / "s1.jsonl")[0]
    assert record["price"] == "0.1"
    assert record["side"] == "buy"
    assert record["payload"]["bid"] == "100"


async def test_dropped_records_are_counted_into_the_file(
    tmp_path: Path,
) -> None:
    """A full queue leaves a hole, and the hole has to be readable.

    Recorded before ``start`` so nothing is draining: the queue fills, and the
    overflow is what a stalled disk would produce under a busy feed.
    """
    log = EventLog("s1", directory=tmp_path, queue_size=2)
    for index in range(5):
        log.record("md", "md.ticker", payload={"n": index})
    assert log.dropped == 3

    await log.start()
    await log.close()

    records = _read(tmp_path / "s1.jsonl")
    kept = [r for r in records if r["event"] == "md.ticker"]
    assert [r["payload"]["n"] for r in kept] == [0, 1]
    # The numbering says three are missing...
    assert [r["seq"] for r in kept] == [1, 2]
    # ...and a line says so outright, rather than leaving it to be noticed.
    markers = [r for r in records if r["event"] == "dropped"]
    assert len(markers) == 1
    assert markers[0]["count"] == 3
    assert markers[0]["kind"] == "eventlog"


async def test_rotation_keeps_the_configured_backups(tmp_path: Path) -> None:
    log = EventLog("s1", directory=tmp_path, max_bytes=200, backups=1)
    await log.start()
    for index in range(40):
        log.record("md", "md.ticker", payload={"n": index, "pad": "x" * 40})
        # One batch per record, so the size check runs between writes.
        await asyncio.sleep(0)
    await log.close()

    assert (tmp_path / "s1.jsonl").exists()
    assert (tmp_path / "s1.jsonl.1").exists()
    # backups=1, so nothing older than .1 is kept.
    assert not (tmp_path / "s1.jsonl.2").exists()


async def test_session_id_cannot_escape_the_directory(tmp_path: Path) -> None:
    """session_id arrives from an API request and becomes a file name."""
    log = EventLog("../../etc/passwd", directory=tmp_path)

    assert log.path is not None
    assert log.path.parent == tmp_path
    assert "/" not in log.path.name


async def test_unwritable_directory_does_not_fail_the_session(
    tmp_path: Path,
) -> None:
    """A session that cannot write its log still trades."""
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory")
    log = EventLog("s1", directory=blocker / "sub")

    await log.start()
    log.record("md", "md.ticker")
    await log.close()

    assert not log.enabled


# --- the session ----------------------------------------------------------


class ProbeStrategy(Strategy):
    name = "eventlog_probe"
    id = 97

    def __init__(self) -> None:
        super().__init__()
        self.seen: list[Ticker] = []

    async def on_ticker(self, ticker: Ticker) -> None:
        self.seen.append(ticker)


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition not met")


def _session(
    broker: Broker, tmp_path: Path, strategy: Strategy, **kwargs
) -> StsSession:
    session_id = kwargs.pop("session_id", "ev-1")
    return StsSession(
        session_id=session_id,
        broker=broker,
        created_by=1,
        strategy=strategy,
        heartbeat_interval=0.1,
        event_log=EventLog(session_id, directory=tmp_path),
        **kwargs,
    )


async def test_session_records_lifecycle_and_market_data(
    broker: Broker, tmp_path: Path
) -> None:
    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, md_ids=["ticker.Paper_Spot_BTCUSDT"]
    )
    await sts.start()
    await asyncio.sleep(0.05)

    await broker.publish(
        Topics.md_session("ev-1"),
        UntypedEnvelope.wrap(_ticker_payload(), type=MD_TICKER, source="md"),
    )
    await _wait_until(lambda: bool(strategy.seen))
    await sts.stop()

    records = _read(tmp_path / "ev-1.jsonl")
    events = [r["event"] for r in records]
    assert events[:3] == ["session_start", "on_start", "on_ready"]
    # The detach records sit between these two, so order rather than position.
    assert events.index("on_stop") < events.index("session_stop")
    assert events[-2:] == ["session_stop", "closed"]

    md = _events(records, "md")
    assert len(md) == 1
    assert md[0]["event"] == MD_TICKER
    assert md[0]["hook"] == "on_ticker"
    assert md[0]["payload"]["bid"] == "100"
    # Both clocks, so the wire latency is a subtraction on one line.
    assert md[0]["sent_ts"] <= md[0]["ts"]
    assert md[0]["source"] == "md"

    start = _events(records, "lifecycle")[0]
    assert start["strategy"] == "eventlog_probe"
    assert start["md"] == ["ticker.Paper_Spot_BTCUSDT"]


async def test_hook_failure_is_recorded_against_the_event(
    broker: Broker, tmp_path: Path
) -> None:
    """The strategy swallowed it; the log should not."""

    class BrokenStrategy(ProbeStrategy):
        async def on_ticker(self, ticker: Ticker) -> None:
            raise RuntimeError("boom")

    sts = _session(
        broker,
        tmp_path,
        BrokenStrategy(),
        md_ids=["ticker.Paper_Spot_BTCUSDT"],
        session_id="ev-broken",
    )
    await sts.start()
    await asyncio.sleep(0.05)

    await broker.publish(
        Topics.md_session("ev-broken"),
        UntypedEnvelope.wrap(_ticker_payload(), type=MD_TICKER, source="md"),
    )
    await _wait_until(
        lambda: (tmp_path / "ev-broken.jsonl").exists()
        and any(
            r["kind"] == "error" for r in _read(tmp_path / "ev-broken.jsonl")
        )
    )
    await sts.stop()

    records = _read(tmp_path / "ev-broken.jsonl")
    failures = _events(records, "error")
    assert len(failures) == 1
    assert failures[0]["event"] == "hook_failed"
    # STS noticing something, not a message from anyone.
    assert failures[0]["dir"] == "self"
    assert failures[0]["hook"] == "on_ticker"
    assert "boom" in failures[0]["error"]
    # The event that caused it is on its own line, ahead of the failure.
    md = _events(records, "md")
    assert md[0]["seq"] < failures[0]["seq"]
    assert md[0]["env_id"] == failures[0]["env_id"]


async def test_unhandled_message_is_recorded(
    broker: Broker, tmp_path: Path
) -> None:
    """No hook claims it, so the process log says nothing above DEBUG."""
    sts = _session(
        broker,
        tmp_path,
        ProbeStrategy(),
        md_ids=["ticker.Paper_Spot_BTCUSDT"],
        session_id="ev-unhandled",
    )
    await sts.start()
    await asyncio.sleep(0.05)

    await broker.publish(
        Topics.md_session("ev-unhandled"),
        UntypedEnvelope.wrap({}, type="md.something_new", source="md"),
    )
    await _wait_until(
        lambda: (tmp_path / "ev-unhandled.jsonl").exists()
        and any(
            r["kind"] == "unhandled"
            for r in _read(tmp_path / "ev-unhandled.jsonl")
        )
    )
    await sts.stop()

    unhandled = _events(_read(tmp_path / "ev-unhandled.jsonl"), "unhandled")
    assert unhandled[0]["event"] == "md.something_new"
    assert unhandled[0]["peer"] == "md"


async def test_lease_ack_is_recorded(broker: Broker, tmp_path: Path) -> None:
    """The nearest thing to a login: TD has accepted this session's lease."""
    sts = _session(broker, tmp_path, ProbeStrategy(), td_api_ids=[11],
                   session_id="ev-lease")
    await sts.start()
    await asyncio.sleep(0.05)

    await broker.publish(
        Topics.td_session(11, "ev-lease"),
        UntypedEnvelope.wrap(
            LeaseAck(session_id="ev-lease", api_id=11, token=1).model_dump(
                mode="json"
            ),
            type=TD_LEASE_ACK,
            source="td",
        ),
    )
    await _wait_until(
        lambda: (tmp_path / "ev-lease.jsonl").exists()
        and any(
            r["kind"] == "lease" for r in _read(tmp_path / "ev-lease.jsonl")
        )
    )
    await sts.stop()

    lease = _events(_read(tmp_path / "ev-lease.jsonl"), "lease")
    assert lease[0]["event"] == TD_LEASE_ACK
    assert lease[0]["api_id"] == 11


async def test_order_submit_and_ack_are_both_recorded(
    broker: Broker, tmp_path: Path
) -> None:
    """The outbound half — without it a fill has nothing to be traced to."""
    stop = asyncio.Event()
    seen: list[str] = []

    async def fake_td() -> None:
        async for req in broker.serve(Topics.td_order(11), stop=stop):
            seen.append(req.envelope.type)
            payload = req.envelope.payload
            await req.reply(
                OrderAckEnvelope.wrap(
                    OrderAck(
                        api_id=11,
                        client_order_id=payload["client_order_id"],
                        accepted=True,
                    ),
                    type=TD_ORDER_ACK,
                    source="td",
                )
            )
            return

    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, td_api_ids=[11], session_id="ev-order"
    )
    await sts.start()
    server = asyncio.create_task(fake_td())
    await asyncio.sleep(0.05)

    accepted = await strategy.oms.submit_order(
        11,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.5"),
        price=Decimal("100"),
    )
    assert accepted is True
    cid = strategy.oms.last_client_order_id
    stop.set()
    await server
    await sts.stop()

    orders = _events(_read(tmp_path / "ev-order.jsonl"), "order")
    assert [r["event"] for r in orders] == ["sts.order.submit", "order_ack"]
    assert orders[0]["dir"] == "out"
    assert orders[0]["cid"] == cid
    assert orders[0]["payload"]["qty"] == "0.5"
    assert orders[0]["payload"]["side"] == "buy"
    assert orders[1]["accepted"] is True
    assert orders[1]["cid"] == cid


async def test_order_with_no_td_records_the_refusal(
    broker: Broker, tmp_path: Path
) -> None:
    """Nothing is serving the account: the log must show the attempt anyway."""
    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, td_api_ids=[12], session_id="ev-noack"
    )
    strategy.oms._ack_timeout = 0.2
    await sts.start()

    accepted = await strategy.oms.submit_order(
        12,
        ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        qty=Decimal("1"),
        price=Decimal("100"),
    )
    assert accepted is False
    await sts.stop()

    orders = _events(_read(tmp_path / "ev-noack.jsonl"), "order")
    assert [r["event"] for r in orders] == ["sts.order.submit", "order_ack"]
    assert orders[1]["accepted"] is False
    assert orders[1]["reason"] == "no ack from TD"


# --- the reads ------------------------------------------------------------
#
# Every one of these goes straight to Redis rather than arriving as an event,
# so without a record of the answer the log cannot say what the strategy knew.


async def test_oms_view_records_the_book_it_was_given(
    broker: Broker, tmp_path: Path
) -> None:
    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, td_api_ids=[11], session_id="ev-oms-read"
    )
    await sts.start()
    await broker.state_put(
        Topics.td_oms(11),
        "555",
        Order(
            client_order_id="555",
            universal_ticker="Paper_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("2"),
            price=Decimal("99"),
            status=OrderStatus.NEW,
        ),
    )

    view = await strategy.oms.view()
    assert set(view.orders) == {"555"}
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-oms-read.jsonl"), "read")
    assert [r["event"] for r in reads] == ["oms.view"]
    assert reads[0]["dir"] == "out"
    assert reads[0]["api_id"] == 11
    assert reads[0]["count"] == 1
    # The rows themselves, so TD's book at that moment can be rebuilt.
    assert reads[0]["payload"]["555"]["price"] == "99"


async def test_oms_order_records_a_miss_as_a_miss(
    broker: Broker, tmp_path: Path
) -> None:
    """None means "not live" here, and that is a fact worth keeping."""
    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, td_api_ids=[11], session_id="ev-oms-miss"
    )
    await sts.start()

    assert await strategy.oms.order("nope") is None
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-oms-miss.jsonl"), "read")
    assert reads[0]["event"] == "oms.order"
    assert reads[0]["cid"] == "nope"
    assert reads[0]["found"] is False
    assert "payload" not in reads[0]


async def test_ledger_view_records_the_balances(
    broker: Broker, tmp_path: Path
) -> None:
    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, td_api_ids=[11], session_id="ev-ledger"
    )
    await sts.start()
    await broker.state_put(
        Topics.td_ledger(11),
        "USDT",
        LedgerEntry(
            free=Decimal("1000"), prelock=Decimal("400"), lock=Decimal("0")
        ),
    )

    assert await strategy.ledger.available("USDT") == Decimal("600")
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-ledger.jsonl"), "read")
    assert [r["event"] for r in reads] == ["ledger.view"]
    # 600 is derived; 1000 and 400 are what TD actually said.
    assert reads[0]["payload"]["USDT"]["free"] == "1000"
    assert reads[0]["payload"]["USDT"]["prelock"] == "400"


async def test_leverage_cache_hit_is_recorded_too(
    broker: Broker, tmp_path: Path
) -> None:
    """The second call never leaves the process, and still decides an order."""
    stop = asyncio.Event()
    calls: list[str] = []

    async def fake_td() -> None:
        async for req in broker.serve(Topics.td_account(11), stop=stop):
            calls.append(req.envelope.type)
            await req.reply(
                LeverageAckEnvelope.wrap(
                    LeverageAck(
                        api_id=11,
                        universal_ticker="Bybit_Perp_BTCUSDT",
                        ok=True,
                        leverage=Decimal("10"),
                    ),
                    type=TD_LEVERAGE_ACK,
                    source="td",
                )
            )

    strategy = ProbeStrategy()
    sts = _session(
        broker, tmp_path, strategy, td_api_ids=[11], session_id="ev-lev"
    )
    await sts.start()
    server = asyncio.create_task(fake_td())
    await asyncio.sleep(0.05)

    first = await strategy.ledger.ensure_leverage("Bybit_Perp_BTCUSDT")
    second = await strategy.ledger.ensure_leverage("Bybit_Perp_BTCUSDT")
    assert first == second == Decimal("10")
    assert len(calls) == 1
    stop.set()
    server.cancel()
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-lev.jsonl"), "read")
    assert [r["event"] for r in reads] == [
        "ledger.leverage",
        "ledger.leverage",
    ]
    assert [r["leverage"] for r in reads] == ["10", "10"]
    assert "cached" not in reads[0]
    assert reads[1]["cached"] is True


async def test_symbol_reads_are_recorded(
    broker: Broker, tmp_path: Path
) -> None:
    """A tick size is as much an input to an order as the price is."""
    info = SymbolInfo(
        universal_ticker="Paper_Spot_BTCUSDT",
        base="BTC",
        quote="USDT",
        exch_ticker="BTCUSDT",
        filters=[SymbolFilterInfo(name="price_tick", value=Decimal("0.01"))],
    )

    class FakeSymbols:
        async def get(self, ticker):  # noqa: ANN001, ANN202
            return info

        async def filter(self, ticker, name):  # noqa: ANN001, ANN202
            return info.filter(name)

    strategy = ProbeStrategy()
    sts = _session(
        broker,
        tmp_path,
        strategy,
        session_id="ev-sym",
        symbols=FakeSymbols(),
    )
    await sts.start()

    assert (await strategy.symbols.get("Paper_Spot_BTCUSDT")) is info
    assert (
        await strategy.symbols.filter("Paper_Spot_BTCUSDT", "price_tick")
    ) == Decimal("0.01")
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-sym.jsonl"), "read")
    assert [r["event"] for r in reads] == ["symbols.get", "symbols.filter"]
    assert reads[0]["payload"]["exch_ticker"] == "BTCUSDT"
    assert reads[1]["filter"] == "price_tick"
    assert reads[1]["payload"] == "0.01"


async def test_tape_read_records_the_prints_not_just_the_coverage(
    broker: Broker, tmp_path: Path, monkeypatch
) -> None:  # noqa: ANN001
    """MD's tape is the only copy and it expires — so this one must be kept."""
    monkeypatch.setattr(tape_module, "LOG_CHUNK", 2)
    feed = Topics.md_feed("aggtrade", UniversalTicker.parse("Paper_Spot_BTCUSDT"))
    await broker.tape_mark_recording(feed, since_ms=1, ttl_seconds=3600)
    for index in range(3):
        await broker.tape_append(
            feed,
            {
                "trade_id": str(index),
                "price": f"6800{index}",
                "qty": "0.5",
                "side": "buy",
                "ts": "1700000000.5",
                "first_trade_id": str(index),
                "last_trade_id": str(index),
            },
            maxlen=1000,
            ttl_seconds=3600,
        )

    strategy = ProbeStrategy()
    sts = _session(broker, tmp_path, strategy, session_id="ev-tape")
    await sts.start()

    slice_ = await strategy.tape.read("Paper_Spot_BTCUSDT")
    assert len(slice_) == 3
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-tape.jsonl"), "read")
    summary = [r for r in reads if r["event"] == "tape.read"][0]
    assert summary["records"] == 3
    assert summary["logged"] == 3
    assert "truncated" not in summary
    # Chunked, oldest first, and the prints are actually there.
    chunks = [r for r in reads if r["event"] == "tape.records"]
    assert [(c["offset"], c["count"]) for c in chunks] == [(0, 2), (2, 1)]
    prices = [row["price"] for c in chunks for row in c["payload"]]
    assert prices == ["68000", "68001", "68002"]


async def test_a_capped_tape_read_says_it_was_capped(
    broker: Broker, tmp_path: Path, monkeypatch
) -> None:  # noqa: ANN001
    """Short is acceptable; short and silent about it is not."""
    monkeypatch.setattr(tape_module, "LOG_CHUNK", 2)
    monkeypatch.setattr(tape_module, "LOG_MAX_RECORDS", 2)
    feed = Topics.md_feed("aggtrade", UniversalTicker.parse("Paper_Spot_BTCUSDT"))
    await broker.tape_mark_recording(feed, since_ms=1, ttl_seconds=3600)
    for index in range(3):
        await broker.tape_append(
            feed,
            {
                "trade_id": str(index),
                "price": f"6800{index}",
                "qty": "0.5",
                "side": "buy",
                "ts": "1700000000.5",
                "first_trade_id": str(index),
                "last_trade_id": str(index),
            },
            maxlen=1000,
            ttl_seconds=3600,
        )

    strategy = ProbeStrategy()
    sts = _session(broker, tmp_path, strategy, session_id="ev-tape-cap")
    await sts.start()

    slice_ = await strategy.tape.read("Paper_Spot_BTCUSDT")
    assert len(slice_) == 3  # the strategy still gets all of them
    await sts.stop()

    reads = _events(_read(tmp_path / "ev-tape-cap.jsonl"), "read")
    summary = [r for r in reads if r["event"] == "tape.read"][0]
    assert summary["records"] == 3
    assert summary["logged"] == 2
    assert summary["truncated"] is True
    chunks = [r for r in reads if r["event"] == "tape.records"]
    assert sum(c["count"] for c in chunks) == 2


async def test_timer_ticks_are_recorded_under_their_label(
    broker: Broker, tmp_path: Path
) -> None:
    ticks: list[int] = []

    class TimerStrategy(ProbeStrategy):
        async def on_start(self) -> None:
            self.timer.token().register(
                self.timer.now_ms(), 20, self._tick, label="probe"
            )

        async def _tick(self) -> None:
            ticks.append(1)

    sts = _session(
        broker, tmp_path, TimerStrategy(), session_id="ev-timer"
    )
    await sts.start()
    await _wait_until(lambda: len(ticks) >= 2)
    await sts.stop()

    timer = _events(_read(tmp_path / "ev-timer.jsonl"), "timer")
    assert timer[0]["event"] == "registered"
    assert timer[0]["token"] == "probe"
    assert timer[0]["interval_ms"] == 20
    fired = [r for r in timer if r["event"] == "fired"]
    assert len(fired) >= 2
    assert all(r["token"] == "probe" for r in fired)
    # The tick it was due at, so a late fire is visible as a late fire.
    assert fired[1]["due_ms"] - fired[0]["due_ms"] == 20

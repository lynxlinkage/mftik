"""StrategyTape — reading MD's recording back as the models the hooks use.

The load-bearing behaviour is the gap: records from before the recording
restarted are not part of the series, and handing them over as if they were is
how a warm-up ends up describing a market that had a hole in it.
"""

from __future__ import annotations

import ast
import asyncio
from decimal import Decimal
from pathlib import Path

import fakeredis.aioredis
import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.models import AggTrade, Side, Trade
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import Topics
from mftik.strategy.eventlog import EventLog
from mftik.strategy.tape import StrategyTape, TapeFeedNotAttached
from mftik_md.rpc.tape import TAPE_RPC_CHUNK
from mftik_md.tape_store import TapeStore
from tape_rpc import serve_tape

TICKER = UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")
OTHER = UniversalTicker.parse("BinanceUM_Perp_ETHUSDT")
AGG_FEED = Topics.md_feed("aggtrade", TICKER)
TRADE_FEED = Topics.md_feed("trade", TICKER)
INSTANCE = "md-jp"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def store() -> TapeStore:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    store = TapeStore(redis)
    try:
        yield store
    finally:
        await store.aclose()


class _Session:
    def __init__(
        self,
        broker: Broker,
        *,
        md: dict[str, list[str]] | None = None,
        md_owners: dict[str, str] | None = None,
    ) -> None:
        self.broker = broker
        self.event_log = EventLog("tape-read", directory=None)
        self.md = md or {
            INSTANCE: [AGG_FEED, TRADE_FEED],
        }
        self.md_owners = md_owners or {}


class _Strategy:
    def __init__(self, session: _Session) -> None:
        self.session = session


def _tape(broker: Broker, **kwargs) -> StrategyTape:
    tape = StrategyTape()
    tape.bind(_Strategy(_Session(broker, **kwargs)))  # type: ignore[arg-type]
    return tape


async def _record(
    store: TapeStore,
    feed: str,
    trade_id: str,
    price: str,
    *,
    agg: bool = True,
    recorded_ms: int | None = None,
) -> None:
    fields = {
        "trade_id": trade_id,
        "price": price,
        "qty": "0.5",
        "side": "buy",
        "ts": "1700000000.5",
    }
    if agg:
        fields["first_trade_id"] = trade_id
        fields["last_trade_id"] = trade_id
    await store.append(
        feed, fields, maxlen=1000, ttl_seconds=3600, recorded_ms=recorded_ms
    )


@pytest.mark.asyncio
async def test_reads_back_as_aggtrade_models(
    broker: Broker, store: TapeStore
) -> None:
    """Same type the live hook is handed, so one code path serves both."""
    await store.mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(store, AGG_FEED, "1", "68000")

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert len(result) == 1
    record = result.records[0]
    assert isinstance(record, AggTrade)
    assert record.price == Decimal("68000")
    assert record.side is Side.BUY
    assert record.universal_ticker == str(TICKER)
    assert record.first_trade_id == "1"


@pytest.mark.asyncio
async def test_trade_topic_reads_back_as_trade(
    broker: Broker, store: TapeStore
) -> None:
    await store.mark_recording(TRADE_FEED, since_ms=1, ttl_seconds=3600)
    await _record(store, TRADE_FEED, "1", "68000", agg=False)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER, topic="trade")
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert len(result) == 1
    assert type(result.records[0]) is Trade


@pytest.mark.asyncio
async def test_records_from_before_a_gap_are_dropped(
    broker: Broker, store: TapeStore
) -> None:
    """Reading across a hole is the failure this whole mechanism prevents."""
    await _record(store, AGG_FEED, "old", "1", recorded_ms=1_000)
    await store.mark_recording(AGG_FEED, since_ms=1_001, ttl_seconds=3600)
    await _record(store, AGG_FEED, "new", "2", recorded_ms=2_000)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["new"]
    assert result.dropped_before_gap == 1


@pytest.mark.asyncio
async def test_coverage_is_reported(broker: Broker, store: TapeStore) -> None:
    await store.mark_recording(AGG_FEED, since_ms=99, ttl_seconds=3600)
    await _record(store, AGG_FEED, "1", "1")

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert result.continuous_since_ms == 99
    assert result.recording is True


@pytest.mark.asyncio
async def test_a_stopped_feed_says_so(broker: Broker, store: TapeStore) -> None:
    """History that ends in the past is still history — but it ends."""
    await store.mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(store, AGG_FEED, "1", "1")
    await store.mark_stopped(AGG_FEED, at_ms=2, ttl_seconds=3600)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert len(result) == 1
    assert result.recording is False


@pytest.mark.asyncio
async def test_nothing_recorded_is_an_empty_slice_not_an_error(
    broker: Broker, store: TapeStore
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(result) == 0
    assert result.continuous_since_ms is None
    assert result.recording is False


@pytest.mark.asyncio
async def test_one_unreadable_record_does_not_lose_the_read(
    broker: Broker, store: TapeStore
) -> None:
    await store.mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(store, AGG_FEED, "1", "68000")
    await store.append(
        AGG_FEED,
        {"trade_id": "2", "price": "nonsense", "qty": "1", "side": "buy",
         "ts": "1"},
        maxlen=1000,
        ttl_seconds=3600,
    )
    await _record(store, AGG_FEED, "3", "68100")

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["1", "3"]


@pytest.mark.asyncio
async def test_limit_takes_the_most_recent(
    broker: Broker, store: TapeStore
) -> None:
    await store.mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    for n in range(5):
        await _record(store, AGG_FEED, str(n), str(68000 + n), recorded_ms=100 + n)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER, limit=2)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["3", "4"]


async def _interrupted(
    store: TapeStore, *, stopped_ms: int, resumed_ms: int
) -> None:
    """One clean stop/start cycle — the shape a deploy leaves behind."""
    await store.mark_stopped(AGG_FEED, at_ms=stopped_ms, ttl_seconds=3600)
    await store.mark_recording(AGG_FEED, since_ms=resumed_ms, ttl_seconds=3600)


@pytest.mark.asyncio
async def test_a_short_measured_gap_is_read_across_and_reported(
    broker: Broker, store: TapeStore
) -> None:
    """A deploy costs seconds. Ending the series over it costs the warm-up."""
    await store.mark_recording(AGG_FEED, since_ms=1_000, ttl_seconds=3600)
    await _record(store, AGG_FEED, "old", "68000", recorded_ms=2_000)
    await _interrupted(store, stopped_ms=3_000, resumed_ms=5_000)
    await _record(store, AGG_FEED, "new", "68000", recorded_ms=6_000)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["old", "new"]
    assert result.dropped_before_gap == 0
    assert [(g.start_ms, g.end_ms) for g in result.gaps] == [(3_000, 5_000)]
    assert result.missing_ms == 2_000


@pytest.mark.asyncio
async def test_a_gap_too_long_to_span_still_ends_the_series(
    broker: Broker, store: TapeStore
) -> None:
    """Measured is not the same as tolerable. An outage is still an outage."""
    await store.mark_recording(AGG_FEED, since_ms=1_000, ttl_seconds=3600)
    await _record(store, AGG_FEED, "old", "68000", recorded_ms=2_000)
    await _interrupted(store, stopped_ms=3_000, resumed_ms=123_000)
    await _record(store, AGG_FEED, "new", "68000", recorded_ms=130_000)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["new"]
    assert result.dropped_before_gap == 1
    assert result.gaps == []


@pytest.mark.asyncio
async def test_a_caller_can_refuse_every_gap(
    broker: Broker, store: TapeStore
) -> None:
    """``max_gap_ms=0`` is the absolute rule this used to apply to everyone."""
    await store.mark_recording(AGG_FEED, since_ms=1_000, ttl_seconds=3600)
    await _record(store, AGG_FEED, "old", "68000", recorded_ms=2_000)
    await _interrupted(store, stopped_ms=3_000, resumed_ms=5_000)
    await _record(store, AGG_FEED, "new", "68000", recorded_ms=6_000)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER, max_gap_ms=0)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["new"]
    assert result.dropped_before_gap == 1


@pytest.mark.asyncio
async def test_a_gap_the_records_no_longer_reach_is_not_reported(
    broker: Broker, store: TapeStore
) -> None:
    """Trimming ages a hole out of the answer along with the prints around it."""
    await store.mark_recording(AGG_FEED, since_ms=1_000, ttl_seconds=3600)
    await _interrupted(store, stopped_ms=3_000, resumed_ms=5_000)
    await _record(store, AGG_FEED, "new", "68000", recorded_ms=6_000)

    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["new"]
    assert result.dropped_before_gap == 0
    assert result.gaps == []


@pytest.mark.asyncio
async def test_an_unattached_feed_raises(
    broker: Broker, store: TapeStore
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, store, instance=INSTANCE, stop=stop))
    try:
        with pytest.raises(TapeFeedNotAttached):
            await _tape(broker).read(OTHER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_star_mapping_does_not_invent_an_owner(
    broker: Broker, store: TapeStore
) -> None:
    with pytest.raises(TapeFeedNotAttached):
        await _tape(broker, md={"*": [AGG_FEED]}).read(TICKER)


@pytest.mark.asyncio
async def test_the_wrong_instance_returns_an_empty_slice(
    broker: Broker, store: TapeStore
) -> None:
    """JP tape lives on JP Redis. Asking TW is empty, not a fallback."""
    await store.mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(store, AGG_FEED, "1", "68000")

    empty = TapeStore(fakeredis.aioredis.FakeRedis(decode_responses=True))
    stop = asyncio.Event()
    task = asyncio.create_task(serve_tape(broker, empty, instance="md-tw", stop=stop))
    try:
        result = await _tape(broker, md={"md-tw": [AGG_FEED]}).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await empty.aclose()

    assert len(result) == 0
    assert result.recording is False


@pytest.mark.asyncio
async def test_a_read_assembles_chunks_into_one_slice(
    broker: Broker, store: TapeStore, monkeypatch
) -> None:
    monkeypatch.setattr("mftik_md.rpc.tape.TAPE_RPC_CHUNK", 2)
    await store.mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    for n in range(5):
        await _record(store, AGG_FEED, str(n), str(68000 + n), recorded_ms=100 + n)

    stop = asyncio.Event()
    task = asyncio.create_task(
        serve_tape(broker, store, instance=INSTANCE, stop=stop, chunk=2)
    )
    try:
        result = await _tape(broker).read(TICKER)
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert [r.trade_id for r in result.records] == ["0", "1", "2", "3", "4"]
    assert TAPE_RPC_CHUNK > 2  # production chunk stays large


def test_sts_and_strategy_do_not_import_redis() -> None:
    """The regional disk is MD's. A session in TW must not open JP Redis."""
    roots = [
        Path(__file__).resolve().parents[1] / "src",
        Path(__file__).resolve().parents[3]
        / "packages"
        / "common"
        / "src"
        / "mftik"
        / "strategy",
    ]
    leaks: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.partition(".")[0] == "redis":
                            leaks.append(f"{path}:{node.lineno}")
                elif isinstance(node, ast.ImportFrom):
                    if node.module and node.module.partition(".")[0] == "redis":
                        leaks.append(f"{path}:{node.lineno}")
    assert leaks == []

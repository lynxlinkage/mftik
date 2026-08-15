"""StrategyTape — reading MD's recording back as the models the hooks use.

The load-bearing behaviour is the gap: records from before the recording
restarted are not part of the series, and handing them over as if they were is
how a warm-up ends up describing a market that had a hole in it.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange.models import AggTrade, Side, Trade
from mft.exchange.tickers import UniversalTicker
from mft.protocol import Topics
from mft_sts.eventlog import EventLog
from mft_sts.tape import StrategyTape

TICKER = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")
AGG_FEED = Topics.md_feed("aggtrade", TICKER)
TRADE_FEED = Topics.md_feed("trade", TICKER)


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


class _Session:
    def __init__(self, broker: Broker) -> None:
        self.broker = broker
        # Disabled, as it is in any deployment without STS_EVENTLOG_DIR — the
        # read still logs what it covered, to nowhere.
        self.event_log = EventLog("tape-read", directory=None)


class _Strategy:
    def __init__(self, broker: Broker) -> None:
        self.session = _Session(broker)


def _tape(broker: Broker) -> StrategyTape:
    tape = StrategyTape()
    tape.bind(_Strategy(broker))  # type: ignore[arg-type]
    return tape


async def _record(
    broker: Broker,
    feed: str,
    trade_id: str,
    price: str,
    *,
    agg: bool = True,
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
    await broker.tape_append(feed, fields, maxlen=1000, ttl_seconds=3600)


@pytest.mark.asyncio
async def test_reads_back_as_aggtrade_models(broker: Broker) -> None:
    """Same type the live hook is handed, so one code path serves both."""
    await broker.tape_mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(broker, AGG_FEED, "1", "68000")

    result = await _tape(broker).read(TICKER)

    assert len(result) == 1
    record = result.records[0]
    assert isinstance(record, AggTrade)
    assert record.price == Decimal("68000")
    assert record.side is Side.BUY
    assert record.universal_ticker == str(TICKER)
    assert record.first_trade_id == "1"


@pytest.mark.asyncio
async def test_trade_topic_reads_back_as_trade(broker: Broker) -> None:
    await broker.tape_mark_recording(TRADE_FEED, since_ms=1, ttl_seconds=3600)
    await _record(broker, TRADE_FEED, "1", "68000", agg=False)

    result = await _tape(broker).read(TICKER, topic="trade")

    assert len(result) == 1
    assert type(result.records[0]) is Trade


@pytest.mark.asyncio
async def test_records_from_before_a_gap_are_dropped(broker: Broker) -> None:
    """Reading across a hole is the failure this whole mechanism prevents."""
    await _record(broker, AGG_FEED, "old", "1")
    rows = await broker.tape_tail(AGG_FEED, count=1)
    after_old = int(rows[0][0].partition("-")[0]) + 1
    # A real millisecond has to pass, or the record standing in for "after the
    # gap" lands in the same millisecond as the one standing in for "before"
    # and the test cannot tell them apart. Stream ids are a clock.
    await asyncio.sleep(0.01)
    # Recording restarted after that record — it is on the far side of a gap.
    await broker.tape_mark_recording(
        AGG_FEED, since_ms=after_old, ttl_seconds=3600
    )
    await _record(broker, AGG_FEED, "new", "2")

    result = await _tape(broker).read(TICKER)

    assert [r.trade_id for r in result.records] == ["new"]
    assert result.dropped_before_gap == 1


@pytest.mark.asyncio
async def test_coverage_is_reported(broker: Broker) -> None:
    await broker.tape_mark_recording(AGG_FEED, since_ms=99, ttl_seconds=3600)
    await _record(broker, AGG_FEED, "1", "1")

    result = await _tape(broker).read(TICKER)

    assert result.continuous_since_ms == 99
    assert result.recording is True


@pytest.mark.asyncio
async def test_a_stopped_feed_says_so(broker: Broker) -> None:
    """History that ends in the past is still history — but it ends."""
    await broker.tape_mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(broker, AGG_FEED, "1", "1")
    await broker.tape_mark_stopped(AGG_FEED, at_ms=2)

    result = await _tape(broker).read(TICKER)

    assert len(result) == 1
    assert result.recording is False


@pytest.mark.asyncio
async def test_nothing_recorded_is_an_empty_slice_not_an_error(
    broker: Broker,
) -> None:
    result = await _tape(broker).read(TICKER)
    assert len(result) == 0
    assert result.continuous_since_ms is None
    assert result.recording is False


@pytest.mark.asyncio
async def test_one_unreadable_record_does_not_lose_the_read(
    broker: Broker,
) -> None:
    await broker.tape_mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    await _record(broker, AGG_FEED, "1", "68000")
    await broker.tape_append(
        AGG_FEED,
        {"trade_id": "2", "price": "nonsense", "qty": "1", "side": "buy",
         "ts": "1"},
        maxlen=1000,
        ttl_seconds=3600,
    )
    await _record(broker, AGG_FEED, "3", "68100")

    result = await _tape(broker).read(TICKER)

    assert [r.trade_id for r in result.records] == ["1", "3"]


@pytest.mark.asyncio
async def test_limit_takes_the_most_recent(broker: Broker) -> None:
    await broker.tape_mark_recording(AGG_FEED, since_ms=1, ttl_seconds=3600)
    for n in range(5):
        await _record(broker, AGG_FEED, str(n), str(68000 + n))

    result = await _tape(broker).read(TICKER, limit=2)

    assert [r.trade_id for r in result.records] == ["3", "4"]

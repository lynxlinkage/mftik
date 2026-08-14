"""Broker tape primitives — the stream a warm-up reads back.

What matters here is not that XADD works, but the three promises the callers
build on: reads come back newest-last, the two bounds are independent, and a
tape nobody writes to goes away on its own.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig

FEED = "aggtrade.BinanceFuture_Perp_BTCUSDT"


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


def _print(n: int) -> dict[str, str]:
    return {
        "trade_id": str(n),
        "price": f"{68000 + n}",
        "qty": "0.5",
        "side": "buy",
        "ts": f"{1_700_000_000 + n}",
    }


async def _append(broker: Broker, count: int, *, maxlen: int = 1000) -> None:
    for n in range(count):
        await broker.tape_append(
            FEED, _print(n), maxlen=maxlen, ttl_seconds=3600
        )


@pytest.mark.asyncio
async def test_tail_returns_oldest_to_newest(broker: Broker) -> None:
    """A warm-up replays forward, so the read has to hand them over forward."""
    await _append(broker, 5)
    rows = await broker.tape_tail(FEED, count=5)
    assert [fields["trade_id"] for _id, fields in rows] == [
        "0",
        "1",
        "2",
        "3",
        "4",
    ]


@pytest.mark.asyncio
async def test_tail_takes_the_newest_when_asked_for_fewer(
    broker: Broker,
) -> None:
    """Catching up means the recent end, not the first records ever kept."""
    await _append(broker, 10)
    rows = await broker.tape_tail(FEED, count=3)
    assert [fields["trade_id"] for _id, fields in rows] == ["7", "8", "9"]


@pytest.mark.asyncio
async def test_maxlen_caps_the_stream(broker: Broker) -> None:
    """The memory fuse holds even when the retention window would not."""
    await _append(broker, 50, maxlen=10)
    rows = await broker.tape_tail(FEED, count=100)
    # Approximate trimming may leave more than maxlen, never the whole 50.
    assert 10 <= len(rows) < 50
    assert rows[-1][1]["trade_id"] == "49"


@pytest.mark.asyncio
async def test_trim_before_drops_by_time_not_count(broker: Broker) -> None:
    await _append(broker, 5)
    rows = await broker.tape_tail(FEED, count=5)
    cutoff = int(rows[3][0].partition("-")[0])

    dropped = await broker.tape_trim_before(FEED, min_id_ms=cutoff)

    assert dropped > 0
    remaining = await broker.tape_tail(FEED, count=10)
    assert all(int(rid.partition("-")[0]) >= cutoff for rid, _ in remaining)


@pytest.mark.asyncio
async def test_coverage_reports_recording_then_stopped(broker: Broker) -> None:
    await broker.tape_mark_recording(FEED, since_ms=1234, ttl_seconds=3600)
    live = await broker.tape_coverage(FEED)
    assert live["continuous_since_ms"] == "1234"
    assert live["recording"] == "1"

    await broker.tape_mark_stopped(FEED, at_ms=5678)
    stopped = await broker.tape_coverage(FEED)
    assert stopped["recording"] == "0"
    assert stopped["stopped_ms"] == "5678"
    # The mark that says where continuity began survives the feed going quiet:
    # the records are still there and still continuous, they just end.
    assert stopped["continuous_since_ms"] == "1234"


@pytest.mark.asyncio
async def test_coverage_of_a_feed_never_recorded_is_empty(
    broker: Broker,
) -> None:
    assert await broker.tape_coverage("trade.Paper_Spot_ETHUSDT") == {}


@pytest.mark.asyncio
async def test_append_renews_the_ttl_on_both_keys(broker: Broker) -> None:
    """Without this a tape outlives the last strategy that ever wanted it."""
    await broker.tape_mark_recording(FEED, since_ms=1, ttl_seconds=3600)
    await broker.tape_append(FEED, _print(0), maxlen=100, ttl_seconds=1800)

    assert 0 < await broker.redis.ttl(broker.tape_key(FEED)) <= 1800
    assert 0 < await broker.redis.ttl(broker.tape_coverage_key(FEED)) <= 1800


@pytest.mark.asyncio
async def test_tail_of_a_missing_feed_is_empty(broker: Broker) -> None:
    assert await broker.tape_tail("trade.Paper_Spot_ETHUSDT", count=10) == []

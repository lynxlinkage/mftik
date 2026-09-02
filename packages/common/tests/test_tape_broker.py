"""Broker tape primitives — the stream a warm-up reads back.

What matters here is not that XADD works, but the three promises the callers
build on: reads come back newest-last, the two bounds are independent, and a
tape nobody writes to goes away on its own.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.broker.client import (
    TAPE_MAX_GAPS,
    decode_tape_gaps,
    encode_tape_gaps,
)

FEED = "aggtrade.BinanceFuture_Perp_BTCUSDT"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


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
    # Two batches with a real millisecond between them. Appends are far faster
    # than the clock they are stamped with, so without the wait all five ids
    # can share one millisecond and no cutoff can separate them — which is a
    # property of the test, not of the trim.
    await _append(broker, 2)
    await asyncio.sleep(0.01)
    await _append(broker, 3)

    rows = await broker.tape_tail(FEED, count=5)
    old_ms = int(rows[0][0].partition("-")[0])
    cutoff = int(rows[2][0].partition("-")[0])
    assert cutoff > old_ms, "the two batches must not share a millisecond"

    dropped = await broker.tape_trim_before(FEED, min_id_ms=cutoff)

    assert dropped == 2
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


@pytest.mark.asyncio
async def test_a_measured_interruption_keeps_continuity(broker: Broker) -> None:
    """A deploy is not a discontinuity, it is a hole somebody wrote down.

    Resetting the mark here is what used to throw away the whole warm-up
    window: two intact hours in the stream, discarded to describe a gap of
    seconds that both edges of the restart had already stamped.
    """
    await broker.tape_mark_recording(FEED, since_ms=1_000, ttl_seconds=3600)
    await broker.tape_mark_stopped(FEED, at_ms=5_000)
    await broker.tape_mark_recording(FEED, since_ms=9_000, ttl_seconds=3600)

    coverage = await broker.tape_coverage(FEED)

    assert coverage["continuous_since_ms"] == "1000"
    assert coverage["recording"] == "1"
    assert coverage["stopped_ms"] == ""
    assert decode_tape_gaps(coverage["gaps"]) == [(5_000, 9_000)]


@pytest.mark.asyncio
async def test_an_unmeasured_interruption_restarts_continuity(
    broker: Broker,
) -> None:
    """No stop stamp means nobody was there to write one — SIGKILL, OOM.

    The hole is real and its length is unknown, so the older behaviour is the
    only honest one: everything before this is off the series.
    """
    await broker.tape_mark_recording(FEED, since_ms=1_000, ttl_seconds=3600)
    await broker.tape_mark_recording(FEED, since_ms=9_000, ttl_seconds=3600)

    coverage = await broker.tape_coverage(FEED)

    assert coverage["continuous_since_ms"] == "9000"
    assert decode_tape_gaps(coverage["gaps"]) == []


@pytest.mark.asyncio
async def test_a_measured_gap_does_not_erase_the_ones_before_it(
    broker: Broker,
) -> None:
    """Two deploys inside a retention window are two holes, not one."""
    await broker.tape_mark_recording(FEED, since_ms=1_000, ttl_seconds=3600)
    for stopped, resumed in ((2_000, 3_000), (4_000, 4_500)):
        await broker.tape_mark_stopped(FEED, at_ms=stopped)
        await broker.tape_mark_recording(
            FEED, since_ms=resumed, ttl_seconds=3600
        )

    coverage = await broker.tape_coverage(FEED)

    assert coverage["continuous_since_ms"] == "1000"
    assert decode_tape_gaps(coverage["gaps"]) == [
        (2_000, 3_000),
        (4_000, 4_500),
    ]


@pytest.mark.asyncio
async def test_too_many_gaps_collapse_to_a_fresh_mark(broker: Broker) -> None:
    """Past some count it is not a recording with holes, it is confetti.

    Also the bound on the field: coverage is a hash value, and a feed in a
    crash loop would otherwise grow it for as long as Redis kept the key.
    """
    await broker.tape_mark_recording(FEED, since_ms=0, ttl_seconds=3600)
    for n in range(TAPE_MAX_GAPS + 1):
        await broker.tape_mark_stopped(FEED, at_ms=n * 10 + 1)
        await broker.tape_mark_recording(
            FEED, since_ms=n * 10 + 2, ttl_seconds=3600
        )

    coverage = await broker.tape_coverage(FEED)

    assert decode_tape_gaps(coverage["gaps"]) == []
    assert coverage["continuous_since_ms"] == str(TAPE_MAX_GAPS * 10 + 2)


@pytest.mark.asyncio
async def test_a_stop_stamped_after_the_resume_is_not_a_gap(
    broker: Broker,
) -> None:
    """A clock that moved backwards measures nothing. Treated as unknown."""
    await broker.tape_mark_recording(FEED, since_ms=1_000, ttl_seconds=3600)
    await broker.tape_mark_stopped(FEED, at_ms=9_000)
    await broker.tape_mark_recording(FEED, since_ms=5_000, ttl_seconds=3600)

    coverage = await broker.tape_coverage(FEED)

    assert coverage["continuous_since_ms"] == "5000"
    assert decode_tape_gaps(coverage["gaps"]) == []


def test_gap_codec_round_trips() -> None:
    gaps = [(1, 2), (30_000, 41_000)]
    assert decode_tape_gaps(encode_tape_gaps(gaps)) == gaps
    assert decode_tape_gaps("") == []
    assert decode_tape_gaps(None) == []


def test_an_unreadable_gap_is_skipped_not_raised() -> None:
    """Coverage is a nicety for a warm-up; the caller is a feed coming up."""
    assert decode_tape_gaps("1-2,rubbish,5-6") == [(1, 2), (5, 6)]

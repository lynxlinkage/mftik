"""MD tape recording — what gets recorded, and what the coverage stamps say.

The interesting cases are the ones about honesty rather than throughput: a book
feed must not be recorded at all, and a feed that stops and starts again must
leave a mark a reader can see, because a reader that cannot see the gap will
read straight across it.
"""

from __future__ import annotations

import fakeredis.aioredis
import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import Envelope, Topics
from mftik_md.tape import TapeRecorder
from mftik_md.tape_store import TapeStore, decode_tape_gaps

TICKER = UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")
AGG_FEED = Topics.md_feed("aggtrade", TICKER)


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


def _agg_payload(trade_id: str = "1", price: str = "68000") -> dict:
    return {
        "universal_ticker": str(TICKER),
        "trade_id": trade_id,
        "price": price,
        "qty": "0.25",
        "side": "buy",
        "ts": 1_700_000_000.5,
        "first_trade_id": trade_id,
        "last_trade_id": trade_id,
    }


@pytest.mark.asyncio
async def test_records_trade_feeds(store: TapeStore) -> None:
    recorder = TapeRecorder(store)
    await recorder.append("aggtrade", TICKER, _agg_payload("42"))

    rows = await store.tail(AGG_FEED, count=10)
    assert len(rows) == 1
    fields = rows[0][1]
    assert fields["trade_id"] == "42"
    assert fields["price"] == "68000"
    assert fields["first_trade_id"] == "42"


@pytest.mark.asyncio
async def test_does_not_record_book_feeds(store: TapeStore) -> None:
    """A twenty-level book is 7x the bytes of a print and no use as history."""
    recorder = TapeRecorder(store)
    assert not recorder.records("orderbook")

    await recorder.append("orderbook", TICKER, {"bids": [], "asks": []})

    book_feed = Topics.md_feed("orderbook", TICKER)
    assert await store.tail(book_feed, count=10) == []


@pytest.mark.asyncio
async def test_a_clean_restart_keeps_continuity_and_records_the_hole(
    store: TapeStore,
) -> None:
    """The whole point of the stamps: a reader can see the hole *and* size it.

    Both edges are stamped here — a shutdown that ran, and the start after it —
    so the interruption is a measured fact. That is a deploy. Ending the series
    over it would discard the warm-up window to describe a gap of seconds.
    """
    recorder = TapeRecorder(store)
    await recorder.started(AGG_FEED)
    first = await store.coverage(AGG_FEED)
    await recorder.append("aggtrade", TICKER, _agg_payload("1"))

    await recorder.stopped(AGG_FEED)
    stopped = await store.coverage(AGG_FEED)
    assert stopped["recording"] == "0"

    await recorder.started(AGG_FEED)
    resumed = await store.coverage(AGG_FEED)

    assert resumed["recording"] == "1"
    assert resumed["continuous_since_ms"] == first["continuous_since_ms"]
    gaps = decode_tape_gaps(resumed["gaps"])
    assert len(gaps) == 1
    assert gaps[0][0] == int(stopped["stopped_ms"])
    # The records from before the break are still in the stream — dropping
    # them here would throw away history a reader may legitimately want.
    assert len(await store.tail(AGG_FEED, count=10)) == 1


@pytest.mark.asyncio
async def test_a_restart_nobody_stamped_moves_the_continuity_mark(
    store: TapeStore,
) -> None:
    """No ``stopped`` call is what SIGKILL and OOM look like from here.

    Nothing ran to write down when the feed went quiet, so the hole has no
    measured length and the records behind it are off the series.
    """
    recorder = TapeRecorder(store)
    await recorder.started(AGG_FEED)
    first = await store.coverage(AGG_FEED)
    await recorder.append("aggtrade", TICKER, _agg_payload("1"))

    await recorder.started(AGG_FEED)
    resumed = await store.coverage(AGG_FEED)

    assert int(resumed["continuous_since_ms"]) >= int(
        first["continuous_since_ms"]
    )
    assert decode_tape_gaps(resumed["gaps"]) == []
    assert len(await store.tail(AGG_FEED, count=10)) == 1


@pytest.mark.asyncio
async def test_an_unreadable_payload_does_not_raise(store: TapeStore) -> None:
    """Recording runs behind a fan-out that is feeding live strategies."""
    recorder = TapeRecorder(store)
    await recorder.append("aggtrade", TICKER, {"universal_ticker": str(TICKER)})
    assert await store.tail(AGG_FEED, count=10) == []


@pytest.mark.asyncio
async def test_trim_only_touches_recorded_topics(store: TapeStore) -> None:
    recorder = TapeRecorder(store, retention_s=1.0)
    await recorder.append("aggtrade", TICKER, _agg_payload("1"))
    book_feed = Topics.md_feed("orderbook", TICKER)
    await store.append(
        book_feed, {"price": "1", "qty": "1"}, maxlen=10, ttl_seconds=60
    )

    await recorder.trim([AGG_FEED, book_feed])

    # The trade tape is inside its (1s) window only if it was just written;
    # the book key is not a recorded topic and is left exactly as found.
    assert len(await store.tail(book_feed, count=10)) == 1


@pytest.mark.asyncio
async def test_configured_topics_replace_the_defaults(store: TapeStore) -> None:
    recorder = TapeRecorder(store, topics=["trade"])
    assert recorder.records("trade")
    assert not recorder.records("aggtrade")

    await recorder.append("aggtrade", TICKER, _agg_payload("1"))
    assert await store.tail(AGG_FEED, count=10) == []


@pytest.mark.asyncio
async def test_dispatcher_records_after_fanning_out(
    broker: Broker, store: TapeStore
) -> None:
    """Wired where every print already passes, and behind the live sessions."""
    from mftik_md.session.dispatcher import Dispatcher

    recorder = TapeRecorder(store)
    dispatcher = Dispatcher(broker, recorder=recorder)
    envelope = Envelope[dict].wrap(
        _agg_payload("7"), type="md.aggtrade", source="md"
    )

    await dispatcher.publish("aggtrade", TICKER, envelope)

    rows = await store.tail(AGG_FEED, count=10)
    assert [fields["trade_id"] for _id, fields in rows] == ["7"]


@pytest.mark.asyncio
async def test_dispatcher_without_a_recorder_records_nothing(
    broker: Broker, store: TapeStore
) -> None:
    from mftik_md.session.dispatcher import Dispatcher

    dispatcher = Dispatcher(broker)
    envelope = Envelope[dict].wrap(
        _agg_payload("7"), type="md.aggtrade", source="md"
    )

    await dispatcher.publish("aggtrade", TICKER, envelope)

    assert await store.tail(AGG_FEED, count=10) == []


@pytest.mark.asyncio
async def test_append_failure_does_not_raise(store: TapeStore, monkeypatch) -> None:
    """A Redis blip loses records, not ticks."""

    async def boom(*_args, **_kwargs):
        raise RuntimeError("redis down")

    monkeypatch.setattr(store, "append", boom)
    recorder = TapeRecorder(store)
    await recorder.append("aggtrade", TICKER, _agg_payload("1"))

"""Chunked ``md.tape.tail`` on the named MD subject."""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    MD_TAPE_TAIL,
    MdTapeTailChunk,
    MdTapeTailRequest,
    MdTapeTailRequestEnvelope,
    Topics,
)
from mftik_md.rpc.tape import TAPE_RPC_CHUNK, handle_tape_tail
from mftik_md.tape_store import TapeStore

TICKER = UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")
FEED = Topics.md_feed("aggtrade", TICKER)


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


async def _serve(
    broker: Broker, store: TapeStore, stop: asyncio.Event, chunk: int
) -> None:
    async for req in broker.serve(Topics.md("md-jp"), stop=stop):
        if req.envelope.type == MD_TAPE_TAIL:
            await handle_tape_tail(req, store=store, chunk=chunk)


async def _record(store: TapeStore, trade_id: str, *, ms: int | None = None) -> None:
    await store.append(
        FEED,
        {
            "trade_id": trade_id,
            "price": "68000",
            "qty": "0.5",
            "side": "buy",
            "ts": "1700000000.5",
            "first_trade_id": trade_id,
            "last_trade_id": trade_id,
        },
        maxlen=1000,
        ttl_seconds=3600,
        recorded_ms=ms,
    )


@pytest.mark.asyncio
async def test_tape_tail_is_chunked(broker: Broker, store: TapeStore) -> None:
    await store.mark_recording(FEED, since_ms=1, ttl_seconds=3600)
    for n in range(5):
        await _record(store, str(n), ms=1_000 + n)

    stop = asyncio.Event()
    task = asyncio.create_task(_serve(broker, store, stop, chunk=2))
    try:
        first = MdTapeTailChunk.model_validate(
            (
                await broker.request(
                    Topics.md("md-jp"),
                    MdTapeTailRequestEnvelope.wrap(
                        MdTapeTailRequest(feed=FEED, limit=5),
                        type=MD_TAPE_TAIL,
                        source="sts",
                    ),
                )
            ).payload
        )
        assert [r.fields["trade_id"] for r in first.records] == ["3", "4"]
        assert first.more is True
        assert first.recording is True

        second = MdTapeTailChunk.model_validate(
            (
                await broker.request(
                    Topics.md("md-jp"),
                    MdTapeTailRequestEnvelope.wrap(
                        MdTapeTailRequest(feed=FEED, limit=5, before=first.before),
                        type=MD_TAPE_TAIL,
                        source="sts",
                    ),
                )
            ).payload
        )
        assert [r.fields["trade_id"] for r in second.records] == ["1", "2"]
        assert second.more is True
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_unknown_feed_is_an_empty_slice(
    broker: Broker, store: TapeStore
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(_serve(broker, store, stop, chunk=TAPE_RPC_CHUNK))
    try:
        chunk = MdTapeTailChunk.model_validate(
            (
                await broker.request(
                    Topics.md("md-jp"),
                    MdTapeTailRequestEnvelope.wrap(
                        MdTapeTailRequest(feed=FEED, limit=10),
                        type=MD_TAPE_TAIL,
                        source="sts",
                    ),
                )
            ).payload
        )
        assert chunk.records == []
        assert chunk.recording is False
        assert chunk.continuous_since_ms is None
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

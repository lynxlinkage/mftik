"""Paper remote public client smoke test (in-process engine + fakeredis)."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.exchange.paper.remote_public import PaperRemotePublicClient
from mft.exchange.tickers import UniversalTicker
from mft.protocol import PAPER_ORDER_BOOK, Topics, UntypedEnvelope


def PAPER(symbol: str) -> UniversalTicker:
    """``BTCUSDT`` → ``Paper_Spot_BTCUSDT``; public reads are keyed by ticker."""
    return UniversalTicker.of("Paper", "Spot", symbol)


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-ppub"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.mark.asyncio
async def test_remote_public_fetch_and_stream(broker: Broker) -> None:
    from mft_paper.rpc import dispatch

    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=2,
        volatility_bps=0,
    ) as exchange:
        stop = asyncio.Event()

        async def _rpc() -> None:
            async for req in broker.serve(Topics.PAPER, stop=stop):
                await dispatch(req, exchange=exchange)

        async def _bridge() -> None:
            public = exchange.public()
            await public.connect()
            try:
                async for book in public.stream_order_book(PAPER("BTCUSDT")):
                    if stop.is_set():
                        return
                    await broker.publish(
                        Topics.paper_order_book("BTCUSDT"),
                        UntypedEnvelope.wrap(
                            book.model_dump(mode="json"),
                            type=PAPER_ORDER_BOOK,
                            source="paper",
                        ),
                    )
            finally:
                await public.close()

        rpc_task = asyncio.create_task(_rpc())
        bridge_task = asyncio.create_task(_bridge())
        await asyncio.sleep(0.05)

        client = PaperRemotePublicClient(broker)
        await client.connect()
        instruments = await client.fetch_instruments()
        assert any(i.symbol == "BTCUSDT" for i in instruments)
        book = await client.fetch_order_book(PAPER("BTCUSDT"), depth=5)
        assert book.symbol == "BTCUSDT"
        assert book.bids and book.asks

        got = None
        async for update in client.stream_order_book(PAPER("BTCUSDT")):
            got = update
            break
        assert got is not None
        assert got.symbol == "BTCUSDT"

        await client.close()
        stop.set()
        rpc_task.cancel()
        bridge_task.cancel()
        await asyncio.gather(rpc_task, bridge_task, return_exceptions=True)

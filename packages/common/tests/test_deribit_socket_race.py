"""Many pumps attaching at once must share one socket and one read loop."""

from __future__ import annotations

import asyncio

from deribit_stub import FakeDeribit
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.public import DeribitPublicClient
from mftik.exchange.deribit.rest import DeribitPublicRest
from mftik.exchange.tickers import UniversalTicker
from test_deribit_public import BASE, FakeApi, StubSymbols, _wire

STRIKES = [60000 + 1000 * i for i in range(11)]
TICKERS = [
    UniversalTicker.parse(f"Deribit_Option_BTCUSD-261030-{k}-C") for k in STRIKES
]


async def test_eleven_feeds_attaching_at_once_open_one_socket(
    deribit_public: FakeDeribit,
) -> None:
    client = DeribitPublicClient(
        symbols=StubSymbols(),
        rest=DeribitPublicRest(base_url=BASE, client=FakeApi().client()),
        ws_url=deribit_public.url,
    )
    async with client:
        streams = [client.stream_ticker(t) for t in TICKERS]
        firsts = [asyncio.ensure_future(s.__anext__()) for s in streams]
        for _ in range(50):
            if len(deribit_public.subscribed) == len(TICKERS):
                break
            await asyncio.sleep(0.02)
        for t in TICKERS:
            await deribit_public.push(
                ch.ticker(_wire(t)),
                {"instrument_name": _wire(t), "last_price": 0.01,
                 "best_bid_price": 0.009, "best_ask_price": 0.011},
            )
        rows = await asyncio.wait_for(asyncio.gather(*firsts), 5)
        feed = await client.feed()
        reconnects = feed.stats.reconnects
    assert len(rows) == len(TICKERS)
    assert deribit_public.connections == 1
    assert reconnects == 0


async def test_subscribes_during_a_reconnect_wait_for_the_read_loop(
    deribit_public: FakeDeribit,
) -> None:
    """A pump attaching mid-reconnect must not read what ``_restore`` reads."""
    client = DeribitPublicClient(
        symbols=StubSymbols(),
        rest=DeribitPublicRest(base_url=BASE, client=FakeApi().client()),
        ws_url=deribit_public.url,
    )
    async with client:
        feed = await client.feed()
        feed.retry_backoff = 0.01
        held = [client.stream_ticker(t) for t in TICKERS[:5]]
        held_first = [asyncio.ensure_future(s.__anext__()) for s in held]
        for _ in range(50):
            if len(deribit_public.subscribed) == 5:
                break
            await asyncio.sleep(0.02)
        for ws in list(deribit_public.clients):
            await ws.close()
        # Attach the rest while the socket is between read loops.
        late = [client.stream_ticker(t) for t in TICKERS[5:]]
        late_first = [asyncio.ensure_future(s.__anext__()) for s in late]
        for _ in range(100):
            if feed.stats.reconnects >= 1 and len(deribit_public.subscribed) == 11:
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.05)
        for t in TICKERS:
            await deribit_public.push(
                ch.ticker(_wire(t)),
                {"instrument_name": _wire(t), "last_price": 0.01,
                 "best_bid_price": 0.009, "best_ask_price": 0.011},
            )
        rows = await asyncio.wait_for(asyncio.gather(*held_first, *late_first), 5)
        reconnects = feed.stats.reconnects
    assert len(rows) == len(TICKERS)
    assert reconnects == 1
    assert deribit_public.connections == 2

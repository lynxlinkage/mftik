"""The bridge from the engine's order book to Redis.

This is the feed every strategy subscribing to ``orderbook.Paper_*`` reads,
and it was dead from the first line: the pump handed a bare symbol to a
client method that wanted a UniversalTicker, raised AttributeError, and died
before publishing anything. Nothing said so — the task's reference was held
for the process lifetime, so Python never collected it and never reported
what it raised. A strategy attached, waited, and looked like a quiet market.

So these tests assert the thing nobody was asserting: that a message actually
comes out.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.protocol import PAPER_ORDER_BOOK, Topics
from mftik_paper.app import _pump_order_book, _tick_order_book, _watch


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("test-book") as client:
        yield client


@pytest.fixture
def exchange() -> PaperExchange:
    # No drift: these tests are about the bridge, and a book moving under them
    # would make "did anything publish" ambiguous.
    return PaperExchange(tick_interval=10.0, volatility_bps=0)


async def _first_book(broker: Broker, symbol: str, timeout: float):  # noqa: ANN202
    """The next book envelope on the paper topic, or None if none arrives."""
    stop = asyncio.Event()
    topic = Topics.paper_order_book(symbol)

    async def read():  # noqa: ANN202
        async for env in broker.subscribe(topic, stop=stop):
            if env.type == PAPER_ORDER_BOOK:
                return env
        return None

    task = asyncio.create_task(read())
    # Give the subscription a moment to land before anything publishes into it:
    # Redis pub/sub drops what arrives with nobody listening.
    await asyncio.sleep(0.1)
    return task, stop


async def test_a_quiet_venue_still_publishes_its_book(
    broker: Broker, exchange: PaperExchange
) -> None:
    """The symptom that started this: subscribe to a still book, wait forever.

    An idle venue is what a paper stack is until something trades on it, and
    the only thing that was going to trade on it was the strategy waiting for
    this feed.
    """
    task, stop = await _first_book(broker, "BTCUSDT", 2.0)
    ticker = asyncio.create_task(
        _tick_order_book(broker, exchange, "BTCUSDT", stop, 0.1)
    )

    env = await asyncio.wait_for(task, timeout=3.0)

    stop.set()
    ticker.cancel()
    assert env is not None
    assert env.payload["universal_ticker"].endswith("BTCUSDT")
    assert env.payload["bids"] and env.payload["asks"]


async def test_a_change_reaches_redis(
    broker: Broker, exchange: PaperExchange
) -> None:
    """The pump handed a bare symbol to a client that wanted a ticker.

    It raised on its first line every time, which is why this is asserted on
    the message rather than on the call not raising: a task that dies before
    publishing looks identical to a market with nothing to say.
    """
    task, stop = await _first_book(broker, "BTCUSDT", 2.0)
    pump = _watch(
        asyncio.create_task(_pump_order_book(broker, exchange, "BTCUSDT", stop))
    )

    env = await asyncio.wait_for(task, timeout=3.0)

    stop.set()
    pump.cancel()
    assert env is not None
    assert env.type == PAPER_ORDER_BOOK
    assert env.payload["universal_ticker"].endswith("BTCUSDT")


async def test_the_pump_survives_its_first_call(
    broker: Broker, exchange: PaperExchange
) -> None:
    """Directly: the task is still running a moment later.

    The regression was not a wrong book, it was no book — and the process it
    ran in reported success throughout.
    """
    stop = asyncio.Event()
    pump = asyncio.create_task(
        _pump_order_book(broker, exchange, "BTCUSDT", stop)
    )
    await asyncio.sleep(0.2)

    assert not pump.done(), (
        f"pump died immediately: {pump.exception() if pump.done() else ''}"
    )
    stop.set()
    pump.cancel()


async def test_a_dying_task_is_reported(caplog) -> None:
    """What made the original bug invisible, made visible.

    A held task reference means Python never collects it and never surfaces
    what it raised, so the failure has to be announced deliberately.
    """

    async def boom() -> None:
        raise RuntimeError("boom")

    task = _watch(asyncio.create_task(boom(), name="paper-book-test"))
    with caplog.at_level("ERROR"):
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

    assert any("paper-book-test" in r.getMessage() for r in caplog.records), (
        caplog.text
    )

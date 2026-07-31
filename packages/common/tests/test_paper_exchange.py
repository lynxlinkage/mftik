from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from mft.exchange import (
    ExchangeNotConnectedError,
    OrderStatus,
    OrderType,
    PaperExchange,
    PlaceOrderRequest,
    Side,
)


@pytest.fixture
async def exchange() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=42,
    ) as ex:
        yield ex


@pytest.mark.asyncio
async def test_public_req_reply(exchange: PaperExchange) -> None:
    public = exchange.public()
    await public.connect()

    instruments = await public.fetch_instruments()
    assert any(i.symbol == "BTCUSDT" for i in instruments)

    ticker = await public.fetch_ticker("BTCUSDT")
    assert ticker.symbol == "BTCUSDT"
    assert ticker.ask >= ticker.bid
    assert ticker.last > 0

    book = await public.fetch_order_book("BTCUSDT", depth=5)
    assert len(book.bids) == 5
    assert len(book.asks) == 5
    assert book.bids[0].price < book.asks[0].price

    await public.close()


@pytest.mark.asyncio
async def test_public_stream_ticker(exchange: PaperExchange) -> None:
    public = exchange.public()
    await public.connect()

    ticks = []
    async for ticker in public.stream_ticker("BTCUSDT"):
        ticks.append(ticker)
        if len(ticks) >= 3:
            break

    assert len(ticks) >= 3
    assert ticks[0].symbol == "BTCUSDT"
    await public.close()


@pytest.mark.asyncio
async def test_private_market_order_and_streams(exchange: PaperExchange) -> None:
    private = exchange.private()
    await private.connect()

    fills: asyncio.Queue = asyncio.Queue()
    orders: asyncio.Queue = asyncio.Queue()

    async def collect_fills() -> None:
        async for fill in private.stream_fills():
            await fills.put(fill)

    async def collect_orders() -> None:
        async for order in private.stream_orders():
            await orders.put(order)

    fill_task = asyncio.create_task(collect_fills())
    order_task = asyncio.create_task(collect_orders())
    await asyncio.sleep(0.05)

    before = {b.asset: b.free for b in await private.fetch_balances()}
    order = await private.place_market_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("0.01"),
    )
    assert order.status is OrderStatus.FILLED
    assert order.filled_qty == Decimal("0.01")
    assert order.avg_price is not None

    got_order = await asyncio.wait_for(orders.get(), timeout=2)
    got_fill = await asyncio.wait_for(fills.get(), timeout=2)
    assert got_order.order_id == order.order_id
    assert got_fill.order_id == order.order_id

    after = {b.asset: b.free for b in await private.fetch_balances()}
    assert after["BTC"] > before["BTC"]
    assert after["USDT"] < before["USDT"]

    fill_task.cancel()
    order_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fill_task
    with pytest.raises(asyncio.CancelledError):
        await order_task
    await private.close()


@pytest.mark.asyncio
async def test_limit_rest_cancel(exchange: PaperExchange) -> None:
    private = exchange.private()
    await private.connect()

    ticker = exchange.get_ticker("BTCUSDT")
    # Far-away buy limit should rest.
    order = await private.place_order(
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=ticker.bid / Decimal("2"),
        )
    )
    assert order.status is OrderStatus.OPEN
    open_orders = await private.fetch_open_orders("BTCUSDT")
    assert any(o.order_id == order.order_id for o in open_orders)

    canceled = await private.cancel_order(order.order_id)
    assert canceled.status is OrderStatus.CANCELED
    assert await private.fetch_open_orders("BTCUSDT") == []
    await private.close()


@pytest.mark.asyncio
async def test_requires_connect() -> None:
    async with PaperExchange(symbols={"BTCUSDT": Decimal("1")}) as ex:
        public = ex.public()
        with pytest.raises(ExchangeNotConnectedError):
            await public.fetch_ticker("BTCUSDT")


@pytest.mark.asyncio
async def test_public_and_private_share_engine(exchange: PaperExchange) -> None:
    public = exchange.public()
    private = exchange.private()
    await public.connect()
    await private.connect()

    trade_fut: asyncio.Future = asyncio.get_running_loop().create_future()

    async def reader() -> None:
        async for trade in public.stream_trades("BTCUSDT"):
            if not trade_fut.done():
                trade_fut.set_result(trade)
            break

    task = asyncio.create_task(reader())
    await asyncio.sleep(0.05)
    await private.place_market_order(
        symbol="BTCUSDT", side=Side.SELL, qty=Decimal("0.01")
    )
    trade = await asyncio.wait_for(trade_fut, timeout=2)
    assert trade.symbol == "BTCUSDT"
    assert trade.side is Side.SELL
    await task
    await public.close()
    await private.close()

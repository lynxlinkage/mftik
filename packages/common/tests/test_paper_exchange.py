from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from mft.exchange import (
    ExchangeNotConnectedError,
    OrderError,
    OrderStatus,
    OrderType,
    PaperAuthError,
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


def _private(exchange: PaperExchange, key: str = "key-a"):
    return exchange.private(api_key=key, api_secret=f"secret-for-{key}")


async def _seed_book(exchange: PaperExchange) -> None:
    """Resting bid/ask from a maker account so takers can match."""
    exchange.register_api(
        "maker",
        "secret-for-maker",
        balances={"BTC": Decimal("10"), "USDT": Decimal("500000")},
    )
    maker = exchange.private(
        api_key="maker", api_secret="secret-for-maker", auto_register=False
    )
    await maker.connect()
    await maker.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("10"),
        price=Decimal("49999"),
    )
    await maker.place_limit_order(
        symbol="BTCUSDT",
        side=Side.SELL,
        qty=Decimal("10"),
        price=Decimal("50001"),
    )
    await maker.close()


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
    assert len(book.bids) == 1
    assert len(book.asks) == 1
    assert book.bids[0].price == Decimal("49999")
    assert book.asks[0].price == Decimal("50001")
    assert book.bids[0].qty == Decimal("1")
    assert book.asks[0].qty == Decimal("1")

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
    await _seed_book(exchange)
    private = _private(exchange)
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
    private = _private(exchange)
    await private.connect()

    ticker = exchange.get_ticker("BTCUSDT")
    price = ticker.bid / Decimal("2")
    qty = Decimal("0.01")
    before = {b.asset: b for b in await private.fetch_balances()}

    order = await private.place_order(
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=qty,
            price=price,
        )
    )
    assert order.status is OrderStatus.OPEN
    assert order.client_order_id  # auto-assigned
    open_orders = await private.fetch_open_orders("BTCUSDT")
    assert any(o.order_id == order.order_id for o in open_orders)

    locked_cost = price * qty
    mid = {b.asset: b for b in await private.fetch_balances()}
    assert mid["USDT"].locked == locked_cost
    assert mid["USDT"].free == before["USDT"].free - locked_cost
    assert mid["BTC"].locked == Decimal("0")

    canceled = await private.cancel_order(order.order_id)
    assert canceled.status is OrderStatus.CANCELED
    assert await private.fetch_open_orders("BTCUSDT") == []
    after = {b.asset: b for b in await private.fetch_balances()}
    assert after["USDT"].locked == Decimal("0")
    assert after["USDT"].free == before["USDT"].free
    await private.close()


@pytest.mark.asyncio
async def test_client_order_id_roundtrip_and_cancel(exchange: PaperExchange) -> None:
    private = _private(exchange)
    await private.connect()

    ticker = exchange.get_ticker("BTCUSDT")
    order = await private.place_order(
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=ticker.bid / Decimal("2"),
            client_order_id="cid-abc",
        )
    )
    assert order.client_order_id == "cid-abc"
    looked_up = exchange.get_order_by_client_id(private.api_key, "cid-abc")
    assert looked_up.order_id == order.order_id

    canceled = await private.cancel_by_client_order_id("cid-abc")
    assert canceled.status is OrderStatus.CANCELED
    assert canceled.client_order_id == "cid-abc"
    assert await private.fetch_open_orders() == []
    await private.close()


@pytest.mark.asyncio
async def test_duplicate_client_order_id_rejected(exchange: PaperExchange) -> None:
    private = _private(exchange)
    await private.connect()
    ticker = exchange.get_ticker("BTCUSDT")
    await private.place_order(
        PlaceOrderRequest(
            symbol="BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            qty=Decimal("0.01"),
            price=ticker.bid / Decimal("2"),
            client_order_id="dup-1",
        )
    )
    with pytest.raises(OrderError, match="duplicate client_order_id"):
        await private.place_order(
            PlaceOrderRequest(
                symbol="BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.01"),
                price=ticker.bid / Decimal("2"),
                client_order_id="dup-1",
            )
        )
    await private.close()


@pytest.mark.asyncio
async def test_requires_connect() -> None:
    async with PaperExchange(symbols={"BTCUSDT": Decimal("1")}) as ex:
        public = ex.public()
        with pytest.raises(ExchangeNotConnectedError):
            await public.fetch_ticker("BTCUSDT")


@pytest.mark.asyncio
async def test_public_and_private_share_engine(exchange: PaperExchange) -> None:
    await _seed_book(exchange)
    public = exchange.public()
    private = _private(exchange)
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


@pytest.mark.asyncio
async def test_cross_account_match(exchange: PaperExchange) -> None:
    await _seed_book(exchange)
    book = exchange.get_order_book("BTCUSDT")
    assert book.asks[0].price == Decimal("50001")
    assert book.asks[0].qty == Decimal("10")

    taker = _private(exchange, "taker")
    await taker.connect()
    order = await taker.place_limit_order(
        symbol="BTCUSDT",
        side=Side.BUY,
        qty=Decimal("1"),
        price=Decimal("50001"),
    )
    assert order.status is OrderStatus.FILLED
    assert order.avg_price == Decimal("50001")
    bals = {b.asset: b.free for b in await taker.fetch_balances()}
    assert bals["BTC"] > Decimal("1")
    await taker.close()


@pytest.mark.asyncio
async def test_api_key_isolates_accounts(exchange: PaperExchange) -> None:
    await _seed_book(exchange)
    a = exchange.private(api_key="alice", api_secret="sa")
    b = exchange.private(api_key="bob", api_secret="sb")
    await a.connect()
    await b.connect()

    await a.place_market_order(
        symbol="BTCUSDT", side=Side.BUY, qty=Decimal("0.01")
    )
    bal_a = {x.asset: x.free for x in await a.fetch_balances()}
    bal_b = {x.asset: x.free for x in await b.fetch_balances()}
    assert bal_a["BTC"] != bal_b["BTC"]
    assert bal_b["BTC"] == Decimal("1")
    await a.close()
    await b.close()


@pytest.mark.asyncio
async def test_paper_auth_rejects_bad_secret(exchange: PaperExchange) -> None:
    exchange.register_api("k1", "correct")
    client = exchange.private(
        api_key="k1", api_secret="wrong", auto_register=False
    )
    with pytest.raises(PaperAuthError):
        await client.connect()

"""Deribit private client — connect gate, post_only, qty, balances."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.models import (
    DeribitFill,
    DeribitOrderUpdate,
    DeribitSummary,
)
from mftik.exchange.deribit.private import DeribitPrivateClient
from mftik.exchange.deribit.protocol import (
    MARGIN_MODELS,
    DeribitAuthError,
    expiry_code_from_name,
    expiry_suffix_from_code,
)
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import Category, UniversalTicker

PERP = UniversalTicker.parse("Deribit_Perp_BTCUSDC")
INVERSE = UniversalTicker.parse("Deribit_Inverse_BTCUSD")
DATED = UniversalTicker.parse("Deribit_Future_BTCUSD-260906")
API_KEY = "cid"
API_SECRET = "secret"


def _wire(ticker: UniversalTicker) -> str:
    symbol = ticker.symbol
    code = None
    if "-" in symbol:
        pair, maybe = symbol.rsplit("-", 1)
        if len(maybe) == 6 and maybe.isdigit():
            symbol, code = pair, maybe
    for quote in ("USDC", "USDT", "USD"):
        if symbol.endswith(quote) and symbol != quote:
            base = symbol[: -len(quote)]
            if quote == "USD":
                if ticker.category is Category.FUTURE and code:
                    return f"{base}-{expiry_suffix_from_code(code)}"
                return f"{base}-PERPETUAL"
            pair = f"{base}_{quote}"
            if ticker.category is Category.PERP:
                return f"{pair}-PERPETUAL"
            if ticker.category is Category.FUTURE and code:
                return f"{pair}-{expiry_suffix_from_code(code)}"
            return pair
    return ticker.symbol


class StubSymbols:
    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return _wire(ticker)

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        code = expiry_code_from_name(exch_ticker)
        body = exch_ticker.replace("-PERPETUAL", "")
        if code:
            body = exch_ticker.rsplit("-", 1)[0]
        symbol = body.replace("_", "") if "_" in body else f"{body}USD"
        if code:
            symbol = f"{symbol}-{code}"
        return UniversalTicker.of(venue, category, symbol)

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return None


def _summary(model: str = "segregated_sm") -> dict[str, Any]:
    return {
        "id": 1,
        "type": "main",
        "summaries": [
            {
                "currency": "BTC",
                "balance": "1",
                "equity": "1.2",
                "available_funds": "0.8",
                "margin_model": model,
            }
        ],
    }


class FakeStream:
    def __init__(self, summaries: dict[str, Any] | None = None) -> None:
        self.orders: EventStream[Any] = EventStream()
        self.fills: EventStream[Any] = EventStream()
        self.account: EventStream[Any] = EventStream()
        self.connected = False
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.results: dict[str, Any] = {
            ch.PRIVATE_GET_ACCOUNT_SUMMARIES: summaries or _summary(),
            ch.PRIVATE_BUY: {
                "order": {
                    "order_id": "ord-1",
                    "label": "c-42",
                    "instrument_name": "BTC_USDC",
                    "direction": "buy",
                    "order_type": "limit",
                    "order_state": "open",
                    "amount": "0.001",
                    "price": "60000",
                }
            },
            ch.PRIVATE_SELL: {
                "order": {
                    "order_id": "ord-2",
                    "instrument_name": "BTC_USDC-PERPETUAL",
                    "direction": "sell",
                    "order_type": "limit",
                    "order_state": "open",
                    "amount": "0.001",
                    "price": "60000",
                }
            },
            ch.PRIVATE_GET_POSITIONS: [
                {
                    "instrument_name": "BTC_USDC-PERPETUAL",
                    "kind": "future",
                    "size": "0.01",
                    "average_price": "60000",
                    "floating_profit_loss": "1",
                },
                {
                    "instrument_name": "BTC-PERPETUAL",
                    "kind": "future",
                    "size": "100",
                    "average_price": "60000",
                    "floating_profit_loss": "2",
                },
                {
                    "instrument_name": "BTC-6SEP26",
                    "kind": "future",
                    "size": "50",
                    "average_price": "60100",
                    "floating_profit_loss": "0",
                },
            ],
            ch.PRIVATE_GET_OPEN_ORDERS: [],
        }
        self._reconnect_cbs: list[Any] = []
        self.portfolios: list[str] = []

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    def on_reconnect(self, callback) -> None:
        self._reconnect_cbs.append(callback)

    async def rpc(
        self, method: str, params: dict[str, Any] | None = None
    ) -> Any:
        self.calls.append((method, params))
        return self.results.get(method, {})

    async def watch_portfolios(self, currencies) -> None:
        self.portfolios.extend(str(c).upper() for c in currencies)

    async def subscribe_orders(self) -> EventStream:
        return self.orders

    async def subscribe_fills(self) -> EventStream:
        return self.fills

    async def subscribe_account(self, currencies=()) -> EventStream:
        return self.account


def _client(stream: FakeStream | None = None) -> DeribitPrivateClient:
    return DeribitPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        symbols=StubSymbols(),
        stream=stream or FakeStream(),
    )


def test_a_missing_secret_fails_before_anything_is_sent() -> None:
    with pytest.raises(DeribitAuthError, match="api_secret"):
        DeribitPrivateClient(
            api_key="k",
            api_secret="",
            symbols=StubSymbols(),
        )


@pytest.mark.parametrize("model", sorted(MARGIN_MODELS))
async def test_v8_every_margin_model_is_accepted(model: str) -> None:
    stream = FakeStream(_summary(model))
    async with _client(stream) as client:
        assert client.connected
        assert client._margin_model == model


async def test_v7_non_post_only_sends_post_only_false() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        order = await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Deribit_Spot_BTCUSDC",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                tif=TimeInForce.GTC,
                client_order_id="c-42",
            )
        )
    method, params = stream.calls[-1]
    assert method == ch.PRIVATE_BUY
    assert params is not None
    assert params["instrument_name"] == "BTC_USDC"
    assert params["amount"] == 0.001
    assert params["post_only"] is False
    assert "reject_post_only" not in params
    assert params["time_in_force"] == "good_til_cancelled"
    assert params["label"] == "c-42"
    assert order.order_id == "ord-1"
    assert order.status is OrderStatus.NEW


async def test_v7_post_only_sends_reject_post_only() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Deribit_Spot_BTCUSDC",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                tif=TimeInForce.POST_ONLY,
            )
        )
    _, params = stream.calls[-1]
    assert params is not None
    assert params["post_only"] is True
    assert params["reject_post_only"] is True


async def test_v6_quote_qty_is_refused() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        with pytest.raises(OrderError, match="quote_qty"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Deribit_Spot_BTCUSDC",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    quote_qty=Decimal("10"),
                )
            )
        with pytest.raises(OrderError, match="quote_qty"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Deribit_Perp_BTCUSDC",
                    side=Side.SELL,
                    type=OrderType.LIMIT,
                    quote_qty=Decimal("10"),
                    price=Decimal("60000"),
                )
            )
        with pytest.raises(OrderError, match="quote_qty"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Deribit_Inverse_BTCUSD",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    quote_qty=Decimal("10"),
                )
            )
    assert not any(
        method in {ch.PRIVATE_BUY, ch.PRIVATE_SELL} for method, _ in stream.calls
    )


async def test_v9_balances_map_available_funds() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        balances = await client.fetch_balances()
    by_asset = {row.asset: row for row in balances}
    assert by_asset["BTC"].free == Decimal("0.8")
    assert by_asset["BTC"].locked == Decimal("0.4")
    assert "BTC" in stream.portfolios


async def test_fills_and_orders_resolve_the_linear_perp() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        fill_task = _first(client.stream_fills())
        stream.fills.push(
            DeribitFill.model_validate(
                {
                    "instrument_name": "BTC_USDC-PERPETUAL",
                    "trade_id": "t-1",
                    "order_id": "ord-2",
                    "direction": "sell",
                    "price": "60000",
                    "amount": "0.01",
                    "timestamp": 1700000000000,
                }
            )
        )
        fill = await fill_task
        stream.orders.push(
            DeribitOrderUpdate.model_validate(
                {
                    "instrument_name": "BTC_USDC-PERPETUAL",
                    "order_id": "ord-2",
                    "direction": "sell",
                    "order_type": "limit",
                    "order_state": "open",
                    "amount": "0.01",
                    "price": "60000",
                }
            )
        )
        order = await _first(client.stream_orders())
    assert fill.ticker == PERP
    assert order.ticker == PERP


async def test_inverse_and_dated_positions_resolve_home() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        rows = await client.fetch_positions()
    by_ticker = {row.universal_ticker: row.qty for row in rows}
    assert by_ticker[str(PERP)] == Decimal("0.01")
    assert by_ticker[str(INVERSE)] == Decimal("100")
    assert by_ticker[str(DATED)] == Decimal("50")


async def test_inverse_limit_sends_usd_amount() -> None:
    stream = FakeStream()
    stream.results[ch.PRIVATE_BUY] = {
        "order": {
            "order_id": "ord-inv",
            "instrument_name": "BTC-PERPETUAL",
            "direction": "buy",
            "order_type": "limit",
            "order_state": "open",
            "amount": "10",
            "price": "60000",
        }
    }
    async with _client(stream) as client:
        order = await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Deribit_Inverse_BTCUSD",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("10"),
                price=Decimal("60000"),
            )
        )
    method, params = stream.calls[-1]
    assert method == ch.PRIVATE_BUY
    assert params is not None
    assert params["instrument_name"] == "BTC-PERPETUAL"
    assert params["amount"] == 10.0
    assert order.universal_ticker == str(INVERSE)


async def test_fills_and_orders_resolve_inverse_and_dated() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        stream.fills.push(
            DeribitFill.model_validate(
                {
                    "instrument_name": "BTC-PERPETUAL",
                    "trade_id": "t-inv",
                    "order_id": "ord-inv",
                    "direction": "buy",
                    "price": "60000",
                    "amount": "10",
                    "timestamp": 1700000000000,
                }
            )
        )
        fill = await _first(client.stream_fills())
        stream.orders.push(
            DeribitOrderUpdate.model_validate(
                {
                    "instrument_name": "BTC-6SEP26",
                    "order_id": "ord-d",
                    "direction": "sell",
                    "order_type": "limit",
                    "order_state": "open",
                    "amount": "10",
                    "price": "60100",
                }
            )
        )
        order = await _first(client.stream_orders())
    assert fill.ticker == INVERSE
    assert order.ticker == DATED


async def test_no_position_stream() -> None:
    async with _client() as client:
        assert hasattr(client, "fetch_positions")
        assert not hasattr(client, "stream_positions")


async def _first(stream):
    return await asyncio.wait_for(stream.__anext__(), 2)


async def test_portfolio_push_maps_the_same_fields() -> None:
    stream = FakeStream()
    async with _client(stream) as client:
        task = _first(client.stream_balances())
        stream.account.push(
            DeribitSummary.model_validate(
                {
                    "currency": "USDC",
                    "equity": "100",
                    "available_funds": "40",
                }
            )
        )
        balance = await task
    assert balance.asset == "USDC"
    assert balance.free == Decimal("40")
    assert balance.locked == Decimal("60")

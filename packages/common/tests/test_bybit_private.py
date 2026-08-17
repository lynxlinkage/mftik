"""The Bybit trading connector — three transports behind one TD-shaped surface."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import httpx
import pytest
from bybit_stub import API_KEY, API_SECRET, FakeBybit
from mftik.exchange.bybit.account import BybitPrivateStream
from mftik.exchange.bybit.private import BybitPrivateClient
from mftik.exchange.bybit.rest import BybitRest
from mftik.exchange.bybit.trade import BybitTradeSocket
from mftik.exchange.errors import OrderError
from mftik.exchange.models import (
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    TimeInForce,
)
from mftik.exchange.tickers import Category, UniversalTicker

#: Bybit's spelling happens to differ from the canonical one here on purpose,
#: so a test would catch a connector that passed symbols through untranslated.
NATIVE = "BTC-USDT"
#: The instrument every order in this module is for.
TICKER = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
BASE = "https://bybit.test"

ORDER_ROW = {
    "category": "spot",
    "symbol": NATIVE,
    "orderId": "ord-1",
    "orderLinkId": "c-42",
    "side": "Buy",
    "orderType": "Limit",
    "orderStatus": "New",
    "price": "60000",
    "qty": "0.001",
    "cumExecQty": "0",
    "leavesQty": "0.001",
    "timeInForce": "GTC",
    "updatedTime": "1700000000000",
}

EXECUTION_ROW = {
    "category": "spot",
    "symbol": NATIVE,
    "orderId": "ord-1",
    "orderLinkId": "c-42",
    "execId": "e-1",
    "side": "Buy",
    "execPrice": "60000",
    "execQty": "0.001",
    "execFee": "0.06",
    "feeCurrency": "USDT",
    "execType": "Trade",
    "execTime": "1700000000000",
}


class StubSymbols:
    """A symbol plane whose venue spelling differs from the canonical one."""

    def __init__(self) -> None:
        self.categories: list[str] = []

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        self.categories.append(str(ticker.category))
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        assert exch_ticker == NATIVE, f"unexpected venue symbol {exch_ticker!r}"
        return UniversalTicker.of(venue, category, "BTCUSDT")


class FakeApi:
    """An httpx transport standing in for Bybit's REST API."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, Any] = {}

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "retMsg": "OK",
                "result": self.results.get(request.url.path, {}),
            },
        )


def _client(
    stub: FakeBybit,
    api: FakeApi,
    *,
    category: Category = Category.SPOT,
    symbols: StubSymbols | None = None,
) -> BybitPrivateClient:
    return BybitPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        symbols=symbols or StubSymbols(),
        category=category,
        trade=BybitTradeSocket(
            api_key=API_KEY, api_secret=API_SECRET, url=stub.url, ping_interval=0
        ),
        stream=BybitPrivateStream(
            api_key=API_KEY,
            api_secret=API_SECRET,
            url=stub.url,
            # Unscoped, like the one the connector builds for itself: a
            # unified account reports every book on one socket.
            product=None,
            ping_interval=0,
        ),
        rest=BybitRest(
            api_key=API_KEY,
            api_secret=API_SECRET,
            base_url=BASE,
            client=api.client(),
        ),
    )


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


# --- order entry -----------------------------------------------------------


async def test_a_limit_order_carries_the_category_and_the_venue_symbol(
    bybit: FakeBybit, api: FakeApi
) -> None:
    symbols = StubSymbols()
    async with _client(bybit, api, symbols=symbols) as client:
        order = await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                tif=TimeInForce.GTC,
                client_order_id="c-42",
            )
        )

    args = bybit.call("order.create")["args"][0]
    assert args["category"] == "spot"
    assert args["symbol"] == NATIVE
    assert args["timeInForce"] == "GTC"
    assert symbols.categories == ["Spot"]
    # The ack carries two ids and no state, so claiming NEW would claim the
    # order is resting — which is what an order that filled on arrival is not.
    assert order.status is OrderStatus.PENDING_NEW
    assert order.symbol == "BTCUSDT"
    assert order.order_id == "ord-1"


async def test_post_only_is_a_time_in_force_here(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Unlike Binance, no order type has to be swapped for it."""
    async with _client(bybit, api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Spot_BTCUSDT",
                side=Side.SELL,
                type=OrderType.LIMIT,
                qty=Decimal("1"),
                price=Decimal("60000"),
                tif=TimeInForce.POST_ONLY,
            )
        )
    args = bybit.call("order.create")["args"][0]
    assert args["orderType"] == "Limit"
    assert args["timeInForce"] == "PostOnly"


async def test_a_spot_market_order_is_sized_in_the_base_asset(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """The single most expensive thing to get wrong on this venue: Bybit reads
    ``qty`` on a spot market buy as quote currency unless told otherwise, so an
    order for 0.5 BTC would spend 50 cents."""
    async with _client(bybit, api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.MARKET,
                qty=Decimal("0.5"),
            )
        )
    args = bybit.call("order.create")["args"][0]
    assert args["marketUnit"] == "baseCoin"
    # A market order cannot rest, so there is nothing to say about how long.
    assert "timeInForce" not in args


async def test_one_session_places_on_both_books(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """The order says which book, so one connector covers the whole account.

    This is what the ticker on the request bought: a unified credential trades
    spot and perps, and until the order carried an instrument the only way to
    reach the second book was a second session — which would have kept its own
    ledger over the same wallet and let the two overspend it together.
    """
    async with _client(bybit, api, category=Category.SPOT) as client:
        for ticker in ("Bybit_Spot_BTCUSDT", "Bybit_Perp_BTCUSDT"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker=ticker,
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    qty=Decimal("1"),
                )
            )

    spot, perp = (f["args"][0] for f in bybit.frames_for("order.create"))
    assert spot["category"] == "spot"
    assert perp["category"] == "linear"
    # And the spot-only trap is applied per order, not per session: a spot
    # market buy sizes in quote currency unless told otherwise, and the
    # contract books have no such parameter and refuse it.
    assert spot["marketUnit"] == "baseCoin"
    assert "marketUnit" not in perp


async def test_an_order_for_another_venue_never_reaches_the_wire(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """TD checks this too, where a strategy's mistake belongs. The connector
    says it for itself because it is reachable directly, and an order routed
    to the wrong venue cannot be undone by noticing later."""
    async with _client(bybit, api) as client:
        with pytest.raises(OrderError, match="Binance order"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Binance_Spot_BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    qty=Decimal("1"),
                )
            )
    assert not bybit.frames_for("order.create")


async def test_a_limit_order_with_no_price_never_reaches_the_venue(
    bybit: FakeBybit, api: FakeApi
) -> None:
    async with _client(bybit, api) as client:
        with pytest.raises(OrderError, match="requires a price"):
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Bybit_Spot_BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.LIMIT,
                    qty=Decimal("1"),
                )
            )
    assert not bybit.frames_for("order.create")


async def test_params_may_not_shadow_a_field_the_request_carries(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """A key that contradicts the request would be a silent contradiction."""
    async with _client(bybit, api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("1"),
                price=Decimal("60000"),
                params={"symbol": "ETHUSDT", "category": "linear", "isLeverage": 1},
            )
        )
    args = bybit.call("order.create")["args"][0]
    assert args["symbol"] == NATIVE
    assert args["category"] == "spot"
    # A venue-only option this connector has no opinion about still rides
    # along — and as the integer Bybit documents, not a stringified one.
    assert args["isLeverage"] == 1


async def test_a_venue_rejection_becomes_an_order_error(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """So TD publishes an order reject rather than treating it as transport
    trouble — and the code survives on the ``__cause__`` for normalization."""
    bybit.errors["order.create"] = (110007, "ab not enough for new order")
    async with _client(bybit, api) as client:
        with pytest.raises(OrderError) as exc:
            await client.place_order(
                PlaceOrderRequest(
                    universal_ticker="Bybit_Spot_BTCUSDT",
                    side=Side.BUY,
                    type=OrderType.MARKET,
                    qty=Decimal("1"),
                )
            )
    assert "110007" in str(exc.value)
    assert getattr(exc.value.__cause__, "code", None) == 110007


# --- cancel ----------------------------------------------------------------


async def test_a_cancel_uses_the_symbol_the_place_taught_it(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Bybit needs the symbol to cancel, and TD addresses an order by id."""
    async with _client(bybit, api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Spot_BTCUSDT",
                side=Side.BUY,
                type=OrderType.LIMIT,
                qty=Decimal("0.001"),
                price=Decimal("60000"),
                client_order_id="c-42",
            )
        )
        order = await client.cancel_by_client_order_id("c-42")

    args = bybit.call("order.cancel")["args"][0]
    assert args["symbol"] == NATIVE
    assert args["orderLinkId"] == "c-42"
    # No REST round trip was needed to answer.
    assert not api.requests
    assert order.status is OrderStatus.PENDING_CANCEL
    assert order.symbol == "BTCUSDT"
    assert order.qty == Decimal("0.001")


async def test_a_cancel_for_an_unseen_order_looks_it_up(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """After a restart the connector has taught itself nothing yet."""
    api.results["/v5/order/realtime"] = {"list": [ORDER_ROW]}
    async with _client(bybit, api) as client:
        order = await client.cancel_by_client_order_id("c-42")

    assert bybit.call("order.cancel")["args"][0]["symbol"] == NATIVE
    assert order.status is OrderStatus.PENDING_CANCEL
    assert order.price == Decimal("60000")


async def test_an_id_no_open_order_matches_is_refused_before_the_cancel(
    bybit: FakeBybit, api: FakeApi
) -> None:
    api.results["/v5/order/realtime"] = {"list": []}
    async with _client(bybit, api) as client:
        with pytest.raises(OrderError, match="no open Bybit order"):
            await client.cancel_by_client_order_id("c-nope")
    assert not bybit.frames_for("order.cancel")


# --- streams ---------------------------------------------------------------


async def test_order_updates_come_home_canonical(
    bybit: FakeBybit, api: FakeApi
) -> None:
    async with _client(bybit, api) as client:
        stream = client.stream_orders()
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit.push("order", [ORDER_ROW])
        order = await asyncio.wait_for(task, 2)

    assert order.symbol == "BTCUSDT"
    assert order.status is OrderStatus.NEW
    assert order.client_order_id == "c-42"


async def test_only_real_executions_reach_the_fill_stream(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Funding and ADL arrive on the same topic and are not something an order
    did."""
    async with _client(bybit, api) as client:
        stream = client.stream_fills()
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit.push("execution", [{**EXECUTION_ROW, "execType": "Funding"}])
        await bybit.push("execution", [EXECUTION_ROW])
        fill = await asyncio.wait_for(task, 2)

    assert fill.fill_id == "e-1"
    assert fill.symbol == "BTCUSDT"
    assert fill.fee == Decimal("0.06")


async def test_a_wallet_push_becomes_one_balance_per_coin(
    bybit: FakeBybit, api: FakeApi
) -> None:
    async with _client(bybit, api) as client:
        stream = client.stream_balances()
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit.push(
            "wallet",
            [
                {
                    "accountType": "UNIFIED",
                    "coin": [
                        {"coin": "USDT", "walletBalance": "100",
                         "availableToWithdraw": "90", "locked": "10"}
                    ],
                }
            ],
        )
        balance = await asyncio.wait_for(task, 2)

    assert balance.asset == "USDT"
    assert balance.free == Decimal("90")
    assert balance.locked == Decimal("10")


async def test_positions_stream_off_the_contract_book(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """A position moves for reasons no fill reports — funding, ADL — so the
    venue's own figure is what a strategy has to be told."""
    async with _client(bybit, api) as client:
        stream = client.stream_positions()
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit.push(
            "position",
            [
                {
                    "category": "linear",
                    "symbol": NATIVE,
                    "side": "Sell",
                    "size": "3",
                    "entryPrice": "60000",
                    "unrealisedPnl": "-12",
                    "positionIdx": 0,
                }
            ],
        )
        position = await asyncio.wait_for(task, 2)

    assert position.qty == Decimal("-3")
    assert position.entry_price == Decimal("60000")
    assert position.unrealised_pnl == Decimal("-12")
    # Resolved onto the perp the row named, even though this session's order
    # path is on spot.
    assert position.universal_ticker == "Bybit_Perp_BTCUSDT"


async def test_a_closed_position_arrives_as_a_zero(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Not as a message that stops coming — which is how the OMS learns to
    drop it rather than carrying a stale one forever."""
    async with _client(bybit, api) as client:
        stream = client.stream_positions()
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit.push(
            "position",
            [{"category": "linear", "symbol": NATIVE, "side": "", "size": "0"}],
        )
        position = await asyncio.wait_for(task, 2)

    assert position.flat


async def test_the_account_stream_is_not_scoped_to_one_book(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """One credential is one account. A spot-ordering session that hid the
    perp fills would be reporting a wallet it could not explain."""
    async with _client(bybit, api, category=Category.SPOT) as client:
        fills = client.stream_fills()
        task = asyncio.ensure_future(fills.__anext__())
        await asyncio.sleep(0.05)
        await bybit.push("execution", [{**EXECUTION_ROW, "category": "linear"}])
        fill = await asyncio.wait_for(task, 2)

    # Subscribed unscoped, so the perp row arrives at all …
    assert bybit.subscribed == {"execution"}
    # … and is resolved on the book it names, not on this session's.
    assert fill.universal_ticker == "Bybit_Perp_BTCUSDT"


# --- recon -----------------------------------------------------------------


async def test_open_orders_and_balances_come_from_rest(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Neither has a WebSocket form on this venue, and recon needs both."""
    api.results["/v5/order/realtime"] = {"list": [ORDER_ROW]}
    api.results["/v5/account/wallet-balance"] = {
        "list": [
            {
                "accountType": "UNIFIED",
                "coin": [
                    {"coin": "USDT", "walletBalance": "100",
                     "availableToWithdraw": "100"}
                ],
            }
        ]
    }
    async with _client(bybit, api) as client:
        orders = await client.fetch_open_orders()
        balances = await client.fetch_balances()

    # Both books asked, because recon wants the account rather than one of
    # its markets — the stub answers the same row for each.
    assert [o.symbol for o in orders] == ["BTCUSDT", "BTCUSDT"]
    assert [b.asset for b in balances] == ["USDT"]


async def test_an_order_the_stream_never_reported_can_be_chased(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """TD's way out of PENDING_NEW: ``None`` means the submit never landed."""
    api.results["/v5/order/realtime"] = {"list": []}
    api.results["/v5/order/history"] = {"list": []}
    async with _client(bybit, api) as client:
        assert (
            await client.fetch_order_by_client_order_id("c-42", ticker=TICKER)
        ) is None

    paths = [r.url.path for r in api.requests]
    assert "/v5/order/realtime" in paths and "/v5/order/history" in paths


async def test_a_spot_session_still_reports_the_accounts_positions(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """A unified account is one account.

    ``category`` says which book this connector *places orders on*; it does
    not narrow what the account holds. A spot-only session that reported no
    positions would be describing the instrument it trades rather than the
    money it is trading with — and its perp exposure moves the same wallet.
    """
    api.results["/v5/position/list"] = {
        "list": [{"symbol": NATIVE, "side": "Buy", "size": "3",
                  "category": "linear"}]
    }
    async with _client(bybit, api, category=Category.SPOT) as client:
        positions = await client.fetch_positions()

    # Read off the contract book, which is the only place positions exist.
    request = next(r for r in api.requests if r.url.path == "/v5/position/list")
    assert "category=linear" in request.url.query.decode()
    assert [(p.symbol, p.qty) for p in positions] == [("BTCUSDT", Decimal("3"))]
    # And resolved onto the perp, not onto the spot pair of the same name.
    assert positions[0].universal_ticker == "Bybit_Perp_BTCUSDT"


async def test_perp_positions_come_home_canonical(
    bybit: FakeBybit, api: FakeApi
) -> None:
    api.results["/v5/position/list"] = {
        "list": [{"symbol": NATIVE, "side": "Sell", "size": "2"}]
    }
    async with _client(bybit, api, category=Category.PERP) as client:
        positions = await client.fetch_positions()

    assert [(p.symbol, p.qty) for p in positions] == [("BTCUSDT", Decimal("-2"))]


# --- lifecycle -------------------------------------------------------------


async def test_a_reconnect_on_either_socket_is_reported(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Either one dropping leaves TD's view of the account older than the
    account: the trade socket loses its auth, the stream its subscriptions."""
    client = _client(bybit, api)
    seen: list[str] = []
    async with client:
        client.on_reconnect(lambda: seen.append("x"))
        client.trade.retry_backoff = 0.01
        client.stream.retry_backoff = 0.01
        await bybit.drop()
        for _ in range(200):
            if len(seen) >= 2:
                break
            await asyncio.sleep(0.01)
    assert len(seen) == 2


async def test_a_half_open_connector_does_not_survive_connect(
    api: FakeApi,
) -> None:
    """An order path with no report path would place orders it could never
    hear about."""
    client = BybitPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        symbols=StubSymbols(),
        trade_url="ws://127.0.0.1:1",
        private_url="ws://127.0.0.1:1",
        rest=BybitRest(
            api_key=API_KEY, api_secret=API_SECRET, base_url=BASE, client=api.client()
        ),
    )
    with pytest.raises(OSError):
        await client.connect()
    assert not client.connected
    assert not client.trade.connected
    assert not client.stream.connected
    await client.close()


def test_the_connector_states_the_category_td_reads_off_it() -> None:
    """TD builds an instrument's ticker from ``name`` and ``category``, so a
    unified venue's connector has to be built for one book."""
    client = BybitPrivateClient(
        api_key=API_KEY,
        api_secret=API_SECRET,
        symbols=StubSymbols(),
        category=Category.PERP,
    )
    assert client.name == "Bybit"
    assert client.category is Category.PERP
    assert client.product == "linear"
    assert str(UniversalTicker.of(client.name, client.category, "BTCUSDT")) == (
        "Bybit_Perp_BTCUSDT"
    )


async def test_reduce_only_reaches_the_venue_on_a_perp(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """A real boolean, not the string "true" — see ``protocol.query_string``."""
    async with _client(bybit, api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Perp_BTCUSDT",
                side=Side.SELL,
                type=OrderType.LIMIT,
                qty=Decimal("1"),
                price=Decimal("60000"),
                tif=TimeInForce.IOC,
                reduce_only=True,
                client_order_id="c-42",
            )
        )

    args = bybit.call("order.create")["args"][0]
    assert args["category"] == "linear"
    assert args["reduceOnly"] is True


async def test_an_ordinary_order_says_nothing_about_reduce_only(
    bybit: FakeBybit, api: FakeApi
) -> None:
    """Off means absent, not ``false`` — the wire is what it always was."""
    async with _client(bybit, api) as client:
        await client.place_order(
            PlaceOrderRequest(
                universal_ticker="Bybit_Perp_BTCUSDT",
                side=Side.SELL,
                type=OrderType.LIMIT,
                qty=Decimal("1"),
                price=Decimal("60000"),
                tif=TimeInForce.IOC,
                client_order_id="c-42",
            )
        )

    assert "reduceOnly" not in bybit.call("order.create")["args"][0]

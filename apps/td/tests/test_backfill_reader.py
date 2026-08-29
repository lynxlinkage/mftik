"""The history readers — pagination, in each venue's own terms.

The executor treats a cursor as one opaque string, so every venue's paging
arithmetic lives in its reader and this is where it has to be right. Each gets
it wrong differently and silently: ``fromId`` is inclusive on Binance, so
resuming from the last id read re-reads it forever; Bybit hands out a token
that must be passed back untouched; and Gate numbers pages, which means nothing
unless the query they were numbered against comes with them.

A short page is the shared signal — it is what says the walk is drained, and
the only thing that lets the settlement line advance to its ceiling.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest
from binance_stub import keypair
from mftik.exchange.binance.delivery.rest import BinanceDeliveryRest
from mftik.exchange.binance.spot.rest import BinanceSpotRest
from mftik.exchange.models import Side
from mftik.exchange.tickers import UniversalTicker
from mftik_td.backfill.reader import (
    BinanceDeliveryHistoryReader,
    BinanceSpotHistoryReader,
    BybitHistoryReader,
    GateFuturesHistoryReader,
    GateSpotHistoryReader,
    HistoryReaderFactory,
    NoHistoryReaderError,
    OkxHistoryReader,
)

TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")
BASE = "https://api.test"


class FakeSymbols:
    """The symbol plane, as far as a reader uses it."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return ticker.symbol

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        return Decimal("0.0001")


class FakeApi:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, object] = {}

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200, json=self.results.get(request.url.path, []))


def trade(trade_id: int, order_id: int = 500) -> dict:
    return {
        "symbol": "BTCUSDT",
        "id": trade_id,
        "orderId": order_id,
        "price": "100.0",
        "qty": "0.25",
        "quoteQty": "25.0",
        "commission": "0.001",
        "commissionAsset": "BNB",
        "time": 1499865549590,
        "isBuyer": True,
        "isMaker": False,
    }


def order(order_id: int, cid: str = "cid-1") -> dict:
    return {
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "clientOrderId": cid,
        "price": "100.0",
        "origQty": "0.25",
        "executedQty": "0.25",
        "cummulativeQuoteQty": "25.0",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "time": 1499827319559,
        "updateTime": 1499865549590,
    }


def reader(api: FakeApi) -> BinanceSpotHistoryReader:
    _key, pem = keypair()
    return BinanceSpotHistoryReader(
        symbols=FakeSymbols(),
        rest=BinanceSpotRest(
            api_key="k", api_secret=pem, base_url=BASE, client=api.client()
        ),
    )


def params_of(request: httpx.Request) -> dict[str, str]:
    from urllib.parse import parse_qsl

    signed, _, _sig = request.url.query.decode().rpartition("&signature=")
    return dict(parse_qsl(signed))


# --- trades ----------------------------------------------------------------


async def test_a_short_page_means_the_walk_is_drained() -> None:
    """The only signal that lets the settlement line advance to the ceiling."""
    api = FakeApi()
    api.results["/api/v3/myTrades"] = [trade(1), trade(2)]

    page = await reader(api).fetch_my_trades(TICKER, limit=10)

    assert page.next_cursor is None
    assert [f.fill_id for f in page.rows] == ["1", "2"]


async def test_a_full_page_resumes_one_past_the_highest_id() -> None:
    """``fromId`` is inclusive: resuming from the last id read re-reads it."""
    api = FakeApi()
    api.results["/api/v3/myTrades"] = [trade(7), trade(9), trade(8)]

    page = await reader(api).fetch_my_trades(TICKER, limit=3)

    assert page.next_cursor == "10"


async def test_a_cursor_is_sent_as_from_id_and_suppresses_the_time() -> None:
    api = FakeApi()
    api.results["/api/v3/myTrades"] = []

    await reader(api).fetch_my_trades(TICKER, cursor="42", since_ts=1_600_000_000)

    params = params_of(api.requests[0])
    assert params["fromId"] == "42"
    assert "startTime" not in params, "Binance ignores the range when an id is set"


async def test_an_opening_walk_is_addressed_by_time_in_milliseconds() -> None:
    api = FakeApi()
    api.results["/api/v3/myTrades"] = []

    await reader(api).fetch_my_trades(TICKER, since_ts=1_600_000_000.5)

    params = params_of(api.requests[0])
    assert params["startTime"] == "1600000000500"
    assert "fromId" not in params


async def test_a_backfilled_fill_carries_no_client_order_id() -> None:
    """Binance does not put one on a trade row; inventing one would make an
    execution look attributable when it is not."""
    api = FakeApi()
    api.results["/api/v3/myTrades"] = [trade(1)]

    page = await reader(api).fetch_my_trades(TICKER)

    fill = page.rows[0]
    assert fill.client_order_id is None
    assert fill.order_id == "500"
    assert fill.side is Side.BUY
    assert fill.qty == Decimal("0.25")
    assert fill.universal_ticker == str(TICKER)


# --- orders ----------------------------------------------------------------


async def test_orders_page_on_the_order_id() -> None:
    api = FakeApi()
    api.results["/api/v3/allOrders"] = [order(11), order(12)]

    page = await reader(api).fetch_orders(TICKER, limit=2)

    assert page.next_cursor == "13"
    assert [o.order_id for o in page.rows] == ["11", "12"]


async def test_orders_carry_the_client_order_id_trades_lack() -> None:
    api = FakeApi()
    api.results["/api/v3/allOrders"] = [order(11, cid="281474976710656001")]

    page = await reader(api).fetch_orders(TICKER)

    assert page.rows[0].client_order_id == "281474976710656001"


# --- routing ---------------------------------------------------------------


async def test_a_reader_refuses_another_venues_ticker() -> None:
    """A misrouted ticker would be asked of the wrong account entirely."""
    api = FakeApi()
    with pytest.raises(ValueError, match="Bybit"):
        await reader(api).fetch_my_trades(
            UniversalTicker.parse("Bybit_Spot_BTCUSDT")
        )


async def test_a_venue_with_no_reader_says_so_by_name() -> None:
    factory = HistoryReaderFactory(FakeSymbols())
    row = type("Row", (), {"api_key": "k", "api_secret": "s"})()

    with pytest.raises(NoHistoryReaderError, match="Nowhere"):
        await factory.create("Nowhere", row)


# --- Binance COIN-M --------------------------------------------------------


DELIVERY = UniversalTicker.parse("BinanceDelivery_Inverse_BTCUSD")


def delivery_trade(trade_id: int, order_id: int = 28) -> dict:
    return {
        "symbol": "BTCUSD_PERP",
        "id": trade_id,
        "orderId": order_id,
        "pair": "BTCUSD",
        "side": "SELL",
        "price": "8800",
        "qty": "2",
        "realizedPnl": "0.01",
        "marginAsset": "BTC",
        "baseQty": "0.0227",
        "commission": "0.00000454",
        "commissionAsset": "BTC",
        "time": 1590743483586,
        "positionSide": "BOTH",
        "buyer": False,
        "maker": False,
    }


def delivery_order(order_id: int, cid: str = "c-1") -> dict:
    return {
        "symbol": "BTCUSD_PERP",
        "orderId": order_id,
        "clientOrderId": cid,
        "price": "8800",
        "origQty": "2",
        "executedQty": "2",
        "status": "FILLED",
        "type": "LIMIT",
        "side": "BUY",
        "updateTime": 1590743483586,
    }


def delivery_reader(api: FakeApi) -> BinanceDeliveryHistoryReader:
    _key, pem = keypair()
    return BinanceDeliveryHistoryReader(
        symbols=FakeSymbols(),
        rest=BinanceDeliveryRest(
            api_key="k", api_secret=pem, base_url=BASE, client=api.client()
        ),
    )


async def test_delivery_a_short_page_means_the_walk_is_drained() -> None:
    api = FakeApi()
    api.results["/dapi/v1/userTrades"] = [delivery_trade(1), delivery_trade(2)]

    page = await delivery_reader(api).fetch_my_trades(DELIVERY, limit=10)

    assert page.next_cursor is None
    assert [f.fill_id for f in page.rows] == ["1", "2"]


async def test_delivery_a_full_page_resumes_one_past_the_highest_id() -> None:
    """``fromId`` is inclusive: resuming from the last id read re-reads it."""
    api = FakeApi()
    api.results["/dapi/v1/userTrades"] = [
        delivery_trade(7),
        delivery_trade(9),
        delivery_trade(8),
    ]

    page = await delivery_reader(api).fetch_my_trades(DELIVERY, limit=3)

    assert page.next_cursor == "10"


async def test_delivery_a_cursor_is_sent_as_from_id_and_suppresses_the_time() -> None:
    api = FakeApi()
    api.results["/dapi/v1/userTrades"] = []

    await delivery_reader(api).fetch_my_trades(
        DELIVERY, cursor="42", since_ts=1_600_000_000
    )

    params = params_of(api.requests[0])
    assert params["fromId"] == "42"
    assert "startTime" not in params, "Binance ignores the range when an id is set"


async def test_delivery_an_opening_walk_is_addressed_by_time_in_milliseconds() -> None:
    api = FakeApi()
    api.results["/dapi/v1/userTrades"] = []

    await delivery_reader(api).fetch_my_trades(DELIVERY, since_ts=1_600_000_000.5)

    params = params_of(api.requests[0])
    assert params["startTime"] == "1600000000500"
    assert "fromId" not in params


async def test_delivery_a_backfilled_fill_is_contracts_and_has_no_client_id() -> None:
    """``qty`` is contracts; ``baseQty`` is not a fill size.

    ``client_order_id`` stays unset — dapi puts none on a trade row, and
    inventing one would make an execution look attributable when it is not.
    """
    api = FakeApi()
    api.results["/dapi/v1/userTrades"] = [delivery_trade(6)]

    page = await delivery_reader(api).fetch_my_trades(DELIVERY)

    fill = page.rows[0]
    assert fill.client_order_id is None
    assert fill.order_id == "28"
    assert fill.side is Side.SELL
    assert fill.qty == Decimal("2")
    assert fill.universal_ticker == str(DELIVERY)


async def test_delivery_orders_page_on_the_order_id() -> None:
    api = FakeApi()
    api.results["/dapi/v1/allOrders"] = [delivery_order(11), delivery_order(12)]

    page = await delivery_reader(api).fetch_orders(DELIVERY, limit=2)

    assert page.next_cursor == "13"
    assert [o.order_id for o in page.rows] == ["11", "12"]


async def test_delivery_orders_carry_the_client_order_id_trades_lack() -> None:
    api = FakeApi()
    api.results["/dapi/v1/allOrders"] = [
        delivery_order(11, cid="281474976710656001")
    ]

    page = await delivery_reader(api).fetch_orders(DELIVERY)

    assert page.rows[0].client_order_id == "281474976710656001"
    assert page.rows[0].qty == Decimal("2")


async def test_delivery_refuses_another_venues_ticker() -> None:
    api = FakeApi()
    with pytest.raises(ValueError, match="BinanceFuture"):
        await delivery_reader(api).fetch_my_trades(
            UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")
        )


# --- the other venues ------------------------------------------------------


class FakeBybitRest:
    """Answers the shape ``BybitRest`` returns: rows plus its own cursor."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.executions: tuple[list, str | None] = ([], None)
        self.orders: tuple[list, str | None] = ([], None)

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def fetch_executions(self, product, symbol, **kw):
        self.calls.append(("executions", {"product": product, "symbol": symbol, **kw}))
        return self.executions

    async def fetch_order_history(self, product, symbol, **kw):
        self.calls.append(("orders", {"product": product, "symbol": symbol, **kw}))
        return self.orders


def bybit_execution(exec_id: str, *, exec_type: str = "Trade") -> Any:
    from mftik.exchange.bybit.models import BybitExecution

    return BybitExecution.model_validate(
        {
            "category": "spot",
            "symbol": "BTCUSDT",
            "orderId": "500",
            "orderLinkId": "cid-1",
            "execId": exec_id,
            "side": "Buy",
            "execPrice": "63863.5",
            "execQty": "0.25",
            "execFee": "0.001",
            "feeCurrency": "USDT",
            "execType": exec_type,
            "execTime": "1699999999000",
        }
    )


async def test_bybit_asks_the_venue_for_trades_only() -> None:
    """Funding, ADL and delivery ride the same endpoint.

    Dropping them at the venue costs a parameter; doing it here would cost
    pages and rate-limit budget shared with whatever is trading on the key.
    """
    rest = FakeBybitRest()
    reader = BybitHistoryReader(symbols=FakeSymbols(), rest=rest)

    await reader.fetch_my_trades(
        UniversalTicker.parse("Bybit_Spot_BTCUSDT"), since_ts=1_600_000_000
    )

    assert rest.calls[0][0] == "executions"
    assert rest.calls[0][1]["start_time"] == 1_600_000_000_000


async def test_bybit_passes_its_own_cursor_straight_back() -> None:
    """The venue numbers its own pages; nothing here does arithmetic on it."""
    rest = FakeBybitRest()
    rest.executions = ([bybit_execution("e1")], "page-token-2")
    reader = BybitHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))

    assert page.next_cursor == "page-token-2"
    assert [f.fill_id for f in page.rows] == ["e1"]


async def test_bybit_drops_a_non_trade_row_that_slips_through() -> None:
    rest = FakeBybitRest()
    rest.executions = (
        [bybit_execution("e1"), bybit_execution("e2", exec_type="Funding")],
        None,
    )
    reader = BybitHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))

    assert [f.fill_id for f in page.rows] == ["e1"]


async def test_bybit_reads_the_book_off_the_ticker() -> None:
    """One credential covers every category, so the book is a parameter."""
    rest = FakeBybitRest()
    reader = BybitHistoryReader(symbols=FakeSymbols(), rest=rest)

    await reader.fetch_my_trades(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))
    await reader.fetch_my_trades(UniversalTicker.parse("Bybit_Perp_BTCUSDT"))

    assert [c[1]["product"] for c in rest.calls] == ["spot", "linear"]


class FakeGateRest:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.trades: list = []

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def fetch_my_trades(self, pair, **kw):
        self.calls.append({"pair": pair, **kw})
        return self.trades

    async def fetch_orders(self, pair, **kw):
        self.calls.append({"pair": pair, **kw})
        return []


def gate_trade(trade_id: int) -> Any:
    from mftik.exchange.gate.spot.models import GateUserTrade

    return GateUserTrade.model_validate(
        {
            "id": trade_id,
            "order_id": "500",
            "currency_pair": "BTC_USDT",
            "create_time_ms": "1699999999000",
            "side": "buy",
            "amount": "0.25",
            "role": "taker",
            "price": "63863.5",
            "fee": "0.001",
            "fee_currency": "USDT",
            "text": "t-281474976710656001",
        }
    )


async def test_gate_carries_the_window_and_the_page_in_one_cursor() -> None:
    """Gate numbers pages, and a page number means nothing on its own.

    The cursor holds the query it belongs to as well, so a resumed page is the
    same question asked again rather than a different one.
    """
    rest = FakeGateRest()
    rest.trades = [gate_trade(i) for i in range(3)]
    reader = GateSpotHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(
        UniversalTicker.parse("Gate_Spot_BTCUSDT"), since_ts=1_600_000_000, limit=3
    )

    assert rest.calls[0]["page"] == 1
    assert page.next_cursor == "1600000000:2"


async def test_gate_is_asked_in_seconds_not_milliseconds() -> None:
    """The one venue here that wants seconds, and it will not say so.

    Verified against the live endpoint: ``from`` in seconds returns rows,
    ``from`` in milliseconds returns ``200`` and ``[]`` — a window starting
    fifty thousand years out. Every layer above reads that empty page as a
    drained walk and settles the account on history it never saw, so the unit
    is the whole correctness of the Gate backfill and belongs in a test that
    says so out loud.
    """
    rest = FakeGateRest()
    reader = GateSpotHistoryReader(symbols=FakeSymbols(), rest=rest)

    await reader.fetch_my_trades(
        UniversalTicker.parse("Gate_Spot_BTCUSDT"), since_ts=1_600_000_000.75
    )
    await reader.fetch_orders(
        UniversalTicker.parse("Gate_Spot_BTCUSDT"), since_ts=1_600_000_000.75
    )

    assert [call["since"] for call in rest.calls] == [1_600_000_000] * 2, (
        "seconds, truncated — not 1_600_000_000_750"
    )


async def test_gate_resumes_the_same_window_on_the_next_page() -> None:
    rest = FakeGateRest()
    reader = GateSpotHistoryReader(symbols=FakeSymbols(), rest=rest)

    await reader.fetch_my_trades(
        UniversalTicker.parse("Gate_Spot_BTCUSDT"), cursor="1600000000:2"
    )

    assert rest.calls[0]["page"] == 2
    assert rest.calls[0]["since"] == 1_600_000_000


async def test_gate_stops_on_a_short_page() -> None:
    rest = FakeGateRest()
    rest.trades = [gate_trade(1)]
    reader = GateSpotHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(
        UniversalTicker.parse("Gate_Spot_BTCUSDT"), limit=10
    )

    assert page.next_cursor is None


async def test_a_gate_fill_arrives_already_attributable() -> None:
    """The one venue whose trade rows carry our client order id."""
    rest = FakeGateRest()
    rest.trades = [gate_trade(1)]
    reader = GateSpotHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(UniversalTicker.parse("Gate_Spot_BTCUSDT"))

    assert page.rows[0].client_order_id == "281474976710656001"


def gate_futures_trade(trade_id: int) -> Any:
    from mftik.exchange.gate.future.models import GateFuturesUserTrade

    return GateFuturesUserTrade.model_validate(
        {
            "id": trade_id,
            "order_id": "500",
            "contract": "BTC_USDT",
            "size": "4",
            "price": "63863.5",
            "text": "t-281474976710656001",
            "create_time": 1_699_999_999,
        }
    )


class FakeGateFuturesRest:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.trades: list[Any] = []

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def fetch_my_trades(self, pair, **kw):
        self.calls.append({"pair": pair, **kw})
        return self.trades

    async def fetch_orders(self, pair, **kw):
        self.calls.append({"pair": pair, **kw})
        return []


async def test_gate_futures_pages_by_offset_in_seconds() -> None:
    rest = FakeGateFuturesRest()
    rest.trades = [gate_futures_trade(i) for i in range(3)]
    reader = GateFuturesHistoryReader(symbols=FakeSymbols(), rest=rest)
    ticker = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")

    page = await reader.fetch_my_trades(ticker, since_ts=1_600_000_000.75, limit=3)

    assert rest.calls[0]["offset"] == 0
    assert rest.calls[0]["since"] == 1_600_000_000
    assert page.next_cursor == "1600000000:3"
    assert page.rows[0].qty == Decimal("0.0004")
    assert page.rows[0].client_order_id == "281474976710656001"

    await reader.fetch_my_trades(ticker, cursor=page.next_cursor)
    assert rest.calls[1]["offset"] == 3
    assert rest.calls[1]["since"] == 1_600_000_000


class FakeOkxRest:
    """Answers the shape ``OkxRest`` returns: a list, newest first."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fills: list = []
        self.orders: list = []

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def fetch_fills(self, product, inst_id=None, **kw):
        self.calls.append(("fills", {"product": product, "inst_id": inst_id, **kw}))
        return self.fills

    async def fetch_order_history(self, product, inst_id=None, **kw):
        self.calls.append(("orders", {"product": product, "inst_id": inst_id, **kw}))
        return self.orders


def okx_fill(
    trade_id: str,
    *,
    bill_id: str = "",
    fill_sz: str = "0.25",
) -> Any:
    from mftik.exchange.okx.models import OkxFill

    return OkxFill.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "500",
            "clOrdId": "281474976710656001",
            "tradeId": trade_id,
            "billId": bill_id,
            "side": "buy",
            "fillPx": "63863.5",
            "fillSz": fill_sz,
            "fillFee": "-0.001",
            "fillFeeCcy": "USDT",
            "ts": "1699999999000",
        }
    )


def okx_order(ord_id: str, *, sz: str = "0.25") -> Any:
    from mftik.exchange.okx.models import OkxOrderUpdate

    return OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": ord_id,
            "clOrdId": "281474976710656001",
            "side": "buy",
            "ordType": "limit",
            "state": "filled",
            "px": "63863.5",
            "sz": sz,
            "accFillSz": sz,
            "uTime": "1699999999000",
        }
    )


async def test_okx_asks_in_milliseconds_and_pages_by_bill_id() -> None:
    """``after`` is a billId, not a tradeId — OKX's own pagination key."""
    rest = FakeOkxRest()
    rest.fills = [okx_fill("t1", bill_id="b1"), okx_fill("t2", bill_id="b2")]
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)
    ticker = UniversalTicker.parse("Okx_Spot_BTCUSDT")

    page = await reader.fetch_my_trades(ticker, since_ts=1_600_000_000.5, limit=2)

    assert rest.calls[0][1]["begin"] == 1_600_000_000_500
    assert rest.calls[0][1]["after"] is None
    assert rest.calls[0][1]["product"] == "SPOT"
    assert page.next_cursor == "b2"
    assert [f.fill_id for f in page.rows] == ["t1", "t2"]
    assert page.rows[0].client_order_id == "281474976710656001"

    await reader.fetch_my_trades(
        ticker, cursor=page.next_cursor, since_ts=1_600_000_000
    )
    assert rest.calls[1][1]["after"] == "b2"
    assert rest.calls[1][1]["begin"] is None


async def test_okx_orders_page_on_the_ord_id() -> None:
    rest = FakeOkxRest()
    rest.orders = [okx_order("o1"), okx_order("o2")]
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_orders(
        UniversalTicker.parse("Okx_Spot_BTCUSDT"), limit=2
    )

    assert page.next_cursor == "o2"
    assert [o.order_id for o in page.rows] == ["o1", "o2"]


async def test_okx_reads_the_book_off_the_ticker() -> None:
    """One credential covers every category, so the book is a parameter."""
    rest = FakeOkxRest()
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)

    await reader.fetch_my_trades(UniversalTicker.parse("Okx_Spot_BTCUSDT"))
    await reader.fetch_my_trades(UniversalTicker.parse("Okx_Perp_BTCUSDT"))

    assert [c[1]["product"] for c in rest.calls] == ["SPOT", "SWAP"]


async def test_okx_swap_qty_is_converted_at_the_reader() -> None:
    """SWAP sizes are contracts; the symbol plane holds the multiplier."""
    rest = FakeOkxRest()
    rest.fills = [okx_fill("t1", bill_id="b1", fill_sz="4")]
    rest.orders = [okx_order("o1", sz="4")]
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)
    ticker = UniversalTicker.parse("Okx_Perp_BTCUSDT")

    fills = await reader.fetch_my_trades(ticker)
    orders = await reader.fetch_orders(ticker)

    assert fills.rows[0].qty == Decimal("0.0004")
    assert orders.rows[0].qty == Decimal("0.0004")


async def test_okx_refuses_a_swap_with_no_contract_size() -> None:
    """Guessing ``1`` would book contracts as base."""

    class NoSize(FakeSymbols):
        async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
            return None

    rest = FakeOkxRest()
    reader = OkxHistoryReader(symbols=NoSize(), rest=rest)

    with pytest.raises(ValueError, match="contract_size"):
        await reader.fetch_my_trades(UniversalTicker.parse("Okx_Perp_BTCUSDT"))


async def test_okx_drops_a_zero_size_row() -> None:
    rest = FakeOkxRest()
    rest.fills = [
        okx_fill("t1", bill_id="b1"),
        okx_fill("t2", bill_id="b2", fill_sz="0"),
    ]
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(UniversalTicker.parse("Okx_Spot_BTCUSDT"))

    assert [f.fill_id for f in page.rows] == ["t1"]


async def test_okx_stops_on_a_short_page() -> None:
    rest = FakeOkxRest()
    rest.fills = [okx_fill("t1", bill_id="b1")]
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)

    page = await reader.fetch_my_trades(
        UniversalTicker.parse("Okx_Spot_BTCUSDT"), limit=10
    )

    assert page.next_cursor is None


async def test_okx_refuses_another_venues_ticker() -> None:
    rest = FakeOkxRest()
    reader = OkxHistoryReader(symbols=FakeSymbols(), rest=rest)

    with pytest.raises(ValueError, match="Bybit"):
        await reader.fetch_my_trades(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))


async def test_every_venue_but_paper_has_a_reader() -> None:
    """Paper's book is invented in another process; there is nothing to re-read.

    A PEM secret throughout because Binance parses its key at construction —
    deliberately, so a malformed credential fails where it was configured. The
    HMAC venues never look at it.
    """
    _key, pem = keypair()
    factory = HistoryReaderFactory(FakeSymbols())
    row = type("Row", (), {"api_key": "k", "api_secret": pem, "passphrase": "p"})()

    for venue in (
        "Bybit",
        "Gate",
        "GateFutures",
        "BinanceFuture",
        "BinanceDelivery",
        "Okx",
    ):
        reader = await factory.create(venue, row)
        assert reader.venue == venue

    with pytest.raises(NoHistoryReaderError, match="Paper"):
        await factory.create("Paper", row)

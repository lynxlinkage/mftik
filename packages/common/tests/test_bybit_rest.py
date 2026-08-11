"""Bybit v5 REST — signing, the envelope, and the reads no socket serves."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest
from bybit_stub import API_KEY, API_SECRET
from mft.exchange.bybit.protocol import BybitRestError, sign_rest
from mft.exchange.bybit.rest import BybitPublicRest, BybitRest
from mft.exchange.tickers import UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Bybit_Spot_BTCUSDT")

BASE = "https://bybit.test"


class FakeApi:
    """An httpx transport standing in for Bybit's REST API."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        #: path → the ``result`` to answer with.
        self.results: dict[str, Any] = {}
        #: path → ``(retCode, retMsg)`` to refuse with instead.
        self.errors: dict[str, tuple[int, str]] = {}
        #: path → a queue of results, for the paginated calls.
        self.pages: dict[str, list[Any]] = {}

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=BASE, transport=self.transport())

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        failure = self.errors.get(path)
        if failure is not None:
            code, message = failure
            # A refusal is still an HTTP 200 on this venue.
            return httpx.Response(
                200, json={"retCode": code, "retMsg": message, "result": {}}
            )
        queue = self.pages.get(path)
        result = queue.pop(0) if queue else self.results.get(path, {})
        return httpx.Response(
            200, json={"retCode": 0, "retMsg": "OK", "result": result}
        )

    def request_for(self, path: str) -> httpx.Request:
        for request in self.requests:
            if request.url.path == path:
                return request
        raise AssertionError(f"no request for {path}")

    def requests_for(self, path: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path == path]


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


def _signed(api: FakeApi) -> BybitRest:
    return BybitRest(
        api_key=API_KEY, api_secret=API_SECRET, base_url=BASE, client=api.client()
    )


def _public(api: FakeApi) -> BybitPublicRest:
    return BybitPublicRest(base_url=BASE, client=api.client())


# --- signing ---------------------------------------------------------------


async def test_a_signed_get_signs_the_query_it_actually_sends(
    api: FakeApi,
) -> None:
    """The signature covers the literal query string, so the two cannot be
    built separately."""
    api.results["/v5/account/wallet-balance"] = {"list": []}
    await _signed(api).fetch_balances()

    request = api.request_for("/v5/account/wallet-balance")
    query = request.url.query.decode()
    assert query == "accountType=UNIFIED"
    assert request.headers["X-BAPI-SIGN"] == sign_rest(
        API_SECRET,
        api_key=API_KEY,
        timestamp=int(request.headers["X-BAPI-TIMESTAMP"]),
        recv_window=int(request.headers["X-BAPI-RECV-WINDOW"]),
        payload=query,
    )


async def test_a_signed_post_signs_the_body_byte_for_byte(api: FakeApi) -> None:
    """Bybit re-reads what it received to verify, so the body is serialized
    once and both signed and sent."""
    api.results["/v5/order/create"] = {"orderId": "ord-1", "orderLinkId": "c-1"}
    await _signed(api).place_order(
        category="spot",
        symbol="BTCUSDT",
        side="buy",
        order_type="limit",
        qty=Decimal("0.00100000"),
        price=Decimal("60000"),
        order_link_id="c-1",
    )

    request = api.request_for("/v5/order/create")
    body = request.content.decode()
    assert request.headers["X-BAPI-SIGN"] == sign_rest(
        API_SECRET,
        api_key=API_KEY,
        timestamp=int(request.headers["X-BAPI-TIMESTAMP"]),
        recv_window=int(request.headers["X-BAPI-RECV-WINDOW"]),
        payload=body,
    )
    sent = json.loads(body)
    assert sent["side"] == "Buy"
    # Numbers cross as strings, unpadded — a trailing-zero qty is refused.
    assert sent["qty"] == "0.001"


async def test_a_public_call_carries_no_credential(api: FakeApi) -> None:
    api.results["/v5/market/tickers"] = {"list": [{"symbol": "BTCUSDT",
                                                  "lastPrice": "60000"}]}
    await _public(api).fetch_ticker("spot", "BTCUSDT", ticker=TICKER)
    assert "X-BAPI-SIGN" not in api.request_for("/v5/market/tickers").headers


# --- the envelope ----------------------------------------------------------


async def test_a_refusal_arrives_as_a_200_and_still_raises(api: FakeApi) -> None:
    """Bybit reports rejections in the body; a caller reading only the status
    line would treat every one of them as a success."""
    api.errors["/v5/order/create"] = (110007, "ab not enough for new order")
    with pytest.raises(BybitRestError) as exc:
        await _signed(api).place_order(
            category="spot",
            symbol="BTCUSDT",
            side="buy",
            order_type="market",
            qty=Decimal("1"),
        )
    assert exc.value.code == 110007
    assert exc.value.status == 200
    assert "not enough" in str(exc.value)


async def test_a_non_json_body_is_a_transport_error_not_a_parse_crash() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="<html>bad gateway</html>")

    client = httpx.AsyncClient(base_url=BASE, transport=httpx.MockTransport(handler))
    rest = BybitPublicRest(base_url=BASE, client=client)
    with pytest.raises(BybitRestError) as exc:
        await rest.fetch_ticker("spot", "BTCUSDT", ticker=TICKER)
    assert exc.value.status == 502


# --- market data -----------------------------------------------------------


async def test_klines_come_back_oldest_first(api: FakeApi) -> None:
    """Bybit answers newest first, which is the opposite of what a series
    wants and of every other venue here."""
    api.results["/v5/market/kline"] = {
        "list": [
            ["1700000120000", "3", "3", "3", "3", "1", "3"],
            ["1700000060000", "2", "2", "2", "2", "1", "2"],
            ["1700000000000", "1", "1", "1", "1", "1", "1"],
        ]
    }
    klines = await _public(api).fetch_klines("spot", "BTCUSDT", "1", ticker=TICKER)
    assert [k.open_time for k in klines] == [
        1700000000.0,
        1700000060.0,
        1700000120.0,
    ]


async def test_instruments_follow_the_cursor_to_the_end(api: FakeApi) -> None:
    """A caller reading only the first page would silently see a fraction of
    the venue."""
    api.pages["/v5/market/instruments-info"] = [
        {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "status": "Trading",
                    "lotSizeFilter": {"basePrecision": "0.000001"},
                    "priceFilter": {"tickSize": "0.01"},
                },
                # Pre-launch, and Instrument has nowhere to say "not yet".
                {
                    "symbol": "SOONUSDT",
                    "baseCoin": "SOON",
                    "quoteCoin": "USDT",
                    "status": "PreLaunch",
                    "lotSizeFilter": {},
                    "priceFilter": {},
                },
            ],
            "nextPageCursor": "page-2",
        },
        {
            "list": [
                {
                    "symbol": "ETHUSDT",
                    "baseCoin": "ETH",
                    "quoteCoin": "USDT",
                    "status": "Trading",
                    "lotSizeFilter": {"basePrecision": "0.0001"},
                    "priceFilter": {"tickSize": "0.01"},
                }
            ],
            "nextPageCursor": "",
        },
    ]
    instruments = await _public(api).fetch_instruments("spot")
    assert [i.symbol for i in instruments] == ["BTCUSDT", "ETHUSDT"]
    assert "cursor=page-2" in api.requests_for("/v5/market/instruments-info")[
        1
    ].url.query.decode()


async def test_a_ticker_with_no_rows_is_an_error_not_an_empty_answer(
    api: FakeApi,
) -> None:
    api.results["/v5/market/tickers"] = {"list": []}
    with pytest.raises(BybitRestError, match="no ticker"):
        await _public(api).fetch_ticker("spot", "NOPEUSDT", ticker=TICKER)


# --- recon reads -----------------------------------------------------------


async def test_open_orders_need_a_filter_on_the_contract_books(
    api: FakeApi,
) -> None:
    """Bybit refuses an unfiltered query there; spot is happy without one."""
    api.results["/v5/order/realtime"] = {"list": []}
    rest = _signed(api)
    await rest.fetch_open_orders("spot")
    await rest.fetch_open_orders("linear")

    queries = [r.url.query.decode() for r in api.requests_for("/v5/order/realtime")]
    assert "settleCoin" not in queries[0]
    assert "settleCoin=USDT" in queries[1]


async def test_a_finished_order_is_found_in_history_not_the_open_list(
    api: FakeApi,
) -> None:
    """Bybit splits them, and an order that filled has left ``realtime``."""
    api.results["/v5/order/realtime"] = {"list": []}
    api.results["/v5/order/history"] = {
        "list": [
            {
                "symbol": "BTCUSDT",
                "orderId": "ord-1",
                "orderLinkId": "c-1",
                "side": "Buy",
                "orderStatus": "Filled",
                "qty": "1",
                "cumExecQty": "1",
                "cumExecValue": "60000",
            }
        ]
    }
    row = await _signed(api).fetch_order("spot", order_link_id="c-1")
    assert row is not None
    assert row.to_order(TICKER).avg_price == Decimal("60000")


async def test_an_order_neither_endpoint_has_is_none_not_an_error(
    api: FakeApi,
) -> None:
    """Which is an answer: the submit never landed."""
    api.results["/v5/order/realtime"] = {"list": []}
    api.results["/v5/order/history"] = {"list": []}
    assert await _signed(api).fetch_order("spot", order_link_id="c-1") is None


async def test_balances_flatten_the_accounts_outer_list(api: FakeApi) -> None:
    api.results["/v5/account/wallet-balance"] = {
        "list": [
            {
                "accountType": "UNIFIED",
                "coin": [
                    {"coin": "USDT", "walletBalance": "100",
                     "availableToWithdraw": "90", "locked": "10"},
                    {"coin": "BTC", "walletBalance": "1",
                     "availableToWithdraw": "1"},
                ],
            }
        ]
    }
    balances = await _signed(api).fetch_balances()
    assert {b.asset: b.free for b in balances} == {
        "USDT": Decimal("90"),
        "BTC": Decimal("1"),
    }


async def test_positions_drop_the_flat_rows_and_skip_spot(api: FakeApi) -> None:
    """Bybit keeps reporting a symbol after it is closed, and an OMS reading
    that as a position would hold a row that says nothing."""
    api.results["/v5/position/list"] = {
        "list": [
            {"symbol": "BTCUSDT", "side": "Sell", "size": "2"},
            {"symbol": "ETHUSDT", "side": "", "size": "0"},
        ]
    }
    rest = _signed(api)
    assert await rest.fetch_position_rows("spot") == []
    # Spot never even asks: it has no positions to report.
    assert not api.requests_for("/v5/position/list")

    rows = await rest.fetch_position_rows("linear")
    # Venue-native: only the connector can say which instrument a row is,
    # because only it holds the symbol plane.
    assert [(r.symbol, r.signed_size) for r in rows] == [
        ("BTCUSDT", Decimal("-2"))
    ]


async def test_leverage_row_keeps_a_flat_symbol(api: FakeApi) -> None:
    """Configured leverage is readable before the first contract is opened."""
    api.results["/v5/position/list"] = {
        "list": [
            {
                "symbol": "BTCUSDT",
                "side": "",
                "size": "0",
                "leverage": "15",
            }
        ]
    }
    rest = _signed(api)
    row = await rest.fetch_leverage_row("linear", "BTCUSDT")
    assert row.symbol == "BTCUSDT"
    assert row.size == Decimal("0")
    assert row.leverage == Decimal("15")
    request = api.request_for("/v5/position/list")
    assert "symbol=BTCUSDT" in str(request.url)
    assert "category=linear" in str(request.url)

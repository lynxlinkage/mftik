"""Gate futures REST — public reads, leverage, history paging."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
from mftik.exchange.gate.future.rest import GateFuturesPublicRest, GateFuturesRest
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")
CS = Decimal("0.0001")
API_KEY = "k"
API_SECRET = "s"


class FakeHttp:
    def __init__(self, routes: dict[str, Any]) -> None:
        self.requests: list[httpx.Request] = []
        self.routes = routes

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        payload = self.routes.get(request.url.path)
        if payload is None:
            return httpx.Response(404, json={"label": "NOT_FOUND", "message": request.url.path})
        return httpx.Response(200, json=payload)

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(self.handler),
            base_url="https://api.gateio.ws",
        )


async def test_public_book_and_klines_convert_size() -> None:
    http = FakeHttp(
        {
            "/api/v4/futures/usdt/order_book": {
                "current": 1_700_000_000_500,
                "bids": [["59999", "20"]],
                "asks": [["60001", "10"]],
            },
            "/api/v4/futures/usdt/candlesticks": [
                {
                    "t": 1_700_000_000,
                    "o": "1",
                    "h": "2",
                    "l": "0.5",
                    "c": "1.5",
                    "v": "100",
                    "sum": "150",
                }
            ],
            "/api/v4/futures/usdt/tickers": [
                {"contract": "BTC_USDT", "last": "60000"}
            ],
        }
    )
    rest = GateFuturesPublicRest(client=http.client())
    book = await rest.fetch_order_book(
        "BTC_USDT", ticker=TICKER, contract_size=CS, depth=10
    )
    assert book.bids[0].qty == Decimal("0.002")
    klines = await rest.fetch_klines(
        "BTC_USDT", "1m", ticker=TICKER, contract_size=CS, limit=10
    )
    assert klines[0].volume == Decimal("0.01")
    ticker = await rest.fetch_ticker("BTC_USDT", ticker=TICKER)
    assert ticker.last == Decimal("60000")


async def test_leverage_prefers_cross_when_isolated_is_zero() -> None:
    http = FakeHttp(
        {
            "/api/v4/futures/usdt/get_leverage/BTC_USDT": {
                "leverage": "0",
                "cross_leverage_limit": "20",
            }
        }
    )
    rest = GateFuturesRest(
        api_key=API_KEY, api_secret=API_SECRET, client=http.client()
    )
    assert await rest.fetch_leverage("BTC_USDT") == Decimal("20")


async def test_history_uses_offset_and_seconds() -> None:
    http = FakeHttp(
        {
            "/api/v4/futures/usdt/my_trades": [
                {
                    "id": 1,
                    "order_id": 9,
                    "contract": "BTC_USDT",
                    "size": "4",
                    "price": "60000",
                    "text": "t-a",
                    "create_time": 1_700_000_000,
                }
            ],
            "/api/v4/futures/usdt/orders": [
                {
                    "id": "9",
                    "contract": "BTC_USDT",
                    "size": "4",
                    "left": "0",
                    "price": "60000",
                    "status": "finished",
                    "finish_as": "filled",
                }
            ],
        }
    )
    rest = GateFuturesRest(
        api_key=API_KEY, api_secret=API_SECRET, client=http.client()
    )
    trades = await rest.fetch_my_trades(
        "BTC_USDT", offset=20, limit=50, since=1_700_000_000
    )
    assert trades[0].id == 1
    params = dict(http.requests[0].url.params)
    assert params["offset"] == "20"
    assert params["from"] == "1700000000"
    orders = await rest.fetch_orders("BTC_USDT", offset=0, since=1_700_000_000)
    assert orders[0].id == "9"

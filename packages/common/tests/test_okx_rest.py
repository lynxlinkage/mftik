"""OKX v5 REST — signing, the envelope, and the reads no socket serves."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import httpx
import pytest
from mftik.exchange.okx.protocol import OkxRestError, sign_rest
from mftik.exchange.okx.rest import OkxPublicRest, OkxRest
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("Okx_Spot_BTCUSDT")
BASE = "https://okx.test"
API_KEY = "key"
API_SECRET = "secret"
PASSPHRASE = "phrase"


class FakeApi:
    """An httpx transport standing in for OKX's REST API."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, Any] = {}
        self.errors: dict[str, tuple[int, str]] = {}

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
            return httpx.Response(
                200, json={"code": str(code), "msg": message, "data": []}
            )
        return httpx.Response(
            200,
            json={"code": "0", "msg": "", "data": self.results.get(path, [])},
        )

    def request_for(self, path: str) -> httpx.Request:
        for request in self.requests:
            if request.url.path == path:
                return request
        raise AssertionError(f"no request for {path}")


@pytest.fixture
def api() -> FakeApi:
    return FakeApi()


def _signed(api: FakeApi) -> OkxRest:
    return OkxRest(
        api_key=API_KEY,
        api_secret=API_SECRET,
        passphrase=PASSPHRASE,
        base_url=BASE,
        client=api.client(),
    )


def _public(api: FakeApi) -> OkxPublicRest:
    return OkxPublicRest(base_url=BASE, client=api.client())


async def test_a_signed_get_signs_the_query_it_actually_sends(api: FakeApi) -> None:
    api.results["/api/v5/account/balance"] = []
    await _signed(api).fetch_balances(ccy="BTC")

    request = api.request_for("/api/v5/account/balance")
    query = request.url.query.decode()
    assert query == "ccy=BTC"
    path = f"{request.url.path}?{query}"
    assert request.headers["OK-ACCESS-SIGN"] == sign_rest(
        API_SECRET,
        timestamp=request.headers["OK-ACCESS-TIMESTAMP"],
        method="GET",
        request_path=path,
    )
    assert request.headers["OK-ACCESS-PASSPHRASE"] == PASSPHRASE


async def test_a_signed_post_signs_the_body_byte_for_byte(api: FakeApi) -> None:
    api.results["/api/v5/trade/order"] = [
        {"ordId": "ord-1", "clOrdId": "c-1", "sCode": "0"}
    ]
    await _signed(api).place_order(
        {
            "instId": "BTC-USDT",
            "tdMode": "cash",
            "side": "buy",
            "ordType": "limit",
            "sz": Decimal("0.00100000"),
            "px": Decimal("60000"),
            "clOrdId": "c-1",
        }
    )

    request = api.request_for("/api/v5/trade/order")
    body = request.content.decode()
    assert request.headers["OK-ACCESS-SIGN"] == sign_rest(
        API_SECRET,
        timestamp=request.headers["OK-ACCESS-TIMESTAMP"],
        method="POST",
        request_path="/api/v5/trade/order",
        body=body,
    )
    sent = json.loads(body)
    assert sent["sz"] == "0.001"
    assert sent["side"] == "buy"


async def test_a_public_call_carries_no_credential(api: FakeApi) -> None:
    api.results["/api/v5/market/ticker"] = [
        {"instId": "BTC-USDT", "last": "60000"}
    ]
    await _public(api).fetch_ticker("BTC-USDT", ticker=TICKER)
    assert "OK-ACCESS-SIGN" not in api.request_for("/api/v5/market/ticker").headers


async def test_a_refusal_arrives_as_a_200_and_still_raises(api: FakeApi) -> None:
    api.errors["/api/v5/trade/order"] = (51008, "Insufficient balance")
    with pytest.raises(OkxRestError) as exc:
        await _signed(api).place_order(
            {"instId": "BTC-USDT", "side": "buy", "ordType": "market", "sz": "1"}
        )
    assert exc.value.code == 51008
    assert exc.value.status == 200
    assert "Insufficient" in str(exc.value)


async def test_an_s_code_refusal_is_a_failed_order_not_a_transport_error(
    api: FakeApi,
) -> None:
    """The envelope can say 0 while the row says the order was refused."""
    api.results["/api/v5/trade/order"] = [
        {"ordId": "", "clOrdId": "c-1", "sCode": "51008", "sMsg": "Insufficient"}
    ]
    with pytest.raises(OkxRestError) as exc:
        await _signed(api).place_order({"instId": "BTC-USDT", "sz": "1"})
    assert exc.value.code == 51008
    assert "Insufficient" in str(exc.value)


async def test_klines_come_back_oldest_first(api: FakeApi) -> None:
    api.results["/api/v5/market/candles"] = [
        ["1700000120000", "3", "3", "3", "3", "1", "3", "3", "1"],
        ["1700000060000", "2", "2", "2", "2", "1", "2", "2", "1"],
        ["1700000000000", "1", "1", "1", "1", "1", "1", "1", "1"],
    ]
    klines = await _public(api).fetch_klines("BTC-USDT", "1m", ticker=TICKER)
    assert [k.open_time for k in klines] == [1700000000.0, 1700000060.0, 1700000120.0]


async def test_instruments_drop_inverse_swaps_and_anything_not_live(
    api: FakeApi,
) -> None:
    api.results["/api/v5/public/instruments"] = [
        {
            "instId": "BTC-USDT-SWAP",
            "ctType": "linear",
            "state": "live",
            "ctValCcy": "BTC",
            "settleCcy": "USDT",
            "tickSz": "0.1",
            "lotSz": "1",
            "minSz": "1",
        },
        {
            "instId": "BTC-USD-SWAP",
            "ctType": "inverse",
            "state": "live",
            "ctValCcy": "USD",
            "settleCcy": "BTC",
        },
        {
            "instId": "ETH-USDT-SWAP",
            "ctType": "linear",
            "state": "suspend",
            "ctValCcy": "ETH",
            "settleCcy": "USDT",
        },
    ]
    rows = await _public(api).fetch_instruments("SWAP")
    assert [row.symbol for row in rows] == ["BTC-USDT-SWAP"]


async def test_flat_positions_are_dropped(api: FakeApi) -> None:
    api.results["/api/v5/account/positions"] = [
        {"instId": "BTC-USDT-SWAP", "pos": "1", "posSide": "net"},
        {"instId": "ETH-USDT-SWAP", "pos": "0", "posSide": "net"},
    ]
    rows = await _signed(api).fetch_position_rows()
    assert [row.inst_id for row in rows] == ["BTC-USDT-SWAP"]


async def test_balances_flatten_the_unified_wallet(api: FakeApi) -> None:
    api.results["/api/v5/account/balance"] = [
        {
            "details": [
                {"ccy": "USDT", "availEq": "10", "frozenBal": "2"},
                {"ccy": "BTC", "availEq": "0.1", "frozenBal": "0"},
            ]
        }
    ]
    balances = await _signed(api).fetch_balances()
    assert {b.asset: b.free for b in balances} == {
        "USDT": Decimal("10"),
        "BTC": Decimal("0.1"),
    }

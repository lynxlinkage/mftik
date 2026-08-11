"""The futures REST client — the reads no socket on this market answers.

The signed half is the part worth checking hardest: it is the only place in
the Binance adapter that signs anything *per call*, and the signature is over a
query string rather than a JSON body. The test below verifies it the way the
venue does — with the public key, over the string that was actually sent.
"""

from __future__ import annotations

import base64
from decimal import Decimal
from urllib.parse import parse_qsl, unquote

import httpx
import pytest
from binance_stub import keypair
from mft.exchange.binance.future.rest import (
    BinanceFuturePublicRest,
    BinanceFutureRest,
    BinanceFutureRestError,
)
from mft.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")
BASE = "https://fapi.test"

EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "100"},
            ],
        },
        {
            "symbol": "BTCUSDT_250926",
            "contractType": "CURRENT_QUARTER",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [],
        },
        {
            "symbol": "HALTUSDT",
            "contractType": "PERPETUAL",
            "status": "SETTLING",
            "baseAsset": "HALT",
            "quoteAsset": "USDT",
            "filters": [],
        },
    ]
}


class FakeApi:
    def __init__(self, response: httpx.Response | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self.results: dict[str, object] = {}
        self.response = response

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=BASE, transport=httpx.MockTransport(self._handle)
        )

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.response is not None:
            return self.response
        return httpx.Response(200, json=self.results.get(request.url.path, {}))


# --- public ----------------------------------------------------------------


async def test_only_perpetuals_are_listed() -> None:
    """Dated futures share the perpetual's canonical symbol and would collide."""
    api = FakeApi()
    api.results["/fapi/v1/exchangeInfo"] = EXCHANGE_INFO

    instruments = await BinanceFuturePublicRest(
        base_url=BASE, client=api.client()
    ).fetch_instruments()

    assert [i.symbol for i in instruments] == ["BTCUSDT"]
    assert instruments[0].min_notional == Decimal("100")


async def test_candles_are_asked_for_within_binances_ceiling() -> None:
    api = FakeApi()
    api.results["/fapi/v1/klines"] = []

    await BinanceFuturePublicRest(base_url=BASE, client=api.client()).fetch_klines(
        "BTCUSDT", "1m", ticker=TICKER, limit=99999
    )

    params = dict(api.requests[0].url.params)
    assert params["limit"] == "1500", "asking for more is a 400, not a truncation"


async def test_a_venue_refusal_keeps_its_code() -> None:
    """TD and MD normalize on the number, whichever transport answered."""
    api = FakeApi(
        response=httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})
    )
    rest = BinanceFuturePublicRest(base_url=BASE, client=api.client())

    with pytest.raises(BinanceFutureRestError) as caught:
        await rest.fetch_klines("NOPEUSDT", "1m", ticker=TICKER)

    assert caught.value.code == -1121
    assert caught.value.status == 400


# --- signed ----------------------------------------------------------------


async def test_open_orders_are_signed_the_way_the_venue_verifies_them() -> None:
    """Ed25519 over the query string that was sent, key named in the header.

    Verified against the public key here rather than pattern-matched, because
    a signature that is merely *present* is the failure this test exists to
    catch — the venue answers ``-1022`` and TD reads it as an auth failure on
    every recon.
    """
    private_key, pem = keypair()
    api = FakeApi()
    api.results["/fapi/v1/openOrders"] = []

    rest = BinanceFutureRest(
        api_key="fk",
        api_secret=pem,
        base_url=BASE,
        client=api.client(),
        recv_window=5000,
    )
    await rest.fetch_open_orders("BTCUSDT")

    request = api.requests[0]
    assert request.headers["X-MBX-APIKEY"] == "fk"

    query = request.url.query.decode()
    signed, _, signature = query.rpartition("&signature=")
    params = dict(parse_qsl(signed))
    assert params["symbol"] == "BTCUSDT"
    assert "timestamp" in params
    assert params["recvWindow"] == "5000"

    # Percent-encoded on the wire (base64 carries ``+``, ``/`` and ``=``) and
    # verified against the un-escaped string, which is what the venue rebuilds.
    private_key.public_key().verify(
        base64.b64decode(unquote(signature)), signed.encode("utf-8")
    )


async def test_the_whole_account_is_asked_for_when_no_symbol_is_known() -> None:
    _key, pem = keypair()
    api = FakeApi()
    api.results["/fapi/v1/openOrders"] = []

    rest = BinanceFutureRest(
        api_key="fk", api_secret=pem, base_url=BASE, client=api.client()
    )
    await rest.fetch_open_orders()

    assert "symbol" not in dict(parse_qsl(api.requests[0].url.query.decode()))


async def test_symbol_config_returns_leverage_for_a_flat_symbol() -> None:
    _key, pem = keypair()
    api = FakeApi()
    api.results["/fapi/v1/symbolConfig"] = [
        {
            "symbol": "BTCUSDT",
            "marginType": "CROSSED",
            "isAutoAddMargin": False,
            "leverage": 20,
            "maxNotionalValue": "1000000",
        }
    ]

    rest = BinanceFutureRest(
        api_key="fk", api_secret=pem, base_url=BASE, client=api.client()
    )
    rows = await rest.fetch_symbol_config("BTCUSDT")

    assert len(rows) == 1
    assert rows[0].symbol == "BTCUSDT"
    assert rows[0].leverage == Decimal("20")
    assert api.requests[0].url.path == "/fapi/v1/symbolConfig"
    assert dict(parse_qsl(api.requests[0].url.query.decode()))["symbol"] == (
        "BTCUSDT"
    )


def test_a_credential_that_is_not_ed25519_fails_where_it_was_configured() -> None:
    """Not on the first recon, hours later, as an auth error."""
    with pytest.raises(Exception, match="Ed25519|base64|PEM"):
        BinanceFutureRest(api_key="fk", api_secret="not-a-key", base_url=BASE)

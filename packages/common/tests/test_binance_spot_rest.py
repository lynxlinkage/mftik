"""The spot REST client — the account-history reads the backfill plane runs.

Two things are worth checking hardest. The signature, because this is the only
place in the spot adapter that signs per call rather than once per session, and
a signature that is merely *present* passes review while the venue answers
``-1022`` to every request. And the pagination arguments, because the whole
point of walking by id is that a resumed page cannot silently answer a
different question than the one asked.
"""

from __future__ import annotations

import base64
from decimal import Decimal
from urllib.parse import parse_qsl, unquote

import httpx
import pytest
from binance_stub import keypair
from mftik.exchange.binance.spot.rest import (
    MAX_ROWS,
    BinanceSpotRest,
    BinanceSpotRestError,
)
from mftik.exchange.models import Side
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")
BASE = "https://api.test"

MY_TRADES = [
    {
        "symbol": "BTCUSDT",
        "id": 28457,
        "orderId": 100234,
        "price": "4.00000100",
        "qty": "12.00000000",
        "quoteQty": "48.000012",
        "commission": "10.10000000",
        "commissionAsset": "BNB",
        "time": 1499865549590,
        "isBuyer": True,
        "isMaker": False,
        "isBestMatch": True,
    }
]

ALL_ORDERS = [
    {
        "symbol": "BTCUSDT",
        "orderId": 100234,
        "clientOrderId": "281474976710656001",
        "price": "4.00000100",
        "origQty": "12.00000000",
        "executedQty": "12.00000000",
        "cummulativeQuoteQty": "48.000012",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "time": 1499827319559,
        "updateTime": 1499865549590,
        "isWorking": True,
    }
]


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
        return httpx.Response(200, json=self.results.get(request.url.path, []))


def rest(api: FakeApi, **kwargs: object) -> BinanceSpotRest:
    _key, pem = keypair()
    return BinanceSpotRest(
        api_key="sk", api_secret=pem, base_url=BASE, client=api.client(), **kwargs
    )


def params_of(request: httpx.Request) -> dict[str, str]:
    signed, _, _signature = request.url.query.decode().rpartition("&signature=")
    return dict(parse_qsl(signed))


# --- signing ---------------------------------------------------------------


async def test_history_reads_are_signed_the_way_the_venue_verifies_them() -> None:
    """Ed25519 over the query string that was sent, key named in the header.

    Verified against the public key rather than pattern-matched: the failure
    this catches is a signature that exists and does not validate, which looks
    identical to a correct one from the caller's side.
    """
    private_key, pem = keypair()
    api = FakeApi()
    api.results["/api/v3/myTrades"] = MY_TRADES

    client = BinanceSpotRest(
        api_key="sk",
        api_secret=pem,
        base_url=BASE,
        client=api.client(),
        recv_window=5000,
    )
    await client.fetch_my_trades("BTCUSDT")

    request = api.requests[0]
    assert request.headers["X-MBX-APIKEY"] == "sk"

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


def test_a_credential_that_is_not_ed25519_fails_where_it_was_configured() -> None:
    """Not on the first backfill, hours later, as an auth error."""
    with pytest.raises(Exception, match="Ed25519|base64|PEM"):
        BinanceSpotRest(api_key="sk", api_secret="not-a-key", base_url=BASE)


async def test_a_venue_refusal_keeps_its_code() -> None:
    """One numbering across the venue, so callers normalize on the number."""
    api = FakeApi(
        response=httpx.Response(400, json={"code": -1121, "msg": "Invalid symbol."})
    )
    with pytest.raises(BinanceSpotRestError) as caught:
        await rest(api).fetch_my_trades("NOPEUSDT")

    assert caught.value.code == -1121
    assert caught.value.status == 400


# --- trades ----------------------------------------------------------------


async def test_trades_come_back_as_fills_the_platform_shares() -> None:
    api = FakeApi()
    api.results["/api/v3/myTrades"] = MY_TRADES

    rows = await rest(api).fetch_my_trades("BTCUSDT")

    assert api.requests[0].url.path == "/api/v3/myTrades"
    assert len(rows) == 1
    fill = rows[0].to_fill(TICKER)
    assert fill.fill_id == "28457"
    assert fill.order_id == "100234"
    assert fill.side is Side.BUY
    assert fill.qty == Decimal("12")
    assert fill.fee == Decimal("10.1")
    assert fill.fee_asset == "BNB"


async def test_a_walk_resumes_from_a_trade_id() -> None:
    api = FakeApi()
    await rest(api).fetch_my_trades("BTCUSDT", from_id=28457)

    params = params_of(api.requests[0])
    assert params["fromId"] == "28457"
    assert "startTime" not in params, "a resumed page is addressed by id alone"


async def test_a_first_page_may_be_addressed_by_time() -> None:
    """There is no id to resume from until the first page has been read."""
    api = FakeApi()
    await rest(api).fetch_my_trades("BTCUSDT", start_time=1499865549590)

    params = params_of(api.requests[0])
    assert params["startTime"] == "1499865549590"
    assert "fromId" not in params


async def test_asking_by_id_and_by_time_at_once_is_refused() -> None:
    """Binance would ignore the range and answer a different question."""
    api = FakeApi()
    with pytest.raises(ValueError, match="not both"):
        await rest(api).fetch_my_trades(
            "BTCUSDT", from_id=28457, start_time=1499865549590
        )
    assert not api.requests, "refused before anything left the process"


async def test_a_page_is_asked_for_within_binances_ceiling() -> None:
    api = FakeApi()
    await rest(api).fetch_my_trades("BTCUSDT", limit=99999)

    assert params_of(api.requests[0])["limit"] == str(MAX_ROWS)


async def test_unset_pagination_arguments_are_not_sent_at_all() -> None:
    """An empty ``fromId`` is not the same request as no ``fromId``."""
    api = FakeApi()
    await rest(api).fetch_my_trades("BTCUSDT")

    params = params_of(api.requests[0])
    assert set(params) == {"symbol", "limit", "timestamp"}


# --- orders ----------------------------------------------------------------


async def test_orders_come_back_with_the_client_order_id_trades_lack() -> None:
    """The join that makes a backfilled fill attributable to a session."""
    api = FakeApi()
    api.results["/api/v3/allOrders"] = ALL_ORDERS

    rows = await rest(api).fetch_orders("BTCUSDT")

    assert api.requests[0].url.path == "/api/v3/allOrders"
    order = rows[0].to_order(TICKER)
    assert order.order_id == "100234"
    assert order.client_order_id == "281474976710656001"
    assert order.filled_qty == Decimal("12")


async def test_an_order_walk_resumes_from_an_order_id() -> None:
    api = FakeApi()
    await rest(api).fetch_orders("BTCUSDT", from_order_id=100234)

    params = params_of(api.requests[0])
    assert params["orderId"] == "100234"
    assert "startTime" not in params


async def test_asking_orders_by_id_and_by_time_at_once_is_refused() -> None:
    api = FakeApi()
    with pytest.raises(ValueError, match="not both"):
        await rest(api).fetch_orders(
            "BTCUSDT", from_order_id=100234, start_time=1499827319559
        )
    assert not api.requests


async def test_an_empty_answer_is_an_empty_list_not_a_failure() -> None:
    """The end of a walk, and the normal case for a symbol never traded."""
    api = FakeApi(response=httpx.Response(200, json=[]))
    client = rest(api)

    assert await client.fetch_my_trades("BTCUSDT") == []
    assert await client.fetch_orders("BTCUSDT") == []

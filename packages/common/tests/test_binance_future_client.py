"""The futures WebSocket API and the listen-key socket beside it.

Two connections, two very different jobs, and the seam between them — the
listen key — is where this market's account feed can go quiet without anything
raising. So the tests below are mostly about that seam: who issues the key, what
carries it, what happens when it dies.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from binance_future_stub import (
    API_KEY,
    LISTEN_KEY,
    FakeBinanceFutureApi,
    FakeBinanceFutureUser,
)
from mftik.exchange.binance.future import methods as m
from mftik.exchange.binance.future.client import BinanceFutureWsApi
from mftik.exchange.binance.future.protocol import BinanceWsError
from mftik.exchange.binance.future.user import BinanceFutureUserStream
from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")


def _api(stub: FakeBinanceFutureApi, pem: str | None = None) -> BinanceFutureWsApi:
    return BinanceFutureWsApi(
        api_key=API_KEY if pem else None,
        api_secret=pem,
        url=stub.url,  # type: ignore[attr-defined]
        keepalive=0,
        retry_backoff=0.01,
    )


# --- session ---------------------------------------------------------------


async def test_connecting_with_credentials_logs_the_session_on(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    """The stub verifies the Ed25519 signature, so this covers signing too."""
    _key, pem = binance_key
    async with _api(future_api, pem) as api:
        assert api.logged_on
    assert future_api.logons == 1


async def test_a_public_client_never_logs_on(
    future_api: FakeBinanceFutureApi,
) -> None:
    """MD holds no credentials, and the market data does not need any."""
    async with _api(future_api) as api:
        assert not api.authenticated
        assert not api.logged_on
    assert future_api.logons == 0
    assert not future_api.calls(m.SESSION_LOGON)


async def test_a_reconnect_logs_on_again_before_anything_else(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    """A lost socket is a lost authentication; the order path must not race it."""
    _key, pem = binance_key
    async with _api(future_api, pem) as api:
        future_api.drop_next = True
        await api.call(m.TICKER_PRICE, {"symbol": "BTCUSDT"})
        await asyncio.sleep(0.2)
        assert future_api.logons == 2
        assert api.logged_on


async def test_trading_without_credentials_is_refused_locally(
    future_api: FakeBinanceFutureApi,
) -> None:
    async with _api(future_api) as api:
        with pytest.raises(ExchangeError, match="trading call"):
            await api.place_order(symbol="BTCUSDT", side="buy", quantity="1")


# --- call shapes -----------------------------------------------------------


async def test_a_signed_call_still_stamps_itself_on_a_logged_on_session(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    """The logon replaces the credential on each call, not the clock.

    The stub answers ``-1102`` for a signed method with no ``timestamp``, which
    is what Binance does — so this passing is the assertion.
    """
    _key, pem = binance_key
    async with _api(future_api, pem) as api:
        await api.fetch_balances()
    params = future_api.call(m.ACCOUNT_BALANCE)["params"]
    assert "timestamp" in params
    assert "apiKey" not in params, "the session says who we are"
    assert "signature" not in params


async def test_a_listen_key_call_carries_neither_clock_nor_signature(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    """``userDataStream.*`` is Binance's key-only class; a timestamp is ``-1101``."""
    _key, pem = binance_key
    async with _api(future_api, pem) as api:
        key = await api.start_user_stream()
    assert key == LISTEN_KEY
    assert future_api.call(m.USER_DATA_STREAM_START).get("params") is None


async def test_a_listen_key_call_off_a_cold_session_names_the_key(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    """Nothing in this adapter sends one — but the API allows it, and the rule
    that decides is the same one the order path uses."""
    _key, pem = binance_key
    api = _api(future_api, pem)
    await api.connect()
    try:
        api._logged_on = False  # pretend the logon has not happened yet
        await api.ping_user_stream()
    finally:
        await api.close()
    assert future_api.call(m.USER_DATA_STREAM_PING)["params"] == {"apiKey": API_KEY}


async def test_an_order_goes_out_in_binances_own_vocabulary(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    _key, pem = binance_key
    future_api.results[m.ORDER_PLACE] = {
        "orderId": 1,
        "symbol": "BTCUSDT",
        "status": "NEW",
        "clientOrderId": "c-1",
        "origQty": "1",
        "price": "40000",
    }
    async with _api(future_api, pem) as api:
        ack = await api.place_order(
            symbol="BTCUSDT",
            side="buy",
            type="limit",
            quantity=Decimal("1.50"),
            price=Decimal("40000.10"),
            time_in_force="gtx",
            client_order_id="c-1",
            reduce_only=True,
        )
    params = future_api.call(m.ORDER_PLACE)["params"]
    assert params["side"] == "BUY"
    assert params["type"] == "LIMIT"
    assert params["timeInForce"] == "GTX"
    assert params["newClientOrderId"] == "c-1"
    assert params["reduceOnly"] is True
    # Trailing zeros are the Decimal's scale, and Binance answers -1111 for a
    # size written to more places than the step allows.
    assert params["quantity"] == "1.5"
    assert params["price"] == "40000.1"
    assert ack.order_id == 1


async def test_cancelling_needs_one_of_the_two_ids(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _api(future_api, pem) as api:
        with pytest.raises(ExchangeError, match="orderId or origClientOrderId"):
            await api.cancel_order("BTCUSDT")


async def test_a_venue_error_comes_back_with_its_code(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    """TD normalizes on the number, so it must survive the round trip typed."""
    _key, pem = binance_key
    future_api.errors[m.ORDER_PLACE] = {
        "code": -5022,
        "msg": "Due to the order could not be executed as maker, the Post Only "
        "order will be rejected.",
    }
    async with _api(future_api, pem) as api:
        with pytest.raises(BinanceWsError) as caught:
            await api.place_order(symbol="BTCUSDT", side="buy", quantity="1")
    assert caught.value.code == -5022


# --- market data -----------------------------------------------------------


async def test_a_ticker_costs_two_calls_because_futures_splits_it(
    future_api: FakeBinanceFutureApi,
) -> None:
    """No ``ticker.24hr`` here, and ``ticker.book`` has no last price."""
    future_api.results[m.TICKER_BOOK] = {
        "symbol": "BTCUSDT",
        "bidPrice": "39999",
        "bidQty": "1",
        "askPrice": "40001",
        "askQty": "2",
        "time": 1589437530011,
    }
    future_api.results[m.TICKER_PRICE] = {
        "symbol": "BTCUSDT",
        "price": "40000",
        "time": 1589437530011,
    }
    async with _api(future_api) as api:
        ticker = await api.fetch_ticker("BTCUSDT", ticker=TICKER)
    assert (ticker.bid, ticker.last, ticker.ask) == (
        Decimal("39999"),
        Decimal("40000"),
        Decimal("40001"),
    )
    assert ticker.universal_ticker == "BinanceUM_Perp_BTCUSDT"


async def test_a_ticker_answered_as_an_array_reads_the_same(
    future_api: FakeBinanceFutureApi,
) -> None:
    """Binance answers an array for some symbols and an object for others."""
    future_api.results[m.TICKER_PRICE] = [
        {"symbol": "BTCUSDT", "price": "40000", "time": 1}
    ]
    async with _api(future_api) as api:
        price = await api.fetch_price("BTCUSDT")
    assert price.price == Decimal("40000")


async def test_positions_come_back_signed(
    future_api: FakeBinanceFutureApi, binance_key
) -> None:
    _key, pem = binance_key
    future_api.results[m.ACCOUNT_POSITION] = [
        {"symbol": "BTCUSDT", "positionAmt": "-0.5", "entryPrice": "40000"}
    ]
    async with _api(future_api, pem) as api:
        rows = await api.fetch_positions()
    assert rows[0].position_amount == Decimal("-0.5")


# --- the listen-key socket -------------------------------------------------


def _user(
    stub: FakeBinanceFutureUser,
    *,
    keys: list[str] | None = None,
    pings: list[int] | None = None,
    keepalive_seconds: float = 60.0,
) -> BinanceFutureUserStream:
    issued = list(keys or [LISTEN_KEY, "listen-key-2"])

    async def start_key() -> str:
        return issued.pop(0)

    async def ping_key() -> None:
        if pings is not None:
            pings.append(1)

    return BinanceFutureUserStream(
        start_key=start_key,
        ping_key=ping_key,
        base_url=stub.url,  # type: ignore[attr-defined]
        keepalive_seconds=keepalive_seconds,
        keepalive=0,
        retry_backoff=0.01,
    )


async def test_the_user_socket_is_opened_on_the_key_it_was_issued(
    future_user: FakeBinanceFutureUser,
) -> None:
    stream = _user(future_user)
    await stream.connect()
    try:
        await asyncio.sleep(0.05)
        assert future_user.listen_keys == [LISTEN_KEY]
        assert stream.listen_key == LISTEN_KEY
    finally:
        await stream.close()


async def test_account_events_route_to_their_own_views(
    future_user: FakeBinanceFutureUser,
) -> None:
    """One socket, two typed feeds — and neither sees the other's events."""
    stream = _user(future_user)
    await stream.connect()
    try:
        orders = await stream.subscribe_order_updates()
        accounts = await stream.subscribe_account_updates()
        order_pump = asyncio.ensure_future(anext(orders))
        account_pump = asyncio.ensure_future(anext(accounts))
        await asyncio.sleep(0.05)

        await future_user.push(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1,
                "T": 1,
                "o": {"s": "BTCUSDT", "S": "BUY", "i": 7, "X": "NEW", "x": "NEW"},
            }
        )
        update = await asyncio.wait_for(order_pump, timeout=2.0)
        assert update.o.order_id == 7
        assert not account_pump.done()

        await future_user.push(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 2,
                "T": 2,
                "a": {"m": "FUNDING_FEE", "B": [{"a": "USDT", "wb": "5", "cw": "5"}]},
            }
        )
        account = await asyncio.wait_for(account_pump, timeout=2.0)
        assert account.reason == "FUNDING_FEE"
    finally:
        await stream.close()


async def test_an_expired_listen_key_reopens_the_socket_with_a_new_one(
    future_user: FakeBinanceFutureUser,
) -> None:
    """Binance announces the expiry and leaves the connection up.

    Nothing raises, nothing disconnects, and the feed simply stops carrying
    events — so the announcement has to be acted on.
    """
    stream = _user(future_user, keys=[LISTEN_KEY, "listen-key-2"])
    await stream.connect()
    try:
        await asyncio.sleep(0.05)
        await future_user.push(
            {"e": "listenKeyExpired", "E": 1, "listenKey": LISTEN_KEY}
        )
        for _ in range(50):
            await asyncio.sleep(0.05)
            if future_user.connections > 1:
                break
        assert future_user.listen_keys == [LISTEN_KEY, "listen-key-2"]
        assert stream.listen_key == "listen-key-2"
    finally:
        await stream.close()


async def test_the_key_is_renewed_for_as_long_as_the_socket_is_up(
    future_user: FakeBinanceFutureUser,
) -> None:
    """Without the ping the key dies at 60 minutes and the feed goes quiet."""
    pings: list[int] = []
    stream = _user(future_user, pings=pings, keepalive_seconds=0.05)
    await stream.connect()
    try:
        await asyncio.sleep(0.2)
        assert pings, "the listen key was never renewed"
    finally:
        await stream.close()
    before = len(pings)
    await asyncio.sleep(0.15)
    assert len(pings) == before, "the renewal loop outlived the socket"

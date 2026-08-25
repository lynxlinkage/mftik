"""Binance spot sockets, driven against local stand-ins for both endpoints.

The stand-ins speak the real envelopes and — for ``session.logon`` — actually
verify the Ed25519 signature, so the client is exercised end-to-end over a real
socket without touching the venue.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_stub import API_KEY, FakeBinanceApi, FakeBinanceStream
from mftik.exchange.binance.spot import methods as m
from mftik.exchange.binance.spot import streams as st
from mftik.exchange.binance.spot.client import BinanceSpotWsApi
from mftik.exchange.binance.spot.feed import BinanceSpotStream
from mftik.exchange.binance.spot.protocol import BinanceWsError
from mftik.exchange.errors import ExchangeError, ExchangeNotConnectedError
from mftik.exchange.tickers import UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")

AGG_TRADE = {
    "e": "aggTrade",
    "E": 1672515782136,
    "s": "BTCUSDT",
    "a": 12345,
    "p": "40000",
    "q": "0.5",
    "T": 1672515782136,
    "m": True,
}

EXECUTION_REPORT = {
    "e": "executionReport",
    "E": 1499405658658,
    "s": "BTCUSDT",
    "c": "my-order-1",
    "S": "BUY",
    "o": "LIMIT",
    "f": "GTC",
    "q": "0.001",
    "p": "60000",
    "x": "NEW",
    "X": "NEW",
    "i": 4293153,
    "l": "0",
    "z": "0",
    "L": "0",
    "n": "0",
    "N": None,
    "T": 1499405658657,
    "t": -1,
    "Z": "0",
}


def _api(stub: FakeBinanceApi, **kwargs: Any) -> BinanceSpotWsApi:
    return BinanceSpotWsApi(url=stub.url, keepalive=0, **kwargs)  # type: ignore[attr-defined]


def _feed(stub: FakeBinanceStream, **kwargs: Any) -> BinanceSpotStream:
    return BinanceSpotStream(url=stub.url, keepalive=0, **kwargs)  # type: ignore[attr-defined]


# --- market streams --------------------------------------------------------


async def test_subscribe_names_the_streams_lowercase(
    binance_stream: FakeBinanceStream,
) -> None:
    """Binance rejects an uppercase stream name outright."""
    async with _feed(binance_stream) as feed:
        await feed.subscribe_agg_trades("BTCUSDT", "ETHUSDT")

    frame = binance_stream.frames_for(st.SUBSCRIBE)[0]
    assert frame["params"] == ["btcusdt@aggTrade", "ethusdt@aggTrade"]


async def test_pushes_are_routed_by_stream_name(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        books = await feed.subscribe_order_book("BTCUSDT", levels=5)

        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        await binance_stream.push(
            "btcusdt@depth5@100ms",
            {"lastUpdateId": 1, "bids": [["39999", "1"]], "asks": [["40001", "2"]]},
        )

        trade = await asyncio.wait_for(anext(trades), timeout=2.0)
        symbol, book = await asyncio.wait_for(anext(books), timeout=2.0)

    assert trade.s == "BTCUSDT"
    assert trade.to_trade(TICKER).price == Decimal("40000")
    # Partial depth names no instrument, so the stream name supplies it.
    assert symbol == "BTCUSDT"
    assert book.to_order_book(symbol).bids[0].price == Decimal("39999")


async def test_a_stream_only_receives_what_it_subscribed_to(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        await binance_stream.push("ethusdt@aggTrade", {**AGG_TRADE, "s": "ETHUSDT"})
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)

        trade = await asyncio.wait_for(anext(trades), timeout=2.0)

    assert trade.s == "BTCUSDT", "the ETH push must not have landed here"


async def test_two_consumers_share_one_venue_subscription(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_agg_trades("BTCUSDT"),
            feed.subscribe_agg_trades("BTCUSDT"),
        )
        assert len(binance_stream.frames_for(st.SUBSCRIBE)) == 1
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        assert (await asyncio.wait_for(anext(first), timeout=2.0)).s == "BTCUSDT"
        assert (await asyncio.wait_for(anext(second), timeout=2.0)).s == "BTCUSDT"


async def test_reconnect_resubscribes_a_shared_stream_once(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream, retry_backoff=0.01) as feed:
        first = await feed.subscribe_agg_trades("BTCUSDT")
        second = await feed.subscribe_agg_trades("BTCUSDT")
        await binance_stream.drop()
        for _ in range(200):
            if (
                binance_stream.connections > 1
                and len(binance_stream.frames_for(st.SUBSCRIBE)) >= 2
            ):
                break
            await asyncio.sleep(0.01)
        assert len(binance_stream.frames_for(st.SUBSCRIBE)) == 2
        replayed = binance_stream.frames_for(st.SUBSCRIBE)[-1]
        assert replayed["params"] == ["btcusdt@aggTrade"]
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        assert (await asyncio.wait_for(anext(first), timeout=2.0)).s == "BTCUSDT"
        assert (await asyncio.wait_for(anext(second), timeout=2.0)).s == "BTCUSDT"


async def test_unsubscribe_raises_when_a_co_reader_still_holds_the_name(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_agg_trades("BTCUSDT"),
            feed.subscribe_agg_trades("BTCUSDT"),
        )
        with pytest.raises(ValueError, match="2 readers"):
            await feed.unsubscribe("btcusdt@aggTrade")
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        assert (await asyncio.wait_for(anext(first), timeout=2.0)).s == "BTCUSDT"
        assert (await asyncio.wait_for(anext(second), timeout=2.0)).s == "BTCUSDT"


async def test_unsubscribe_in_the_reconnect_gap_closes_locally(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream, retry_backoff=2.0) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        await binance_stream.drop()
        await asyncio.sleep(0.05)
        with pytest.raises(Exception):
            await feed.unsubscribe("btcusdt@aggTrade")
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(trades), timeout=2.0)
        for _ in range(80):
            if binance_stream.connections > 1:
                break
            await asyncio.sleep(0.05)
        replayed = [
            name
            for frame in binance_stream.frames_for(st.SUBSCRIBE)
            for name in (frame.get("params") or [])
        ]
        assert replayed.count("btcusdt@aggTrade") == 1


async def test_a_failed_unsubscribe_keeps_the_name_held(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        binance_stream.errors[st.UNSUBSCRIBE] = {"code": 1, "msg": "nope"}
        with pytest.raises(BinanceWsError, match="nope"):
            await feed.unsubscribe("btcusdt@aggTrade")
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(trades), timeout=2.0)
        assert "btcusdt@aggTrade" in feed._ledger.held()
        again = await feed.subscribe_agg_trades("BTCUSDT")
        assert len(binance_stream.frames_for(st.SUBSCRIBE)) == 1
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        assert (await asyncio.wait_for(anext(again), timeout=2.0)).s == "BTCUSDT"


async def test_unsubscribe_closes_the_streams_reading_it(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        await feed.unsubscribe("btcusdt@aggTrade")

        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(trades), timeout=2.0)

    assert binance_stream.frames_for(st.UNSUBSCRIBE)[0]["params"] == [
        "btcusdt@aggTrade"
    ]
    assert binance_stream.subscribed == set()


async def test_reconnect_replays_every_live_subscription(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _feed(binance_stream, retry_backoff=0.01) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        await asyncio.wait_for(anext(trades), timeout=2.0)
        await binance_stream.drop()

        for _ in range(200):
            if (
                binance_stream.connections > 1
                and len(binance_stream.frames_for(st.SUBSCRIBE)) > 1
            ):
                break
            await asyncio.sleep(0.01)

        assert binance_stream.connections > 1
        replayed = binance_stream.frames_for(st.SUBSCRIBE)[-1]
        assert replayed["params"] == ["btcusdt@aggTrade"]

        # And the stream the caller is holding keeps working.
        await binance_stream.push("btcusdt@aggTrade", AGG_TRADE)
        assert (await asyncio.wait_for(anext(trades), timeout=2.0)).s == "BTCUSDT"


async def test_reconnect_fires_the_callback_so_the_owner_can_rebuild(
    binance_stream: FakeBinanceStream,
) -> None:
    fired = asyncio.Event()
    async with _feed(binance_stream, retry_backoff=0.01) as feed:
        feed.on_reconnect(fired.set)
        await feed.subscribe_agg_trades("BTCUSDT")
        await binance_stream.drop()
        await asyncio.wait_for(fired.wait(), timeout=5.0)


async def test_a_subscribe_error_raises_with_the_venues_words(
    binance_stream: FakeBinanceStream,
) -> None:
    binance_stream.errors[st.SUBSCRIBE] = {
        "code": 2,
        "msg": "Invalid request: invalid stream",
    }
    async with _feed(binance_stream) as feed:
        with pytest.raises(BinanceWsError) as exc:
            await feed.subscribe_agg_trades("NOPE")
    assert exc.value.code == 2
    assert "invalid stream" in str(exc.value)


async def test_streams_are_refused_before_connect(
    binance_stream: FakeBinanceStream,
) -> None:
    feed = _feed(binance_stream)
    with pytest.raises(ExchangeNotConnectedError):
        await feed.subscribe_agg_trades("BTCUSDT")


# --- WebSocket API: session ------------------------------------------------


async def test_connect_with_credentials_logs_the_session_on(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        assert api.authenticated
        assert api.logged_on

    # The stub verifies the signature itself; reaching here means it passed.
    assert binance_api.logons == 1
    params = binance_api.call(m.SESSION_LOGON)["params"]
    assert params["apiKey"] == API_KEY
    assert "signature" in params


async def test_a_bad_signature_fails_the_connect_rather_than_the_first_order(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    from binance_stub import keypair

    _wrong, wrong_pem = keypair()  # a key the stub does not know
    api = _api(binance_api, api_key=API_KEY, api_secret=wrong_pem)
    with pytest.raises(BinanceWsError) as exc:
        await api.connect()
    assert exc.value.code == -1022
    assert not api.connected


async def test_without_credentials_market_data_still_works(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.results[m.DEPTH] = {
        "lastUpdateId": 1,
        "bids": [["39999", "1"]],
        "asks": [["40001", "2"]],
    }
    async with _api(binance_api) as api:
        assert not api.authenticated
        assert not api.logged_on
        book = await api.fetch_order_book("BTCUSDT", ticker=TICKER, depth=5)

    assert book.symbol == "BTCUSDT"
    assert book.asks[0].qty == Decimal("2")
    assert binance_api.logons == 0


async def test_a_trading_call_without_credentials_is_refused_locally(
    binance_api: FakeBinanceApi,
) -> None:
    async with _api(binance_api) as api:
        with pytest.raises(ExchangeError, match="api_key and api_secret"):
            await api.place_order(symbol="BTCUSDT", side="buy", quantity="1")
    assert binance_api.calls(m.ORDER_PLACE) == []


async def test_a_logged_on_call_carries_no_key_and_no_signature(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """The session authenticates it; per-call crypto would be wasted work."""
    _key, pem = binance_key
    binance_api.results[m.ORDER_PLACE] = {
        "symbol": "BTCUSDT",
        "orderId": 1,
        "clientOrderId": "c-1",
        "status": "NEW",
        "origQty": "0.001",
        "executedQty": "0",
        "cummulativeQuoteQty": "0",
        "price": "60000",
        "side": "BUY",
        "type": "LIMIT",
        "transactTime": 1660801715639,
    }
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        ack = await api.place_order(
            symbol="BTCUSDT",
            side="buy",
            quantity=Decimal("0.001"),
            price=Decimal("60000"),
            time_in_force="GTC",
            client_order_id="c-1",
        )

    params = binance_api.call(m.ORDER_PLACE)["params"]
    assert "apiKey" not in params
    assert "signature" not in params
    # But the clock is still the endpoint's own mandatory parameter: logon
    # replaces the credential on each call, not the timestamp. Binance answers
    # -1102 without it, however the connection was authenticated.
    assert "timestamp" in params
    assert params["side"] == "BUY"
    assert params["type"] == "LIMIT"
    assert params["quantity"] == "0.001"
    assert params["newClientOrderId"] == "c-1"
    assert ack.order_id == 1


async def test_every_signed_method_carries_a_timestamp_on_a_logged_on_session(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """The recon reads an attach makes are signed methods too.

    ``account.status`` and ``openOrders.status`` are the first calls a TD
    session makes, and both were answered ``-1102`` when the timestamp was
    left to the signing path — which only runs on a socket that has *not*
    logged on. Attach failed before a single order was placed.
    """
    _key, pem = binance_key
    binance_api.results[m.ACCOUNT_STATUS] = {"balances": []}
    binance_api.results[m.OPEN_ORDERS_STATUS] = []
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        assert api.logged_on
        await api.fetch_account()
        await api.fetch_open_orders()

    for method in (m.ACCOUNT_STATUS, m.OPEN_ORDERS_STATUS):
        assert "timestamp" in binance_api.call(method)["params"], method


async def test_a_session_only_method_is_not_given_a_timestamp(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """``userDataStream.subscribe`` takes no params; one would be rejected."""
    _key, pem = binance_key
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        await api.subscribe_execution_reports()

    assert "params" not in binance_api.call(m.USER_DATA_STREAM_SUBSCRIBE)


async def test_recv_window_rides_along_when_it_is_configured(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.results[m.ACCOUNT_STATUS] = {"balances": []}
    async with _api(
        binance_api, api_key=API_KEY, api_secret=pem, recv_window=20000
    ) as api:
        await api.fetch_account()

    assert binance_api.call(m.ACCOUNT_STATUS)["params"]["recvWindow"] == 20000


# --- WebSocket API: correlation and errors ---------------------------------


async def test_concurrent_calls_correlate_by_id_not_by_arrival(
    binance_api: FakeBinanceApi,
) -> None:
    """Replies come back reversed; each caller must still get its own."""
    binance_api.hold_replies = 2
    async with _api(binance_api) as api:
        first, second = await asyncio.gather(
            api.call(m.TIME, {"tag": "first"}),
            api.call(m.TIME, {"tag": "second"}),
        )
    assert first == {} and second == {}
    tags = [c["params"]["tag"] for c in binance_api.calls(m.TIME)]
    assert sorted(tags) == ["first", "second"]


async def test_a_venue_error_raises_with_its_own_code(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    binance_api.errors[m.ORDER_PLACE] = {
        "code": -2010,
        "msg": "Account has insufficient balance for requested action.",
    }
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        with pytest.raises(BinanceWsError) as exc:
            await api.place_order(symbol="BTCUSDT", side="buy", quantity="1")
    assert exc.value.code == -2010
    assert exc.value.status == 400


async def test_a_silent_venue_times_out_rather_than_hanging(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.hold_replies = 99  # never released
    async with _api(binance_api, ack_timeout=0.2) as api:
        with pytest.raises(BinanceWsError, match="no reply within"):
            await api.call(m.TIME)


async def test_cancel_needs_one_of_the_two_ids(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        with pytest.raises(ExchangeError, match="orderId or origClientOrderId"):
            await api.cancel_order("BTCUSDT")


# --- WebSocket API: user data stream ---------------------------------------


async def test_the_three_views_share_one_subscription(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        reports = await api.subscribe_execution_reports()
        positions = await api.subscribe_account_positions()

        assert binance_api.user_subscribes == 1, "one subscription, three views"

        await binance_api.push_event(EXECUTION_REPORT)
        await binance_api.push_event(
            {
                "e": "outboundAccountPosition",
                "E": 1,
                "u": 1,
                "B": [{"a": "USDT", "f": "100", "l": "0"}],
            }
        )

        report = await asyncio.wait_for(anext(reports), timeout=2.0)
        position = await asyncio.wait_for(anext(positions), timeout=2.0)

    assert report.s == "BTCUSDT"
    assert position.to_balances()[0].asset == "USDT"


async def test_events_are_routed_by_type_not_broadcast(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _api(binance_api, api_key=API_KEY, api_secret=pem) as api:
        reports = await api.subscribe_execution_reports()
        await binance_api.push_event(
            {"e": "balanceUpdate", "E": 1, "a": "BTC", "d": "1"}
        )
        await binance_api.push_event(EXECUTION_REPORT)

        report = await asyncio.wait_for(anext(reports), timeout=2.0)

    assert report.e == "executionReport", "the balanceUpdate must not have landed"


async def test_the_user_stream_needs_a_logged_on_session(
    binance_api: FakeBinanceApi,
) -> None:
    async with _api(binance_api) as api:
        with pytest.raises(ExchangeError, match="api_key and api_secret"):
            await api.subscribe_execution_reports()


async def test_reconnect_logs_on_again_and_resubscribes_user_data(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    """A lost socket is a lost authentication as well as a lost subscription."""
    _key, pem = binance_key
    async with _api(
        binance_api, api_key=API_KEY, api_secret=pem, retry_backoff=0.01
    ) as api:
        reports = await api.subscribe_execution_reports()
        await binance_api.push_event(EXECUTION_REPORT)
        await asyncio.wait_for(anext(reports), timeout=2.0)
        await binance_api.drop()

        for _ in range(300):
            if binance_api.logons > 1 and binance_api.user_subscribes > 1:
                break
            await asyncio.sleep(0.01)

        assert binance_api.logons > 1, "the session must be re-established"
        assert binance_api.user_subscribes > 1
        assert api.logged_on

        await binance_api.push_event(EXECUTION_REPORT)
        assert (await asyncio.wait_for(anext(reports), timeout=2.0)).s == "BTCUSDT"


async def test_reconnect_does_not_resubscribe_a_stream_nobody_reads(
    binance_api: FakeBinanceApi, binance_key
) -> None:
    _key, pem = binance_key
    async with _api(
        binance_api, api_key=API_KEY, api_secret=pem, retry_backoff=0.01
    ) as api:
        reports = await api.subscribe_execution_reports()
        reports.close()
        await binance_api.drop()

        for _ in range(300):
            if binance_api.logons > 1:
                break
            await asyncio.sleep(0.01)

        assert binance_api.logons > 1
        assert binance_api.user_subscribes == 1, "nothing was left reading it"


# --- WebSocket API: market data reads --------------------------------------


async def test_klines_come_back_oldest_first_in_the_asked_for_spelling(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.results[m.KLINES] = [
        [
            1499040000000,
            "1",
            "2",
            "0.5",
            "1.5",
            "100",
            1499040059999,
            "150",
            10,
            "50",
            "75",
            "0",
        ]
    ]
    async with _api(binance_api) as api:
        klines = await api.fetch_klines("BTCUSDT", "1m", ticker=TICKER, limit=5)

    assert len(klines) == 1
    assert klines[0].symbol == "BTCUSDT"
    assert klines[0].interval == "1m"
    assert klines[0].close == Decimal("1.5")
    assert binance_api.call(m.KLINES)["params"]["limit"] == 5


async def test_exchange_info_drops_symbols_that_cannot_be_traded(
    binance_api: FakeBinanceApi,
) -> None:
    binance_api.results[m.EXCHANGE_INFO] = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.00001000",
                        "minQty": "0.00001000",
                    },
                    {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
                ],
            },
            {
                "symbol": "HALTUSDT",
                "status": "HALT",
                "baseAsset": "HALT",
                "quoteAsset": "USDT",
                "filters": [],
            },
        ]
    }
    async with _api(binance_api) as api:
        instruments = await api.fetch_instruments()

    assert [i.symbol for i in instruments] == ["BTCUSDT"]
    btc = instruments[0]
    assert btc.tick_size == Decimal("0.01")
    assert btc.lot_size == Decimal("0.00001")
    assert btc.min_qty == Decimal("0.00001")
    assert btc.min_notional == Decimal("5")


async def test_a_zero_filter_reads_as_absent_not_as_a_zero_step(
    binance_api: FakeBinanceApi,
) -> None:
    """Binance publishes ``0`` for a step it does not enforce; zero divides."""
    binance_api.results[m.EXCHANGE_INFO] = {
        "symbols": [
            {
                "symbol": "XUSDT",
                "status": "TRADING",
                "baseAsset": "X",
                "quoteAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.00000000"},
                    {"filterType": "NOTIONAL", "minNotional": "0.00000000"},
                ],
            }
        ]
    }
    async with _api(binance_api) as api:
        instrument = (await api.fetch_instruments())[0]

    assert instrument.tick_size > 0
    assert instrument.min_notional is None

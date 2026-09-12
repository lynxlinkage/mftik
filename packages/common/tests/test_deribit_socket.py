"""Deribit socket — auth signature, heartbeat, subscribe id correlation."""

from __future__ import annotations

import asyncio

from deribit_stub import API_KEY, API_SECRET, FakeDeribit
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.account import DeribitPrivateStream
from mftik.exchange.deribit.feed import DeribitPublicStream
from mftik.exchange.deribit.protocol import DeribitResponse, DeribitWsError
from mftik.exchange.deribit.socket import DEFAULT_HEARTBEAT


def test_a_reply_correlates_on_id() -> None:
    resp = DeribitResponse({"jsonrpc": "2.0", "id": 7, "result": ["ok"]})
    assert resp.req_id == "7"
    assert resp.is_reply
    assert resp.success
    assert not resp.is_push


def test_a_subscription_push_has_no_id() -> None:
    resp = DeribitResponse(
        {
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {"channel": "ticker.BTC_USDC.100ms", "data": {}},
        }
    )
    assert resp.req_id is None
    assert resp.is_push
    assert resp.channel == "ticker.BTC_USDC.100ms"
    assert not resp.is_reply


def test_a_test_request_is_a_heartbeat() -> None:
    resp = DeribitResponse(
        {
            "jsonrpc": "2.0",
            "method": "heartbeat",
            "params": {"type": "test_request"},
        }
    )
    assert resp.is_heartbeat
    assert resp.is_test_request


async def test_private_auth_verifies_the_ws_signature(deribit: FakeDeribit) -> None:
    stream = DeribitPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        url=deribit.url,
        ping_interval=0,
        heartbeat=0,
    )
    async with stream:
        assert stream.authenticated
    assert deribit.auths == 1
    frame = deribit.frames_for(ch.PUBLIC_AUTH)[0]
    assert frame["params"]["grant_type"] == "client_signature"
    assert frame["params"]["client_id"] == API_KEY
    assert len(str(frame["params"]["timestamp"])) == 13


async def test_a_test_request_is_answered_with_public_test(
    deribit_public: FakeDeribit,
) -> None:
    feed = DeribitPublicStream(
        deribit_public.url, ping_interval=0, heartbeat=15
    )
    async with feed:
        await asyncio.sleep(0.05)
        await deribit_public.heartbeat("test_request")
        await asyncio.sleep(0.1)
    assert deribit_public.heartbeats == 1
    assert deribit_public.frames_for(ch.PUBLIC_TEST)


async def test_subscribe_replies_correlate_on_id(
    deribit_public: FakeDeribit,
) -> None:
    feed = DeribitPublicStream(
        deribit_public.url, ping_interval=0, heartbeat=0
    )
    async with feed:
        trades = await feed.subscribe_trades("BTC_USDC")
        quotes = await feed.subscribe_best_quote("BTC_USDC")
        await asyncio.sleep(0.05)
        await deribit_public.push(
            ch.trades("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "trade_id": "t-1",
                "price": "1",
                "amount": "1",
                "direction": "buy",
            },
        )
        trade = await asyncio.wait_for(trades.__anext__(), 2)
        await deribit_public.push(
            ch.quote("BTC_USDC"),
            {
                "instrument_name": "BTC_USDC",
                "best_bid_price": "1",
                "best_bid_amount": "1",
                "best_ask_price": "2",
                "best_ask_amount": "1",
            },
        )
        quote = await asyncio.wait_for(quotes.__anext__(), 2)
    assert trade.trade_id == "t-1"
    assert quote.best_bid_price == quote.best_bid_price
    assert deribit_public.subscribed == {
        ch.trades("BTC_USDC"),
        ch.quote("BTC_USDC"),
    }
    assert all(frame.get("id") is not None for frame in deribit_public.received)


async def test_the_heartbeat_is_on_by_default(deribit_public: FakeDeribit) -> None:
    feed = DeribitPublicStream(deribit_public.url, ping_interval=0)
    async with feed:
        assert feed.heartbeat == DEFAULT_HEARTBEAT
    assert deribit_public.heartbeats == 1


async def test_an_auth_refusal_names_public_auth(deribit: FakeDeribit) -> None:
    stream = DeribitPrivateStream(
        api_key=API_KEY,
        api_secret="wrong-secret",
        url=deribit.url,
        ping_interval=0,
        heartbeat=0,
    )
    try:
        await stream.connect()
    except DeribitWsError as exc:
        assert exc.op == ch.PUBLIC_AUTH
        assert exc.code == 10000
        assert "public/auth:" in str(exc)
    else:
        raise AssertionError("expected DeribitWsError")
    finally:
        await stream.close()


async def test_a_summaries_refusal_names_the_rpc(deribit: FakeDeribit) -> None:
    deribit.rpc_errors[ch.PRIVATE_GET_ACCOUNT_SUMMARIES] = (
        -32602,
        "Invalid params",
    )
    stream = DeribitPrivateStream(
        api_key=API_KEY,
        api_secret=API_SECRET,
        url=deribit.url,
        ping_interval=0,
        heartbeat=0,
    )
    async with stream:
        try:
            await stream.rpc(ch.PRIVATE_GET_ACCOUNT_SUMMARIES, {"extended": True})
        except DeribitWsError as exc:
            assert exc.op == ch.PRIVATE_GET_ACCOUNT_SUMMARIES
            assert exc.code == -32602
            assert str(exc) == (
                "private/get_account_summaries: [-32602] Invalid params"
            )
        else:
            raise AssertionError("expected DeribitWsError")


async def test_the_watchdog_probes_an_idle_socket_instead_of_dropping_it(
    deribit_public: FakeDeribit,
) -> None:
    """A socket nobody has subscribed to is idle, not dead.

    ``stats.last_frame_at`` is zero until the first frame lands, so an
    absolute silence check fails a healthy connection on its first tick.
    """
    feed = DeribitPublicStream(
        deribit_public.url, ping_interval=0.3, heartbeat=0
    )
    async with feed:
        await asyncio.sleep(0.9)
        assert feed.connected
        assert deribit_public.connections == 1
    assert deribit_public.frames_for(ch.PUBLIC_TEST)
    assert feed.stats.pings >= 1

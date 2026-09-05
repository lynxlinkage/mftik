"""Deribit socket — auth signature, heartbeat, subscribe id correlation."""

from __future__ import annotations

import asyncio

from deribit_stub import API_KEY, API_SECRET, FakeDeribit
from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.account import DeribitPrivateStream
from mftik.exchange.deribit.feed import DeribitPublicStream
from mftik.exchange.deribit.protocol import DeribitResponse


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

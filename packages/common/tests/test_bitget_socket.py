"""Bitget socket ACK correlation — id when present, else (event, arg) (V12)."""

from __future__ import annotations

import asyncio

from mftik.exchange.bitget import channels as ch
from mftik.exchange.bitget.protocol import BitgetResponse
from mftik.exchange.bitget.socket import BitgetSocket, _ack_matches, _Pending

TICKER = ch.ticker("spot", "BTCUSDT")
TRADE = ch.public_trade("spot", "BTCUSDT")
BOOKS1 = ch.books("spot", "BTCUSDT", topic=ch.BOOKS1)


def test_an_idless_subscribe_ack_matches_the_pending_arg() -> None:
    resp = BitgetResponse(
        {"event": "subscribe", "arg": TICKER, "connId": "c1"}
    )
    assert resp.req_id is None
    assert _ack_matches(resp, req_id="abc", op="subscribe", args=(TICKER,))
    assert not _ack_matches(resp, req_id="abc", op="subscribe", args=(TRADE,))
    assert not _ack_matches(resp, req_id="abc", op="unsubscribe", args=(TICKER,))


def test_an_idless_login_ack_matches_on_event() -> None:
    resp = BitgetResponse({"event": "login", "code": "0", "msg": ""})
    assert resp.req_id is None
    assert _ack_matches(resp, req_id="login-1", op="login")
    assert not _ack_matches(resp, req_id="login-1", op="subscribe")


def test_an_echoed_id_still_wins() -> None:
    resp = BitgetResponse(
        {"id": "abc", "event": "subscribe", "arg": TRADE, "code": "0"}
    )
    assert _ack_matches(resp, req_id="abc", op="subscribe", args=(TICKER,))
    assert not _ack_matches(resp, req_id="other", op="subscribe", args=(TRADE,))


async def test_two_idless_acks_on_one_socket_match_by_arg() -> None:
    """bestquote + trade share a socket; the first ACK must not steal both."""
    socket = BitgetSocket("ws://unused")
    loop = asyncio.get_running_loop()
    quote = _Pending(
        future=loop.create_future(), op="subscribe", args=(BOOKS1,)
    )
    trade = _Pending(
        future=loop.create_future(), op="subscribe", args=(TRADE,)
    )
    socket._pending["quote"] = quote
    socket._pending["trade"] = trade

    socket._dispatch(
        BitgetResponse({"event": "subscribe", "arg": TRADE, "connId": "c1"})
    )
    socket._dispatch(
        BitgetResponse({"event": "subscribe", "arg": BOOKS1, "connId": "c1"})
    )

    assert (await asyncio.wait_for(trade.future, 0.1)).symbol == "BTCUSDT"
    assert (await asyncio.wait_for(quote.future, 0.1)).topic == ch.BOOKS1
    assert trade.future.result().topic == ch.PUBLIC_TRADE
    assert quote.future.result().topic == ch.BOOKS1

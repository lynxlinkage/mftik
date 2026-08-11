"""Binance USDⓈ-M futures endpoints, on the venue-wide framing.

How a frame is built, signed and read is identical to spot — same envelope,
same Ed25519 ``session.logon``, same error block — so all of it comes from
:mod:`mft.exchange.binance.protocol` and is re-exported here. What is futures'
own is *where the frames go*, and that is where this market stops resembling
the other one.

**Order entry and account reads: one URL.** ``wss://ws-fapi.binance.com`` is
the futures WebSocket API, the same request/reply socket spot has on
``ws-api.binance.com``.

**Market pushes: three URLs, split by traffic class.** Binance retired the old
``fstream.binance.com/ws`` and ``/stream`` endpoints on 2026-04-23 and replaced
them with one endpoint per class of feed:

* ``/public`` — the book: ``@bookTicker``, ``@depth<levels>``, ``@depth``.
* ``/market`` — everything else: ``@aggTrade``, ``@markPrice``, ``@kline_*``,
  ``@ticker``, ``@miniTicker``, ``@forceOrder``.
* ``/private`` — the user data stream, addressed by listen key.

A subscribe sent to the wrong one is *accepted and then silent*, which is the
worst failure mode a feed has, so nothing here picks an endpoint by hand:
:func:`mft.exchange.binance.future.streams.group_of` maps a stream name to its
class and :class:`~mft.exchange.binance.future.feed.BinanceFutureStream` holds
one socket per class.

**The user data stream is a listen key, not a subscription.** Spot subscribes
to it on the authenticated WebSocket API connection; futures has no such
method. ``userDataStream.start`` answers with a key, the events arrive on a
*different* socket opened at ``/private/ws/<listenKey>``, and the key dies 60
minutes after its last ping. See
:class:`~mft.exchange.binance.future.user.BinanceFutureUserStream`.
"""

from __future__ import annotations

from mft.exchange.binance.protocol import (
    SESSION_LOGON,
    BinanceAuthError,
    BinanceResponse,
    BinanceWsError,
    decimal_text,
    load_private_key,
    logon_frame,
    now_ms,
    payload_for,
    render,
    request_frame,
    sign,
    signed_frame,
    subscribe_frame,
    wire,
)

#: Request/reply: trading, account reads, listen keys, and the three market
#: snapshots the futures WebSocket API serves.
BINANCE_FUTURE_WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"

#: Market pushes, by traffic class. Always the ``/stream`` (combined) form, for
#: the reason :mod:`mft.exchange.binance.feed` gives: the wrapper puts the
#: stream name on every message.
BINANCE_FUTURE_PUBLIC_STREAM_URL = "wss://fstream.binance.com/public/stream"
BINANCE_FUTURE_MARKET_STREAM_URL = "wss://fstream.binance.com/market/stream"
#: User data. A listen key is appended: ``…/private/ws/<listenKey>``.
BINANCE_FUTURE_PRIVATE_STREAM_URL = "wss://fstream.binance.com/private/ws"

#: REST, for the two reads the futures WebSocket API does not serve at all —
#: candles and the instrument listing — and for the signed open-orders scan
#: recon needs. See :mod:`mft.exchange.binance.future.rest`.
BINANCE_FUTURE_REST_URL = "https://fapi.binance.com"

#: Testnet, for smoke-testing a credential without risking one. It is not split
#: by traffic class — the whole point of the split is production load — so the
#: one stream host answers for every feed.
BINANCE_FUTURE_WS_API_TESTNET_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
BINANCE_FUTURE_STREAM_TESTNET_URL = "wss://stream.binancefuture.com/stream"
BINANCE_FUTURE_REST_TESTNET_URL = "https://testnet.binancefuture.com"


def user_stream_url(
    listen_key: str, *, base: str = BINANCE_FUTURE_PRIVATE_STREAM_URL
) -> str:
    """The socket one listen key's events arrive on.

    A function rather than a format string at the call site because the key is
    a credential: it is the *only* thing authenticating that socket, and
    building the URL in one place keeps it out of logs and off the end of a
    combined-stream query string, where it would be a subscription name.
    """
    if not listen_key:
        raise BinanceAuthError(
            "a futures user data socket needs a listen key; "
            "call userDataStream.start first"
        )
    return f"{base.rstrip('/')}/{listen_key}"


__all__ = [
    "BINANCE_FUTURE_MARKET_STREAM_URL",
    "BINANCE_FUTURE_PRIVATE_STREAM_URL",
    "BINANCE_FUTURE_PUBLIC_STREAM_URL",
    "BINANCE_FUTURE_REST_TESTNET_URL",
    "BINANCE_FUTURE_REST_URL",
    "BINANCE_FUTURE_STREAM_TESTNET_URL",
    "BINANCE_FUTURE_WS_API_TESTNET_URL",
    "BINANCE_FUTURE_WS_API_URL",
    "SESSION_LOGON",
    "BinanceAuthError",
    "BinanceResponse",
    "BinanceWsError",
    "decimal_text",
    "load_private_key",
    "logon_frame",
    "now_ms",
    "payload_for",
    "render",
    "request_frame",
    "sign",
    "signed_frame",
    "subscribe_frame",
    "user_stream_url",
    "wire",
]

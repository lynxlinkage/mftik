"""Binance COIN-M (dapi) endpoints, on the venue-wide framing.

This is Binance's historical "delivery" plane: coin-margined perpetuals and
dated contracts, registered as the venue ``BinanceCM``. Framing, signing
and the error block are the same as spot and USD-M — they live in
:mod:`mftik.exchange.binance.protocol`. What is this market's own is *where
the frames go*.

**REST is the listing and candle path.** ``exchangeInfo`` and ``klines`` have
no WebSocket API method on dapi either. The host is ``dapi.binance.com``,
prefix ``/dapi/v1``.

**Sockets are two hosts, three jobs.** Market pushes stay on one combined
stream (``dstream.binance.com/stream``). Order entry and account reads are
the WebSocket API on ``ws-dapi.binance.com`` — request/reply, authenticated
by ``session.logon``. The account *feed* is a listen key issued on that API
and read at ``dstream.binance.com/ws/<listenKey>``. dapi was not part of
the 2026 ``/public`` / ``/market`` / ``/private`` split.
"""

from __future__ import annotations

from mftik.exchange.binance.protocol import (
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

#: Request/reply — trading, account reads, ``session.logon``.
BINANCE_DELIVERY_WS_API_URL = "wss://ws-dapi.binance.com/ws-dapi/v1"

#: Combined market stream. dapi was not part of the 2026 ``/public`` /
#: ``/market`` / ``/private`` split that USD-M took.
BINANCE_DELIVERY_STREAM_URL = "wss://dstream.binance.com/stream"

#: User data. A listen key is appended: ``…/ws/<listenKey>``.
BINANCE_DELIVERY_PRIVATE_STREAM_URL = "wss://dstream.binance.com/ws"

#: REST — instrument listing, candles, ticker and depth.
BINANCE_DELIVERY_REST_URL = "https://dapi.binance.com"

#: Testnet shares the USD-M test host; the path, not the host, is dapi.
BINANCE_DELIVERY_WS_API_TESTNET_URL = "wss://testnet.binancefuture.com/ws-dapi/v1"
BINANCE_DELIVERY_STREAM_TESTNET_URL = "wss://dstream.binancefuture.com/stream"
BINANCE_DELIVERY_REST_TESTNET_URL = "https://testnet.binancefuture.com"


def user_stream_url(
    listen_key: str, *, base: str = BINANCE_DELIVERY_PRIVATE_STREAM_URL
) -> str:
    """The socket one listen key's events arrive on.

    The key is a credential: it is the only thing authenticating that
    socket. Built in one place so it stays off the combined-stream query
    string, where it would be a subscription name.
    """
    if not listen_key:
        raise BinanceAuthError(
            "a delivery user data socket needs a listen key; "
            "call userDataStream.start first"
        )
    return f"{base.rstrip('/')}/{listen_key}"


__all__ = [
    "BINANCE_DELIVERY_PRIVATE_STREAM_URL",
    "BINANCE_DELIVERY_REST_TESTNET_URL",
    "BINANCE_DELIVERY_REST_URL",
    "BINANCE_DELIVERY_STREAM_TESTNET_URL",
    "BINANCE_DELIVERY_STREAM_URL",
    "BINANCE_DELIVERY_WS_API_TESTNET_URL",
    "BINANCE_DELIVERY_WS_API_URL",
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

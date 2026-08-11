"""Binance spot endpoints, on the venue-wide framing.

Everything about *how* a Binance frame is built, signed and read is the same on
spot and on futures, so it lives one level up in
:mod:`mft.exchange.binance.protocol` and is re-exported here — this module is
the spot half: which URLs those frames go to.

Two of them, because Binance splits what Gate serves on one: request/reply on
``ws-api.binance.com`` and market pushes on ``stream.binance.com``.
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

#: Request/reply: trading, account reads, and market-data snapshots.
BINANCE_SPOT_WS_API_URL = "wss://ws-api.binance.com:443/ws-api/v3"
#: Market pushes. Always the ``/stream`` (combined) form — see :mod:`.streams`.
BINANCE_SPOT_STREAM_URL = "wss://stream.binance.com:9443/stream"

#: Testnet, for smoke-testing a credential without risking one.
BINANCE_SPOT_WS_API_TESTNET_URL = "wss://ws-api.testnet.binance.vision/ws-api/v3"
BINANCE_SPOT_STREAM_TESTNET_URL = "wss://stream.testnet.binance.vision/stream"

__all__ = [
    "BINANCE_SPOT_STREAM_TESTNET_URL",
    "BINANCE_SPOT_STREAM_URL",
    "BINANCE_SPOT_WS_API_TESTNET_URL",
    "BINANCE_SPOT_WS_API_URL",
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
    "wire",
]

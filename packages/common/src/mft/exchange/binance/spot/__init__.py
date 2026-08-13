"""Binance spot adapter — two sockets, one venue.

Binance splits what Gate serves on one URL: market pushes come from
``stream.binance.com`` and everything request/reply — orders, cancels, account
reads, market-data snapshots and the user data stream — from
``ws-api.binance.com``. So this package has two socket classes rather than one,
sharing their connection machinery through
:class:`~mft.exchange.binance.spot.socket.BinanceSocket`.

* :class:`BinanceSpotStream` — public market pushes.
* :class:`BinanceSpotWsApi` — request/reply, authenticated by an Ed25519
  ``session.logon`` when credentials are supplied and open to everyone
  otherwise.

Above those sit the two connectors the platform composes:
:class:`BinanceSpotPublicClient` for MD and :class:`BinanceSpotPrivateClient`
for TD.

The **trading path** has no REST half on purpose: Binance answers every read it
needs over the WebSocket API, and one envelope and one error type across the
live order path is worth more than a second transport would be.
:class:`BinanceSpotRest` sits outside that path — history reads for the
backfill plane, holding no order state — and is not composed into
:class:`BinanceSpotPrivateClient`. See :mod:`mft.exchange.binance.spot.rest`.
"""

from mft.exchange.binance.spot import methods, streams
from mft.exchange.binance.spot.client import (
    MAX_DEPTH,
    MAX_KLINES,
    BinanceSpotWsApi,
)
from mft.exchange.binance.spot.feed import BinanceSpotStream
from mft.exchange.binance.spot.models import (
    BinanceAccountBalance,
    BinanceAccountInfo,
    BinanceAccountPosition,
    BinanceAggTrade,
    BinanceBalanceUpdate,
    BinanceBookTicker,
    BinanceDepth,
    BinanceDepthUpdate,
    BinanceExecutionReport,
    BinanceKlineEvent,
    BinanceKlineWindow,
    BinanceMessage,
    BinanceOrderAck,
    BinanceOrderFill,
    BinanceSpotHistoricalOrder,
    BinanceSpotMyTrade,
    BinanceTicker,
    BinanceTrade,
    kline_from_row,
    side_of,
    status_of,
    type_of,
)
from mft.exchange.binance.spot.private import BinanceSpotPrivateClient
from mft.exchange.binance.spot.protocol import (
    BINANCE_SPOT_REST_TESTNET_URL,
    BINANCE_SPOT_REST_URL,
    BINANCE_SPOT_STREAM_TESTNET_URL,
    BINANCE_SPOT_STREAM_URL,
    BINANCE_SPOT_WS_API_TESTNET_URL,
    BINANCE_SPOT_WS_API_URL,
    BinanceAuthError,
    BinanceResponse,
    BinanceWsError,
    load_private_key,
    logon_frame,
    payload_for,
    request_frame,
    sign,
    signed_frame,
    subscribe_frame,
)
from mft.exchange.binance.spot.public import (
    BINANCE_INTERVALS,
    BinanceSpotPublicClient,
    venue_interval,
)
from mft.exchange.binance.spot.rest import BinanceSpotRest, BinanceSpotRestError
from mft.exchange.binance.spot.socket import BinanceSocket

__all__ = [
    "BINANCE_INTERVALS",
    "BINANCE_SPOT_REST_TESTNET_URL",
    "BINANCE_SPOT_REST_URL",
    "BINANCE_SPOT_STREAM_TESTNET_URL",
    "BINANCE_SPOT_STREAM_URL",
    "BINANCE_SPOT_WS_API_TESTNET_URL",
    "BINANCE_SPOT_WS_API_URL",
    "MAX_DEPTH",
    "MAX_KLINES",
    "BinanceAccountBalance",
    "BinanceAccountInfo",
    "BinanceAccountPosition",
    "BinanceAggTrade",
    "BinanceAuthError",
    "BinanceBalanceUpdate",
    "BinanceBookTicker",
    "BinanceDepth",
    "BinanceDepthUpdate",
    "BinanceExecutionReport",
    "BinanceKlineEvent",
    "BinanceKlineWindow",
    "BinanceMessage",
    "BinanceOrderAck",
    "BinanceOrderFill",
    "BinanceResponse",
    "BinanceSocket",
    "BinanceSpotHistoricalOrder",
    "BinanceSpotMyTrade",
    "BinanceSpotPrivateClient",
    "BinanceSpotPublicClient",
    "BinanceSpotRest",
    "BinanceSpotRestError",
    "BinanceSpotStream",
    "BinanceSpotWsApi",
    "BinanceTicker",
    "BinanceTrade",
    "BinanceWsError",
    "kline_from_row",
    "load_private_key",
    "logon_frame",
    "methods",
    "payload_for",
    "request_frame",
    "side_of",
    "sign",
    "signed_frame",
    "status_of",
    "streams",
    "subscribe_frame",
    "type_of",
    "venue_interval",
]

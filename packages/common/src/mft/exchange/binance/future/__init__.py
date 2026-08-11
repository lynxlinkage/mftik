"""Binance USDⓈ-M futures adapter — one venue, four connections.

The perpetual half of Binance, registered as the venue ``BinanceFuture`` and
addressed as ``BinanceFuture_Perp_BTCUSDT``. A venue of its own rather than a
category of the spot one, because that is what the API is: separate hosts,
separate credentials, separate wallet, separate order book — see
:mod:`mft.exchange.venues` for the rule.

It shares its framing, signing and socket machinery with the spot adapter
(:mod:`mft.exchange.binance.protocol`, :mod:`mft.exchange.binance.socket`,
:mod:`mft.exchange.binance.feed`) and almost nothing else. Where it differs it
differs structurally:

* **Market pushes are split across two endpoints.** ``/public`` carries the
  book and ``/market`` everything else, since Binance retired the single
  ``fstream`` endpoint in April 2026 — and a subscribe on the wrong one is
  accepted and then silent. :class:`BinanceFutureStream` holds one socket per
  class and routes by stream name.
* **The account feed is a listen key**, read on a third socket
  (:class:`BinanceFutureUserStream`), not a subscription on the WebSocket API.
  It expires 60 minutes after its last ping.
* **Candles, the instrument listing and "what is open" are REST-only**, which
  is why this package has a REST client where the spot one has none.
* **Positions and liquidations exist**, and the shared models for both were
  waiting for a venue that had them.

Above those sit the two connectors the platform composes:
:class:`BinanceFuturePublicClient` for MD and
:class:`BinanceFuturePrivateClient` for TD.
"""

from mft.exchange.binance.future import methods, streams
from mft.exchange.binance.future.client import MAX_DEPTH, BinanceFutureWsApi
from mft.exchange.binance.future.feed import (
    DEFAULT_BOOK_LEVELS,
    DEFAULT_BOOK_SPEED,
    BinanceFutureStream,
)
from mft.exchange.binance.future.models import (
    BinanceFutureAccountUpdate,
    BinanceFutureAggTrade,
    BinanceFutureBalance,
    BinanceFutureBookQuote,
    BinanceFutureBookTicker,
    BinanceFutureDepth,
    BinanceFutureDepthUpdate,
    BinanceFutureKlineEvent,
    BinanceFutureKlineWindow,
    BinanceFutureLiquidation,
    BinanceFutureLiquidationOrder,
    BinanceFutureMarkPrice,
    BinanceFutureOrderAck,
    BinanceFutureOrderTradeUpdate,
    BinanceFutureOrderUpdate,
    BinanceFuturePosition,
    BinanceFuturePositionRow,
    BinanceFuturePrice,
    BinanceFutureTicker,
    BinanceFutureWalletBalance,
    BinanceListenKeyExpired,
    instrument_from_row,
    kline_from_row,
    status_of,
    type_of,
)
from mft.exchange.binance.future.private import BinanceFuturePrivateClient
from mft.exchange.binance.future.protocol import (
    BINANCE_FUTURE_MARKET_STREAM_URL,
    BINANCE_FUTURE_PRIVATE_STREAM_URL,
    BINANCE_FUTURE_PUBLIC_STREAM_URL,
    BINANCE_FUTURE_REST_TESTNET_URL,
    BINANCE_FUTURE_REST_URL,
    BINANCE_FUTURE_STREAM_TESTNET_URL,
    BINANCE_FUTURE_WS_API_TESTNET_URL,
    BINANCE_FUTURE_WS_API_URL,
    BinanceAuthError,
    BinanceResponse,
    BinanceWsError,
    user_stream_url,
)
from mft.exchange.binance.future.public import (
    BINANCE_FUTURE_INTERVALS,
    BinanceFuturePublicClient,
    venue_interval,
)
from mft.exchange.binance.future.rest import (
    BinanceFuturePublicRest,
    BinanceFutureRest,
    BinanceFutureRestError,
)
from mft.exchange.binance.future.user import (
    KEEPALIVE_SECONDS,
    BinanceFutureUserStream,
)

__all__ = [
    "BINANCE_FUTURE_INTERVALS",
    "BINANCE_FUTURE_MARKET_STREAM_URL",
    "BINANCE_FUTURE_PRIVATE_STREAM_URL",
    "BINANCE_FUTURE_PUBLIC_STREAM_URL",
    "BINANCE_FUTURE_REST_TESTNET_URL",
    "BINANCE_FUTURE_REST_URL",
    "BINANCE_FUTURE_STREAM_TESTNET_URL",
    "BINANCE_FUTURE_WS_API_TESTNET_URL",
    "BINANCE_FUTURE_WS_API_URL",
    "DEFAULT_BOOK_LEVELS",
    "DEFAULT_BOOK_SPEED",
    "KEEPALIVE_SECONDS",
    "MAX_DEPTH",
    "BinanceAuthError",
    "BinanceFutureAccountUpdate",
    "BinanceFutureAggTrade",
    "BinanceFutureBalance",
    "BinanceFutureBookQuote",
    "BinanceFutureBookTicker",
    "BinanceFutureDepth",
    "BinanceFutureDepthUpdate",
    "BinanceFutureKlineEvent",
    "BinanceFutureKlineWindow",
    "BinanceFutureLiquidation",
    "BinanceFutureLiquidationOrder",
    "BinanceFutureMarkPrice",
    "BinanceFutureOrderAck",
    "BinanceFutureOrderTradeUpdate",
    "BinanceFutureOrderUpdate",
    "BinanceFuturePosition",
    "BinanceFuturePositionRow",
    "BinanceFuturePrice",
    "BinanceFuturePrivateClient",
    "BinanceFuturePublicClient",
    "BinanceFuturePublicRest",
    "BinanceFutureRest",
    "BinanceFutureRestError",
    "BinanceFutureStream",
    "BinanceFutureTicker",
    "BinanceFutureUserStream",
    "BinanceFutureWalletBalance",
    "BinanceFutureWsApi",
    "BinanceListenKeyExpired",
    "BinanceResponse",
    "BinanceWsError",
    "instrument_from_row",
    "kline_from_row",
    "methods",
    "status_of",
    "streams",
    "type_of",
    "user_stream_url",
    "venue_interval",
]

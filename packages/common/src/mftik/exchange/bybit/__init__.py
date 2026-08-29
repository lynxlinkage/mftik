"""Bybit v5 adapter — one credential, four books, three sockets.

Bybit is the platform's first **unified** venue, and the reason
:class:`~mftik.exchange.tickers.UniversalTicker` carries a category at all: one
API key trades spot, linear perps, inverse perps and options, so ``Bybit_Spot_
BTCUSDT`` and ``Bybit_Perp_BTCUSDT`` are two instruments behind one connection
rather than two venues that share a brand. There is no ``bybit/spot`` package
here for that reason — the category is a parameter, not an endpoint.

What *is* split is the transports, along an axis that has nothing to do with
the account:

* :class:`BybitPrivateStream` — ``wss://…/v5/private``. Account pushes: order,
  execution, wallet, position, every category at once. TD's report path, and
  the one thing an order-entry adapter cannot work without, because Bybit's
  order acks carry no state.
* :class:`BybitTradeSocket` — ``wss://…/v5/trade``. Order entry, request/reply.
* :class:`BybitPublicStream` — ``wss://…/v5/public/<category>``. Market pushes,
  one socket per category.
* :class:`BybitRest` / :class:`BybitPublicRest` — the reads no socket serves:
  open orders, balances, instruments, candle history.

Above those sit the two connectors the platform composes:
:class:`BybitPublicClient` for MD and :class:`BybitPrivateClient` for TD.

Unlike Binance, REST is not optional here: "what is open right now" and "what
is the balance" have no WebSocket form on this venue, and recon needs both.
"""

from mftik.exchange.bybit import channels
from mftik.exchange.bybit.account import BybitPrivateStream
from mftik.exchange.bybit.feed import (
    DEFAULT_BOOK_DEPTH,
    BybitBook,
    BybitBookSnapshot,
    BybitPublicStream,
)
from mftik.exchange.bybit.models import (
    EXEC_TYPE_TRADE,
    BybitExecution,
    BybitKline,
    BybitLiquidation,
    BybitMessage,
    BybitOrderAck,
    BybitOrderBook,
    BybitOrderUpdate,
    BybitPosition,
    BybitPublicTrade,
    BybitTicker,
    BybitWallet,
    BybitWalletCoin,
    kline_from_row,
    order_book_from_result,
    status_of,
    type_of,
)
from mftik.exchange.bybit.private import BybitPrivateClient
from mftik.exchange.bybit.protocol import (
    BYBIT_REST_TESTNET_URL,
    BYBIT_REST_URL,
    BYBIT_WS_PRIVATE_TESTNET_URL,
    BYBIT_WS_PRIVATE_URL,
    BYBIT_WS_PUBLIC_URL,
    BYBIT_WS_TRADE_TESTNET_URL,
    BYBIT_WS_TRADE_URL,
    INVERSE,
    LINEAR,
    OPTION,
    SPOT,
    BybitAuthError,
    BybitError,
    BybitResponse,
    BybitRestError,
    BybitWsError,
    auth_frame,
    product_of,
    public_url,
    sign_rest,
    sign_ws,
    subscribe_frame,
    trade_frame,
)
from mftik.exchange.bybit.public import (
    BYBIT_INTERVALS,
    LIQUIDATION_PRODUCTS,
    BybitPublicClient,
    venue_interval,
)
from mftik.exchange.bybit.rest import UNIFIED, BybitPublicRest, BybitRest
from mftik.exchange.bybit.socket import BybitSocket
from mftik.exchange.bybit.trade import BybitTradeSocket

__all__ = [
    "BYBIT_INTERVALS",
    "BYBIT_REST_TESTNET_URL",
    "BYBIT_REST_URL",
    "BYBIT_WS_PRIVATE_TESTNET_URL",
    "BYBIT_WS_PRIVATE_URL",
    "BYBIT_WS_PUBLIC_URL",
    "BYBIT_WS_TRADE_TESTNET_URL",
    "BYBIT_WS_TRADE_URL",
    "DEFAULT_BOOK_DEPTH",
    "EXEC_TYPE_TRADE",
    "INVERSE",
    "LINEAR",
    "OPTION",
    "SPOT",
    "UNIFIED",
    "BybitAuthError",
    "BybitBook",
    "BybitBookSnapshot",
    "BybitError",
    "BybitExecution",
    "BybitKline",
    "BybitLiquidation",
    "BybitMessage",
    "BybitOrderAck",
    "BybitOrderBook",
    "BybitOrderUpdate",
    "BybitPosition",
    "BybitPrivateClient",
    "BybitPrivateStream",
    "BybitPublicClient",
    "BybitPublicRest",
    "BybitPublicStream",
    "BybitPublicTrade",
    "BybitResponse",
    "BybitRest",
    "BybitRestError",
    "BybitSocket",
    "BybitTicker",
    "BybitTradeSocket",
    "BybitWallet",
    "BybitWalletCoin",
    "BybitWsError",
    "LIQUIDATION_PRODUCTS",
    "auth_frame",
    "channels",
    "kline_from_row",
    "order_book_from_result",
    "product_of",
    "public_url",
    "sign_rest",
    "sign_ws",
    "status_of",
    "subscribe_frame",
    "trade_frame",
    "type_of",
    "venue_interval",
]

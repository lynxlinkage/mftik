"""Bitget UTA v3 adapter — one credential, two books, three public sockets.

Bitget is a **unified** venue: one API key (plus a passphrase) trades spot
and both linear perpetual books, so ``Bitget_Spot_BTCUSDT`` and
``Bitget_Perp_BTCUSDT`` (and ``Bitget_Perp_BTCUSDC``) are instruments
behind one connection rather than two venues that share a brand. There is
no ``bitget/spot`` package here for that reason — the category is a
parameter, not an endpoint.

What *is* split is the wire product on the two linear books. Identity is
one :class:`~mftik.exchange.tickers.Category.PERP`; routing is not.
:func:`product_of` therefore takes a ticker, never a category alone.

Classic Bitget accounts are not modelled. The adapter talks to the
unified trading account (``/api/v3/account/assets``, ``settings``) and
nowhere else.
"""

from mftik.exchange.bitget.account import BitgetPrivateStream
from mftik.exchange.bitget.feed import (
    DEFAULT_BOOK_CHANNEL,
    BitgetBook,
    BitgetBookSnapshot,
    BitgetPublicStream,
)
from mftik.exchange.bitget.models import (
    CANCEL_REFUSALS,
    BitgetAccount,
    BitgetAsset,
    BitgetFill,
    BitgetLiquidation,
    BitgetOrderAck,
    BitgetOrderBook,
    BitgetOrderUpdate,
    BitgetPosition,
    BitgetPublicTrade,
    BitgetSettings,
    BitgetTicker,
    category_of,
    kline_from_row,
    order_book_from_result,
    status_of,
    type_of,
)
from mftik.exchange.bitget.private import BitgetPrivateClient
from mftik.exchange.bitget.protocol import (
    BITGET_REST_URL,
    BITGET_WS_PRIVATE_URL,
    BITGET_WS_PUBLIC_URL,
    SPOT,
    USDC_FUTURES,
    USDT_FUTURES,
    BitgetAccountModeError,
    BitgetAuthError,
    BitgetError,
    BitgetResponse,
    BitgetRestError,
    BitgetWsError,
    inst_type_of,
    product_of,
    sign_rest,
    sign_ws,
)
from mftik.exchange.bitget.public import (
    BITGET_INTERVALS,
    FUNDING_CATEGORIES,
    LIQUIDATION_PRODUCTS,
    OPEN_INTEREST_CATEGORIES,
    BitgetPublicClient,
    venue_interval,
)
from mftik.exchange.bitget.rest import BitgetPublicRest, BitgetRest
from mftik.exchange.bitget.socket import BitgetSocket

__all__ = [
    "BITGET_INTERVALS",
    "BITGET_REST_URL",
    "BITGET_WS_PRIVATE_URL",
    "BITGET_WS_PUBLIC_URL",
    "CANCEL_REFUSALS",
    "DEFAULT_BOOK_CHANNEL",
    "FUNDING_CATEGORIES",
    "LIQUIDATION_PRODUCTS",
    "OPEN_INTEREST_CATEGORIES",
    "SPOT",
    "USDC_FUTURES",
    "USDT_FUTURES",
    "BitgetAccount",
    "BitgetAsset",
    "BitgetAccountModeError",
    "BitgetAuthError",
    "BitgetBook",
    "BitgetBookSnapshot",
    "BitgetError",
    "BitgetFill",
    "BitgetLiquidation",
    "BitgetOrderAck",
    "BitgetOrderBook",
    "BitgetOrderUpdate",
    "BitgetPosition",
    "BitgetPrivateClient",
    "BitgetPrivateStream",
    "BitgetPublicClient",
    "BitgetPublicRest",
    "BitgetPublicStream",
    "BitgetPublicTrade",
    "BitgetResponse",
    "BitgetRest",
    "BitgetRestError",
    "BitgetSettings",
    "BitgetSocket",
    "BitgetTicker",
    "BitgetWsError",
    "category_of",
    "inst_type_of",
    "kline_from_row",
    "order_book_from_result",
    "product_of",
    "sign_rest",
    "sign_ws",
    "status_of",
    "type_of",
    "venue_interval",
]

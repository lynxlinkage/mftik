"""OKX v5 adapter — one credential, two books, three sockets.

OKX is a **unified** venue: one API key (plus a passphrase) trades spot and
USDT-margined swaps, so ``Okx_Spot_BTCUSDT`` and ``Okx_Perp_BTCUSDT`` are
two instruments behind one connection rather than two venues that share a
brand. There is no ``okx/spot`` package here for that reason — the category
is a parameter, not an endpoint.

What *is* split is the transports:

* :class:`OkxPrivateStream` — ``wss://…/ws/v5/private``. Account pushes.
* :class:`OkxPublicStream` — ``wss://…/ws/v5/public`` and the business
  socket for candles.
* :class:`OkxRest` / :class:`OkxPublicRest` — order entry and the reads no
  socket serves.

Above those sit the two connectors the platform composes:
:class:`OkxPublicClient` for MD and :class:`OkxPrivateClient` for TD.

Classic OKX accounts are not modelled. The adapter talks to the unified
trading account (``/api/v5/account/balance``, ``positions``) and nowhere
else.
"""

from mftik.exchange.okx.account import OkxPrivateStream
from mftik.exchange.okx.feed import (
    DEFAULT_BOOK_CHANNEL,
    OkxBook,
    OkxBookSnapshot,
    OkxPublicStream,
)
from mftik.exchange.okx.models import (
    OkxAccount,
    OkxFill,
    OkxFundingRate,
    OkxOpenInterest,
    OkxOrderAck,
    OkxOrderBook,
    OkxOrderUpdate,
    OkxPosition,
    OkxPublicTrade,
    OkxTicker,
    base_to_contracts,
    category_of,
    contracts_to_base,
    kline_from_row,
    order_book_from_result,
    status_of,
    type_of,
)
from mftik.exchange.okx.private import OkxPrivateClient
from mftik.exchange.okx.protocol import (
    OKX_REST_URL,
    OKX_WS_BUSINESS_URL,
    OKX_WS_PRIVATE_URL,
    OKX_WS_PUBLIC_URL,
    SPOT,
    SWAP,
    OkxAuthError,
    OkxError,
    OkxResponse,
    OkxRestError,
    OkxWsError,
    product_of,
    sign_rest,
    sign_ws,
)
from mftik.exchange.okx.public import (
    FUNDING_PRODUCTS,
    LIQUIDATION_PRODUCTS,
    OKX_INTERVALS,
    OPEN_INTEREST_PRODUCTS,
    OkxPublicClient,
    venue_interval,
)
from mftik.exchange.okx.rest import OkxPublicRest, OkxRest
from mftik.exchange.okx.socket import OkxSocket

__all__ = [
    "DEFAULT_BOOK_CHANNEL",
    "FUNDING_PRODUCTS",
    "LIQUIDATION_PRODUCTS",
    "OPEN_INTEREST_PRODUCTS",
    "OKX_INTERVALS",
    "OKX_REST_URL",
    "OKX_WS_BUSINESS_URL",
    "OKX_WS_PRIVATE_URL",
    "OKX_WS_PUBLIC_URL",
    "SPOT",
    "SWAP",
    "OkxAccount",
    "OkxAuthError",
    "OkxBook",
    "OkxBookSnapshot",
    "OkxError",
    "OkxFill",
    "OkxFundingRate",
    "OkxOpenInterest",
    "OkxOrderAck",
    "OkxOrderBook",
    "OkxOrderUpdate",
    "OkxPosition",
    "OkxPrivateClient",
    "OkxPrivateStream",
    "OkxPublicClient",
    "OkxPublicRest",
    "OkxPublicStream",
    "OkxPublicTrade",
    "OkxResponse",
    "OkxRest",
    "OkxRestError",
    "OkxSocket",
    "OkxTicker",
    "OkxWsError",
    "base_to_contracts",
    "category_of",
    "contracts_to_base",
    "kline_from_row",
    "order_book_from_result",
    "product_of",
    "sign_rest",
    "sign_ws",
    "status_of",
    "type_of",
    "venue_interval",
]

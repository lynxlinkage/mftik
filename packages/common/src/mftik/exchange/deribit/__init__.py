"""Deribit adapter — one HMAC credential, four books, one public socket.

Deribit is a **unified** venue: one Client ID / Client Secret (no
passphrase) trades spot, linear perps, inverse perps and dated futures,
so ``Deribit_Spot_BTCUSDC``, ``Deribit_Perp_BTCUSDC``,
``Deribit_Inverse_BTCUSD`` and ``Deribit_Future_BTCUSD-260906`` are
instruments behind one connection. Options are not modelled.

HTTP and WebSocket speak the same JSON-RPC 2.0 methods. v1 places and
cancels on the authenticated socket; REST is listing and MDS.
"""

from mftik.exchange.deribit.account import DeribitPrivateStream
from mftik.exchange.deribit.feed import (
    DeribitBook,
    DeribitBookSnapshot,
    DeribitPublicStream,
)
from mftik.exchange.deribit.models import (
    DeribitAccountSummaries,
    DeribitFill,
    DeribitOrderAck,
    DeribitOrderBook,
    DeribitOrderUpdate,
    DeribitPosition,
    DeribitPublicTrade,
    DeribitQuote,
    DeribitSummary,
    DeribitTicker,
    category_of,
    kline_from_chart,
    kline_from_tick,
    order_book_from_result,
    status_of,
    type_of,
)
from mftik.exchange.deribit.private import DeribitPrivateClient
from mftik.exchange.deribit.protocol import (
    DERIBIT_REST_URL,
    DERIBIT_WS_URL,
    KIND_FUTURE,
    KIND_SPOT,
    LINEAR,
    MARGIN_MODELS,
    PERPETUAL,
    DeribitAuthError,
    DeribitError,
    DeribitResponse,
    DeribitRestError,
    DeribitWsError,
    auth_params,
    category_from_instrument,
    expiry_code_from_name,
    is_cbe_routed,
    is_dated_future,
    is_inverse_perp,
    is_inverse_perp_name,
    is_linear_perp,
    is_linear_perp_name,
    kind_of,
    sign_rest,
    sign_ws,
)
from mftik.exchange.deribit.public import (
    DERIBIT_INTERVALS,
    FUNDING_CATEGORIES,
    OPEN_INTEREST_CATEGORIES,
    DeribitPublicClient,
    venue_interval,
)
from mftik.exchange.deribit.rest import DeribitPublicRest
from mftik.exchange.deribit.socket import DeribitSocket

__all__ = [
    "DERIBIT_INTERVALS",
    "DERIBIT_REST_URL",
    "DERIBIT_WS_URL",
    "FUNDING_CATEGORIES",
    "KIND_FUTURE",
    "KIND_SPOT",
    "LINEAR",
    "MARGIN_MODELS",
    "OPEN_INTEREST_CATEGORIES",
    "PERPETUAL",
    "DeribitAccountSummaries",
    "DeribitAuthError",
    "DeribitBook",
    "DeribitBookSnapshot",
    "DeribitError",
    "DeribitFill",
    "DeribitOrderAck",
    "DeribitOrderBook",
    "DeribitOrderUpdate",
    "DeribitPosition",
    "DeribitPrivateClient",
    "DeribitPrivateStream",
    "DeribitPublicClient",
    "DeribitPublicRest",
    "DeribitPublicStream",
    "DeribitPublicTrade",
    "DeribitQuote",
    "DeribitResponse",
    "DeribitRestError",
    "DeribitSocket",
    "DeribitSummary",
    "DeribitTicker",
    "DeribitWsError",
    "auth_params",
    "category_from_instrument",
    "category_of",
    "expiry_code_from_name",
    "is_cbe_routed",
    "is_dated_future",
    "is_inverse_perp",
    "is_inverse_perp_name",
    "is_linear_perp",
    "is_linear_perp_name",
    "kind_of",
    "kline_from_chart",
    "kline_from_tick",
    "order_book_from_result",
    "sign_rest",
    "sign_ws",
    "status_of",
    "type_of",
    "venue_interval",
]

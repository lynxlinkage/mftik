"""Gate spot WebSocket v4 adapter.

One client for the whole venue surface — Gate serves market data, account
pushes and order entry from the same URL with the same envelope, so there is
no public/private split here. See :class:`GateSpotWebSocket`.
"""

from mft.exchange.gate.spot import channels
from mft.exchange.gate.spot.client import GATE_SPOT_WS_URL, GateSpotWebSocket
from mft.exchange.gate.spot.models import (
    TEXT_PREFIX,
    GateBalance,
    GateBookTicker,
    GateCandlestick,
    GateMessage,
    GateOrderAck,
    GateOrderBook,
    GateOrderBookUpdate,
    GateOrderUpdate,
    GateTicker,
    GateTrade,
    GateUserTrade,
    from_text,
    to_text,
)
from mft.exchange.gate.spot.private import GateSpotPrivateClient
from mft.exchange.gate.spot.protocol import (
    GateApiError,
    GateResponse,
    GateWsError,
    api_frame,
    api_sign,
    ping_frame,
    request_frame,
    sign,
)
from mft.exchange.gate.spot.rest import (
    GATE_SPOT_REST_URL,
    GateRestError,
    GateSpotRest,
    sign_rest,
)

__all__ = [
    "GATE_SPOT_REST_URL",
    "GATE_SPOT_WS_URL",
    "TEXT_PREFIX",
    "GateApiError",
    "GateBalance",
    "GateBookTicker",
    "GateCandlestick",
    "GateMessage",
    "GateOrderAck",
    "GateOrderBook",
    "GateOrderBookUpdate",
    "GateOrderUpdate",
    "GateResponse",
    "GateRestError",
    "GateSpotPrivateClient",
    "GateSpotRest",
    "GateSpotWebSocket",
    "GateTicker",
    "GateTrade",
    "GateUserTrade",
    "GateWsError",
    "api_frame",
    "api_sign",
    "channels",
    "from_text",
    "ping_frame",
    "request_frame",
    "sign",
    "sign_rest",
    "to_text",
]

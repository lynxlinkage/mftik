"""Gate spot WebSocket v4 framing — re-export of the shared envelope.

The HMAC, the two request styles and :class:`GateResponse` live in
:mod:`mftik.exchange.gate.protocol` so the futures plane can share them.
``login_frame`` and ``ping_frame`` still default to the spot channel names.
"""

from mftik.exchange.gate.protocol import (
    GateApiError,
    GateResponse,
    GateRestError,
    GateWsError,
    api_frame,
    api_sign,
    login_frame,
    ping_frame,
    request_frame,
    session_api_frame,
    sign,
    sign_rest,
)

__all__ = [
    "GateApiError",
    "GateResponse",
    "GateRestError",
    "GateWsError",
    "api_frame",
    "api_sign",
    "login_frame",
    "ping_frame",
    "request_frame",
    "session_api_frame",
    "sign",
    "sign_rest",
]

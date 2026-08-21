"""Gate USDT-perpetual hosts and the shared v4 envelope.

REST stays on ``api.gateio.ws`` with ``/futures/usdt/...`` paths. The
WebSocket is a different host — ``fx-ws`` with ``/v4/ws/usdt`` — and the
unsettled URL would silently land on BTC-settled contracts.
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

GATE_FUTURES_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
GATE_FUTURES_REST_URL = "https://api.gateio.ws"
SETTLE = "usdt"
API_PREFIX = "/api/v4"

#: Sent on every WebSocket so size fields stay decimal strings.
SIZE_DECIMAL_HEADER = {"X-Gate-Size-Decimal": "1"}

__all__ = [
    "API_PREFIX",
    "GATE_FUTURES_REST_URL",
    "GATE_FUTURES_WS_URL",
    "SETTLE",
    "SIZE_DECIMAL_HEADER",
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

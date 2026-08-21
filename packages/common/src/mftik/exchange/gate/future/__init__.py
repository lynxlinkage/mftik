"""Gate USDT-perpetual adapter — one venue, one socket.

Registered as ``GateFutures`` and addressed as ``GateFutures_Perp_BTCUSDT``.
A venue of its own rather than a category of ``Gate``: separate host, separate
credential, separate wallet. See :mod:`mftik.exchange.venues`.

It shares HMAC-SHA512 framing with the spot adapter
(:mod:`mftik.exchange.gate.protocol`) and almost nothing else. Where it
differs:

* The WebSocket host is ``wss://fx-ws.gateio.ws/v4/ws/usdt``, not the spot
  v4 URL. The unsettled path would silently trade BTC-settled contracts.
* Private channel payloads need the ``uid`` from ``futures.login``.
* Size on the wire is contracts. The connectors convert to base using
  ``contract_size`` from the symbol plane.
* Positions and public liquidations exist.

:class:`GateFuturesPublicClient` is what MD composes;
:class:`GateFuturesPrivateClient` is what TD composes.
"""

from mftik.exchange.gate.future.client import (
    GATE_FUTURES_WS_URL,
    GateFuturesWebSocket,
)
from mftik.exchange.gate.future.models import (
    GateFuturesBalance,
    GateFuturesBookTicker,
    GateFuturesCandlestick,
    GateFuturesLiquidation,
    GateFuturesOrder,
    GateFuturesOrderBook,
    GateFuturesPosition,
    GateFuturesTicker,
    GateFuturesTrade,
    GateFuturesUserTrade,
    base_to_contracts,
    contracts_to_base,
    from_text,
    signed_contracts,
    to_text,
)
from mftik.exchange.gate.future.private import GateFuturesPrivateClient
from mftik.exchange.gate.future.protocol import (
    GATE_FUTURES_REST_URL,
    SETTLE,
    GateApiError,
    GateRestError,
    GateWsError,
)
from mftik.exchange.gate.future.public import (
    GATE_FUTURES_INTERVALS,
    GateFuturesPublicClient,
    venue_interval,
)
from mftik.exchange.gate.future.rest import (
    GateFuturesPublicRest,
    GateFuturesRest,
)

__all__ = [
    "GATE_FUTURES_INTERVALS",
    "GATE_FUTURES_REST_URL",
    "GATE_FUTURES_WS_URL",
    "SETTLE",
    "GateApiError",
    "GateFuturesBalance",
    "GateFuturesBookTicker",
    "GateFuturesCandlestick",
    "GateFuturesLiquidation",
    "GateFuturesOrder",
    "GateFuturesOrderBook",
    "GateFuturesPosition",
    "GateFuturesPrivateClient",
    "GateFuturesPublicClient",
    "GateFuturesPublicRest",
    "GateFuturesRest",
    "GateFuturesTicker",
    "GateFuturesTrade",
    "GateFuturesUserTrade",
    "GateFuturesWebSocket",
    "GateRestError",
    "GateWsError",
    "base_to_contracts",
    "contracts_to_base",
    "from_text",
    "signed_contracts",
    "to_text",
    "venue_interval",
]

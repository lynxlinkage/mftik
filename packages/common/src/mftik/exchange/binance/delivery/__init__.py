"""Binance COIN-M adapter — listing and public REST in this slice.

Registered as the venue ``BinanceDelivery`` and addressed as
``BinanceDelivery_Perp_BTCUSD``. A venue of its own rather than a category of
spot or of ``BinanceFuture``: separate hosts (``dapi`` / ``ws-dapi`` /
``dstream``), separate wallet, separate API key.

It shares framing and REST transport with the other Binance products
(:mod:`mftik.exchange.binance.protocol`, :mod:`mftik.exchange.binance.rest`)
and imports nothing from :mod:`mftik.exchange.binance.future`. USD-M models
assume linear quantity and a ``status`` field; dapi uses ``contractStatus``
and sizes in contracts worth a fixed amount of quote.

``contract_size`` on the listed row is that quote amount (USD per contract),
not base per contract. Gate/OKX converters must not see it.
"""

from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_REST_TESTNET_URL,
    BINANCE_DELIVERY_REST_URL,
    BINANCE_DELIVERY_STREAM_TESTNET_URL,
    BINANCE_DELIVERY_STREAM_URL,
    BINANCE_DELIVERY_WS_API_TESTNET_URL,
    BINANCE_DELIVERY_WS_API_URL,
)
from mftik.exchange.binance.delivery.rest import (
    BinanceDeliveryPublicRest,
    BinanceDeliveryRestError,
)

__all__ = [
    "BINANCE_DELIVERY_REST_TESTNET_URL",
    "BINANCE_DELIVERY_REST_URL",
    "BINANCE_DELIVERY_STREAM_TESTNET_URL",
    "BINANCE_DELIVERY_STREAM_URL",
    "BINANCE_DELIVERY_WS_API_TESTNET_URL",
    "BINANCE_DELIVERY_WS_API_URL",
    "BinanceDeliveryPublicRest",
    "BinanceDeliveryRestError",
]

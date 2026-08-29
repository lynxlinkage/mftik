"""Binance COIN-M adapter — listing, public REST, dstream, ws-dapi, listen-key.

Registered as the venue ``BinanceDelivery`` and addressed as
``BinanceDelivery_Inverse_BTCUSD``. Inverse is the product — not a kind of
linear perp, and not a category of spot or of ``BinanceFuture``. Separate
hosts (``dapi`` / ``ws-dapi`` / ``dstream``), separate wallet, separate API
key.

It shares framing and REST/socket transport with the other Binance products
(:mod:`mftik.exchange.binance.protocol`, :mod:`mftik.exchange.binance.rest`,
:mod:`mftik.exchange.binance.feed`) and imports nothing from
:mod:`mftik.exchange.binance.future`. USD-M models assume linear quantity
and a ``status`` field; dapi uses ``contractStatus`` and sizes in contracts
worth a fixed amount of quote.

``contract_size`` on the listed row is that quote amount (USD per contract),
not base per contract. Gate/OKX converters must not see it. Book, tape and
liquidation quantities stay in contracts; only klines use the size, as
``quote_per_contract``.
"""

from mftik.exchange.binance.delivery.client import BinanceDeliveryWsApi
from mftik.exchange.binance.delivery.feed import BinanceDeliveryStream
from mftik.exchange.binance.delivery.private import BinanceDeliveryPrivateClient
from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_PRIVATE_STREAM_URL,
    BINANCE_DELIVERY_REST_TESTNET_URL,
    BINANCE_DELIVERY_REST_URL,
    BINANCE_DELIVERY_STREAM_TESTNET_URL,
    BINANCE_DELIVERY_STREAM_URL,
    BINANCE_DELIVERY_WS_API_TESTNET_URL,
    BINANCE_DELIVERY_WS_API_URL,
)
from mftik.exchange.binance.delivery.public import BinanceDeliveryPublicClient
from mftik.exchange.binance.delivery.rest import (
    BinanceDeliveryPublicRest,
    BinanceDeliveryRest,
    BinanceDeliveryRestError,
)
from mftik.exchange.binance.delivery.user import BinanceDeliveryUserStream

__all__ = [
    "BINANCE_DELIVERY_PRIVATE_STREAM_URL",
    "BINANCE_DELIVERY_REST_TESTNET_URL",
    "BINANCE_DELIVERY_REST_URL",
    "BINANCE_DELIVERY_STREAM_TESTNET_URL",
    "BINANCE_DELIVERY_STREAM_URL",
    "BINANCE_DELIVERY_WS_API_TESTNET_URL",
    "BINANCE_DELIVERY_WS_API_URL",
    "BinanceDeliveryPrivateClient",
    "BinanceDeliveryPublicClient",
    "BinanceDeliveryPublicRest",
    "BinanceDeliveryRest",
    "BinanceDeliveryRestError",
    "BinanceDeliveryStream",
    "BinanceDeliveryUserStream",
    "BinanceDeliveryWsApi",
]

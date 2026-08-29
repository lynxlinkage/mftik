"""Binance COIN-M (dapi) endpoints, on the venue-wide framing.

This is Binance's historical "delivery" plane: coin-margined perpetuals and
dated contracts, registered as the venue ``BinanceDelivery``. Framing, signing
and the error block are the same as spot and USD-M — they live in
:mod:`mftik.exchange.binance.protocol`. What is this market's own is *where
the frames go*.

**REST is the listing and candle path.** ``exchangeInfo`` and ``klines`` have
no WebSocket API method on dapi either. The host is ``dapi.binance.com``,
prefix ``/dapi/v1``.

**Sockets are the live market path.** Unlike USD-M's 2026 ``fstream``
split, coin-margined market data still answers on one combined stream
(``dstream.binance.com/stream``). The WebSocket API is
``ws-dapi.binance.com`` and is unused until the private client lands.
"""

from __future__ import annotations

#: Request/reply — unused until the private client lands. Kept next to the
#: REST URL so the three hosts stay one module.
BINANCE_DELIVERY_WS_API_URL = "wss://ws-dapi.binance.com/ws-dapi/v1"

#: Combined market stream. dapi was not part of the 2026 ``/public`` /
#: ``/market`` / ``/private`` split that USD-M took.
BINANCE_DELIVERY_STREAM_URL = "wss://dstream.binance.com/stream"

#: REST — instrument listing, candles, ticker and depth.
BINANCE_DELIVERY_REST_URL = "https://dapi.binance.com"

#: Testnet shares the USD-M test host; the path, not the host, is dapi.
BINANCE_DELIVERY_WS_API_TESTNET_URL = "wss://testnet.binancefuture.com/ws-dapi/v1"
BINANCE_DELIVERY_STREAM_TESTNET_URL = "wss://dstream.binancefuture.com/stream"
BINANCE_DELIVERY_REST_TESTNET_URL = "https://testnet.binancefuture.com"


__all__ = [
    "BINANCE_DELIVERY_REST_TESTNET_URL",
    "BINANCE_DELIVERY_REST_URL",
    "BINANCE_DELIVERY_STREAM_TESTNET_URL",
    "BINANCE_DELIVERY_STREAM_URL",
    "BINANCE_DELIVERY_WS_API_TESTNET_URL",
    "BINANCE_DELIVERY_WS_API_URL",
]

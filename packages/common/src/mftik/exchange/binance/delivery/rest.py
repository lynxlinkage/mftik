"""Binance COIN-M public REST — listing, candles, ticker and depth.

dapi has no ``exchangeInfo`` or ``klines`` on its WebSocket API, same as
USD-M. This client is the public half only; signed recon reads wait for the
private connector.

``contractSize`` is USD per contract. :meth:`fetch_klines` therefore takes
``quote_per_contract`` and will not guess — passing that number to a Gate/OKX
``contracts_to_base`` helper would invent a base quantity off a quote unit.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from mftik.exchange.binance.delivery.listing import to_listed
from mftik.exchange.binance.delivery.models import BinanceDeliveryDepth
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.models import kline_from_row, secs
from mftik.exchange.binance.rest import BinanceRestError, BinanceRestTransport
from mftik.exchange.models import Kline, OrderBook, Ticker
from mftik.exchange.tickers import UniversalTicker
from mftik.symbols.listed import ListedInstrument

API_PREFIX = "/dapi/v1"

#: Most candles ``/dapi/v1/klines`` returns in one call. Asking for more is a
#: 400, not a truncated answer.
MAX_KLINES = 1500


class BinanceDeliveryRestError(BinanceRestError):
    """A non-2xx answer from Binance's coin-margined REST API."""


class BinanceDeliveryPublicRest(BinanceRestTransport):
    """The public reads dapi serves over HTTP."""

    default_base_url = BINANCE_DELIVERY_REST_URL
    error_type = BinanceDeliveryRestError

    async def fetch_instruments(self) -> list[ListedInstrument]:
        """``GET /dapi/v1/exchangeInfo`` — every live perpetual."""
        payload = await self._get(f"{API_PREFIX}/exchangeInfo")
        out: list[ListedInstrument] = []
        for row in (payload or {}).get("symbols") or []:
            listed = to_listed(row)
            if listed is not None and listed.is_active:
                out.append(listed)
        return out

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        quote_per_contract: Decimal,
        limit: int = 100,
    ) -> list[Kline]:
        """``GET /dapi/v1/klines`` — recent candles, oldest first.

        ``quote_per_contract`` is the row's ``contractSize``. Required so a
        linear read of a dapi bar cannot land here by omitting the argument.
        """
        rows = await self._get(
            f"{API_PREFIX}/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": min(limit, MAX_KLINES),
            },
        )
        return [
            kline_from_row(
                row, ticker, interval, quote_per_contract=quote_per_contract
            )
            for row in rows or []
        ]

    async def fetch_ticker(self, symbol: str, *, ticker: UniversalTicker) -> Ticker:
        """Last price and quote — two endpoints, the same split as USD-M.

        Asking by ``pair`` returns an array (perp plus dated). ``_first``
        keeps that off every caller.
        """
        stats = await self._get(f"{API_PREFIX}/ticker/24hr", {"symbol": symbol})
        quote = await self._get(f"{API_PREFIX}/ticker/bookTicker", {"symbol": symbol})
        row = _first(stats)
        book = _first(quote)
        return Ticker(
            universal_ticker=str(ticker),
            bid=Decimal(str(book.get("bidPrice", "0") or "0")),
            ask=Decimal(str(book.get("askPrice", "0") or "0")),
            last=Decimal(str(row.get("lastPrice", "0") or "0")),
            ts=secs(row.get("closeTime") or book.get("time") or 0),
        )

    async def fetch_order_book(
        self, symbol: str, *, ticker: UniversalTicker, depth: int = 100
    ) -> OrderBook:
        """``GET /dapi/v1/depth`` — a whole book, capped at ``depth``."""
        payload = await self._get(
            f"{API_PREFIX}/depth", {"symbol": symbol, "limit": depth}
        )
        return BinanceDeliveryDepth.model_validate(payload or {}).to_order_book(
            ticker
        )


def _first(payload: Any) -> dict[str, Any]:
    """One row, whether Binance answered with an object or a one-item array."""
    if isinstance(payload, list):
        return dict(payload[0]) if payload else {}
    return dict(payload or {})


__all__ = [
    "API_PREFIX",
    "MAX_KLINES",
    "BinanceDeliveryPublicRest",
    "BinanceDeliveryRestError",
]

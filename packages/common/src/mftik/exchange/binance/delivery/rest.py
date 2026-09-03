"""Binance COIN-M REST — public reads, open orders, and history.

dapi has no ``exchangeInfo`` or ``klines`` on its WebSocket API, same as
USD-M. It also has no ``openOrders.status``: recon asks
``GET /dapi/v1/openOrders`` over signed REST. History
(``userTrades`` / ``allOrders``) is here for the same reason as USD-M: a
batch job should not open an authenticated socket to walk a window.

``contractSize`` is USD per contract. :meth:`fetch_klines` therefore takes
``quote_per_contract`` and will not guess — passing that number to a Gate/OKX
``contracts_to_base`` helper would invent a base quantity off a quote unit.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from mftik.exchange.binance.delivery.listing import to_listed
from mftik.exchange.binance.delivery.models import (
    BinanceDeliveryDepth,
    BinanceDeliveryMyTrade,
    BinanceDeliveryOrderAck,
)
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_REST_URL
from mftik.exchange.binance.models import kline_from_row, secs
from mftik.exchange.binance.rest import (
    BinanceRestError,
    BinanceRestTransport,
    BinanceSignedRest,
)
from mftik.exchange.models import FundingRate, Kline, OpenInterest, OrderBook, Ticker
from mftik.exchange.tickers import UniversalTicker
from mftik.symbols.listed import ListedInstrument

API_PREFIX = "/dapi/v1"

#: Most candles ``/dapi/v1/klines`` returns in one call. Asking for more is a
#: 400, not a truncated answer.
MAX_KLINES = 1500

#: Most history rows ``userTrades`` returns in one call.
MAX_HISTORY = 1000

#: ``allOrders`` caps far lower than ``userTrades`` on dapi — 100, where the
#: USD-M endpoint of the same name takes 1000. Asking for more is a ``-1130``,
#: not a truncated page, so the walk has to ask for the smaller number.
MAX_ORDERS = 100


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

    async def fetch_funding_history(
        self, symbol: str, *, ticker: UniversalTicker, limit: int = 100
    ) -> list[FundingRate]:
        """``GET /dapi/v1/fundingRate`` — settled rates, already oldest first."""
        rows = await self._get(
            f"{API_PREFIX}/fundingRate",
            {"symbol": symbol, "limit": min(limit, MAX_HISTORY)},
        )
        return [
            FundingRate(
                universal_ticker=str(ticker),
                rate=Decimal(str(row.get("fundingRate") or "0")),
                ts=secs(row.get("fundingTime")),
            )
            for row in rows or []
        ]

    async def fetch_open_interest(
        self, symbol: str, *, ticker: UniversalTicker
    ) -> OpenInterest:
        """``GET /dapi/v1/openInterest`` — current size, in contracts.

        Absent means the read failed, not that nothing is open. Binance
        answers a refusal with 4xx and a ``code``, which the transport
        raises on; what reaches here without the field is a 2xx whose body
        did not parse as JSON, and :meth:`BinanceRestTransport._parse`
        hands that back as ``None``. Defaulting it to zero would publish a
        print indistinguishable from the real zero a newly listed contract
        has — and ``MdOpenInterestResult`` says an ``ok`` zero is a real
        answer.
        """
        row = await self._get(f"{API_PREFIX}/openInterest", {"symbol": symbol})
        if not isinstance(row, dict) or row.get("openInterest") in (None, ""):
            raise BinanceDeliveryRestError(
                200, None, f"no openInterest for {symbol}"
            )
        stamp = secs(row.get("time"))
        fields: dict[str, Any] = {} if stamp <= 0 else {"ts": stamp}
        return OpenInterest(
            universal_ticker=str(ticker),
            qty=Decimal(str(row["openInterest"])),
            **fields,
        )


class BinanceDeliveryRest(BinanceSignedRest):
    """Signed reads dapi has nowhere else, or wants off a socket.

    ``openOrders`` has no WebSocket method. The history pair does, but a
    batch job walking a window should not build an authenticated session
    to ask it. Order entry stays on the WebSocket API.
    """

    default_base_url = BINANCE_DELIVERY_REST_URL
    error_type = BinanceDeliveryRestError

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[BinanceDeliveryOrderAck]:
        """``GET /dapi/v1/openOrders`` — one symbol's resting orders, or all.

        Asking across the account is heavily weighted by Binance (it scans
        every symbol), so pass a symbol when one is known.
        """
        rows = await self._signed_get(
            f"{API_PREFIX}/openOrders", {"symbol": symbol} if symbol else {}
        )
        return [BinanceDeliveryOrderAck.model_validate(row) for row in rows or []]

    async def fetch_my_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        start_time: int | None = None,
        limit: int = MAX_HISTORY,
    ) -> list[BinanceDeliveryMyTrade]:
        """``GET /dapi/v1/userTrades`` — this account's executions, oldest first.

        Per-symbol and paginated by trade id. ``qty`` is contracts. Time
        opens a walk that has no id to resume from and nothing else.
        """
        if from_id is not None and start_time is not None:
            raise ValueError(
                "pass from_id or start_time, not both: Binance ignores the "
                "range when fromId is set"
            )
        rows = await self._signed_get(
            f"{API_PREFIX}/userTrades",
            {
                "symbol": symbol,
                "fromId": from_id,
                "startTime": start_time,
                "limit": min(limit, MAX_HISTORY),
            },
        )
        return [BinanceDeliveryMyTrade.model_validate(row) for row in rows or []]

    async def fetch_orders(
        self,
        symbol: str,
        *,
        from_order_id: int | None = None,
        start_time: int | None = None,
        limit: int = MAX_ORDERS,
    ) -> list[BinanceDeliveryOrderAck]:
        """``GET /dapi/v1/allOrders`` — every order on ``symbol``, open or not.

        A trade row carries no client order id, so this is the only read
        that can tie an execution back to the session that placed it.
        """
        if from_order_id is not None and start_time is not None:
            raise ValueError(
                "pass from_order_id or start_time, not both: Binance ignores "
                "the range when orderId is set"
            )
        rows = await self._signed_get(
            f"{API_PREFIX}/allOrders",
            {
                "symbol": symbol,
                "orderId": from_order_id,
                "startTime": start_time,
                "limit": min(limit, MAX_ORDERS),
            },
        )
        return [BinanceDeliveryOrderAck.model_validate(row) for row in rows or []]


def _first(payload: Any) -> dict[str, Any]:
    """One row, whether Binance answered with an object or a one-item array."""
    if isinstance(payload, list):
        return dict(payload[0]) if payload else {}
    return dict(payload or {})


__all__ = [
    "API_PREFIX",
    "MAX_HISTORY",
    "MAX_KLINES",
    "MAX_ORDERS",
    "BinanceDeliveryPublicRest",
    "BinanceDeliveryRest",
    "BinanceDeliveryRestError",
]

"""Binance futures REST — only what the WebSocket API cannot answer.

Futures needs REST for reads ``ws-fapi`` has no method for, and the gaps are
not small ones:

* :class:`BinanceFuturePublicRest` — **candles and the instrument listing.**
  ``klines`` and ``exchangeInfo`` are not WebSocket API methods on this market
  at all, and the kline *stream* pushes the window in progress and nothing
  before it, so history has to be asked for.
* :class:`BinanceFutureRest` — **"what is open right now".** Futures has no
  ``openOrders.status``; the WebSocket API can only be asked about one order it
  is given the id of. Recon needs the list at attach time, and answering it
  with nothing would leave the OMS believing the account is flat, which is
  worse than not reconciling at all.

The transport and the per-call signature are the venue's, not this product's,
and live in :mod:`mftik.exchange.binance.rest`. What is here is the futures host,
the futures paths and the futures models.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from mftik.exchange.binance.future.models import (
    BinanceFutureDepth,
    BinanceFutureMyTrade,
    BinanceFutureOrderAck,
    BinanceFutureSymbolConfig,
    instrument_from_row,
    kline_from_row,
)
from mftik.exchange.binance.future.protocol import BINANCE_FUTURE_REST_URL
from mftik.exchange.binance.models import secs
from mftik.exchange.binance.rest import (
    BinanceRestError,
    BinanceRestTransport,
    BinanceSignedRest,
)
from mftik.exchange.models import Instrument, Kline, OrderBook, Ticker
from mftik.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

API_PREFIX = "/fapi/v1"

#: Most candles ``/fapi/v1/klines`` returns in one call. Asking for more is a
#: 400, not a truncated answer.
MAX_KLINES = 1500

#: Most history rows ``userTrades`` / ``allOrders`` return in one call.
MAX_HISTORY = 1000

#: The only ``status`` that means a contract can be traded right now.
TRADING = "TRADING"

#: The only ``contractType`` this venue trades here. ``exchangeInfo`` also
#: lists dated futures (``BTCUSDT_250926``), which are different instruments
#: that canonicalize onto the perpetual's symbol — see
#: :mod:`mftik_sym.sources.binance_future` for what that would collide with.
PERPETUAL = "PERPETUAL"


class BinanceFutureRestError(BinanceRestError):
    """A non-2xx answer from Binance's futures REST API."""


class BinanceFuturePublicRest(BinanceRestTransport):
    """The two public reads the futures WebSocket API does not serve."""

    default_base_url = BINANCE_FUTURE_REST_URL
    error_type = BinanceFutureRestError

    async def fetch_instruments(self) -> list[Instrument]:
        """``GET /fapi/v1/exchangeInfo`` — every perpetual, in Binance's spelling.

        Left native on purpose: this is what the symbol plane ingests to
        *build* the canonical mapping, so it cannot depend on that mapping
        existing.

        Dated futures are dropped. They are listed on the same endpoint and
        their base and quote are the perpetual's, so ``BTCUSDT_250926`` and
        ``BTCUSDT`` canonicalize to one symbol — keeping both would mean one
        overwriting the other and orders for the perp routing to a contract
        that expires.
        """
        payload = await self._get(f"{API_PREFIX}/exchangeInfo")
        return [
            instrument_from_row(row)
            for row in (payload or {}).get("symbols") or []
            if row.get("status") == TRADING
            and str(row.get("contractType") or "") == PERPETUAL
        ]

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        *,
        ticker: UniversalTicker,
        limit: int = 100,
    ) -> list[Kline]:
        """``GET /fapi/v1/klines`` — recent candles, oldest first.

        ``interval`` is Binance's own spelling; translating from the canonical
        one happens a layer up, in
        :class:`~mftik.exchange.binance.future.public.BinanceFuturePublicClient`.
        """
        rows = await self._get(
            f"{API_PREFIX}/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": min(limit, MAX_KLINES),
            },
        )
        return [kline_from_row(row, ticker, interval) for row in rows or []]

    async def fetch_ticker(self, symbol: str, *, ticker: UniversalTicker) -> Ticker:
        """Last price and quote — two endpoints, because futures splits them.

        ``/ticker/24hr`` carries the rolling stats and the last price but no
        bid or ask, and ``/ticker/bookTicker`` carries the quote and no last
        price. Both are asked rather than one being derived: a mid price is not
        a last price, and a last price is not a quote.
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
        """``GET /fapi/v1/depth`` — a whole book, capped at ``depth``.

        The same book the WebSocket API's ``depth`` answers. It is here as well
        because the fetch plane holds this client and not a socket, and opening
        one to ask a single question is the coupling that plane exists to
        avoid.
        """
        payload = await self._get(
            f"{API_PREFIX}/depth", {"symbol": symbol, "limit": depth}
        )
        return BinanceFutureDepth.model_validate(payload or {}).to_order_book(ticker)


class BinanceFutureRest(BinanceSignedRest):
    """The signed reads futures has nowhere else, or wants off a socket.

    Two kinds, and they are here for different reasons. ``openOrders`` and
    ``symbolConfig`` have no WebSocket API method at all, and recon needs them
    at attach time. The history pair does have socket equivalents and is here
    anyway: it is read by a batch job on a schedule, where building and tearing
    down an authenticated session to ask three questions costs more and fails
    in more ways than three HTTP GETs — the same argument spot's history client
    is built on.

    Order entry stays on the WebSocket API regardless: one authenticated
    connection and no per-call signature.
    """

    default_base_url = BINANCE_FUTURE_REST_URL
    error_type = BinanceFutureRestError

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[BinanceFutureOrderAck]:
        """``GET /fapi/v1/openOrders`` — one symbol's resting orders, or all.

        Asking across the account is heavily weighted by Binance (it scans
        every symbol), so pass a symbol when one is known.
        """
        rows = await self._signed_get(
            f"{API_PREFIX}/openOrders", {"symbol": symbol} if symbol else {}
        )
        return [BinanceFutureOrderAck.model_validate(row) for row in rows or []]

    async def fetch_my_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        start_time: int | None = None,
        limit: int = MAX_HISTORY,
    ) -> list[BinanceFutureMyTrade]:
        """``GET /fapi/v1/userTrades`` — this account's executions, oldest first.

        Per-symbol and paginated by trade id, the same shape spot uses and for
        the same reasons: ids are monotonic per symbol with no window cap, and
        a time range cannot separate two trades inside one millisecond. Time
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
        return [BinanceFutureMyTrade.model_validate(row) for row in rows or []]

    async def fetch_orders(
        self,
        symbol: str,
        *,
        from_order_id: int | None = None,
        start_time: int | None = None,
        limit: int = MAX_HISTORY,
    ) -> list[BinanceFutureOrderAck]:
        """``GET /fapi/v1/allOrders`` — every order on ``symbol``, open or not.

        Not optional beside :meth:`fetch_my_trades`: a trade row carries no
        client order id, so this is the only read that can tie an execution
        back to the session that placed it. It returns orders this platform
        never sent as well, which is how an account's manual activity stops
        being invisible.
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
                "limit": min(limit, MAX_HISTORY),
            },
        )
        return [BinanceFutureOrderAck.model_validate(row) for row in rows or []]

    async def fetch_symbol_config(
        self, symbol: str | None = None
    ) -> list[BinanceFutureSymbolConfig]:
        """``GET /fapi/v1/symbolConfig`` — margin type and configured leverage.

        Answers for flat symbols too. ``positionRisk`` v3 does not — it only
        returns symbols with a position or resting order — so leverage for a
        book about to quote has to come from here.
        """
        rows = await self._signed_get(
            f"{API_PREFIX}/symbolConfig",
            {"symbol": symbol} if symbol else {},
        )
        return [
            BinanceFutureSymbolConfig.model_validate(row) for row in rows or []
        ]


def _first(payload: Any) -> dict[str, Any]:
    """One row, whether Binance answered with an object or a one-item array.

    The ticker endpoints answer an array when asked for every symbol and a
    bare object when asked for one; reading both shapes here keeps that off
    every caller.
    """
    if isinstance(payload, list):
        return dict(payload[0]) if payload else {}
    return dict(payload or {})


__all__ = [
    "API_PREFIX",
    "MAX_KLINES",
    "PERPETUAL",
    "TRADING",
    "BinanceFuturePublicRest",
    "BinanceFutureRest",
    "BinanceFutureRestError",
]

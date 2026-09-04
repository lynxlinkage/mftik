"""The Binance COIN-M market-data connector.

Not an implementation of a shared public interface — there is none; see
:mod:`mftik.exchange.base`. The methods here resemble the USD-M connector's
because the same questions get asked of every venue, and where dapi differs
the difference shows up in this file rather than being flattened away.

Composes two transports on one market:

* :class:`~mftik.exchange.binance.delivery.feed.BinanceDeliveryStream` — every
  live feed, on the single ``dstream`` combined socket. dapi was not part of
  the 2026 ``fstream`` split.
* :class:`~mftik.exchange.binance.delivery.rest.BinanceDeliveryPublicRest` —
  the on-demand reads. REST, because dapi's WebSocket API serves no
  ``klines`` and no ``exchangeInfo``.

Three differences from USD-M are worth knowing before reading the streams:

* **Quantities on the book, tape and liquidations stay in contracts.**
  ``contractSize`` is USD per contract; multiplying by it invents a dollar
  notional, not BTC.
* **Klines need that size.** REST ``[5]`` / WS ``k.v`` count contracts.
  :meth:`fetch_klines` and :meth:`stream_kline` read ``contract_size`` off
  the plane as ``quote_per_contract`` and refuse rather than guess.
* **The 24h ticker still carries no quote**, so :meth:`stream_ticker` reads
  ``@bookTicker`` alongside it. Both subscriptions land on the **same**
  socket.

Symbols cross this boundary canonical (``BTCUSD``) and are resolved to
Binance's spelling (``BTCUSD_PERP``) through the symbol plane, never by
string surgery — see :mod:`mftik.exchange.symbols`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, TypeVar

from mftik.exchange.base import BaseClient
from mftik.exchange.binance.delivery.feed import (
    DEFAULT_BOOK_LEVELS,
    DEFAULT_BOOK_SPEED,
    BinanceDeliveryStream,
)
from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_REST_URL,
    BINANCE_DELIVERY_STREAM_URL,
)
from mftik.exchange.binance.delivery.rest import BinanceDeliveryPublicRest
from mftik.exchange.intervals import InvalidIntervalError, normalize_interval
from mftik.exchange.models import (
    AggTrade,
    BestQuote,
    FundingRate,
    Kline,
    Liquidation,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.stream import EventStream
from mftik.exchange.symbols import SymbolResolver
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Categories that pay a funding hook. A dated future settles at expiry;
#: ``@markPrice`` may still push, but without ``r``, so a subscribe would
#: open a pump that never yields. Inverse (the coin-margined perp) funds.
FUNDING_CATEGORIES = frozenset({Category.INVERSE})

#: Canonical interval → Binance's COIN-M spelling.
#:
#: The USD-M table, copied rather than imported: this package does not
#: depend on :mod:`mftik.exchange.binance.future`. dapi serves **no
#: one-second candles**. ``1M`` for the month is Binance's spelling of the
#: one window :mod:`mftik.exchange.intervals` refuses to accept from above.
BINANCE_DELIVERY_INTERVALS: dict[str, str] = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "1mo": "1M",
}


def venue_interval(interval: str) -> str:
    """Canonical interval → Binance COIN-M's, or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = BINANCE_DELIVERY_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"Binance delivery serves no {canonical} candles; "
            f"supported: {sorted(BINANCE_DELIVERY_INTERVALS)}"
        )
    return found


class BinanceDeliveryPublicClient(BaseClient):
    """Binance COIN-M market data for MD.

    All six feeds are live — the five every venue has, plus liquidations::

        client = BinanceDeliveryPublicClient(symbols=symbol_client)
        async with client:
            ticker = UniversalTicker.parse("BinanceDelivery_Inverse_BTCUSD")
            async for book in client.stream_order_book(ticker):
                ...

    Reads take a :class:`~mftik.exchange.tickers.UniversalTicker` rather than
    a symbol, as every connector here does.
    """

    name = "BinanceDelivery"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        stream_url: str = BINANCE_DELIVERY_STREAM_URL,
        rest_url: str = BINANCE_DELIVERY_REST_URL,
        feed: BinanceDeliveryStream | None = None,
        rest: BinanceDeliveryPublicRest | None = None,
        book_levels: int = DEFAULT_BOOK_LEVELS,
        book_speed: str = DEFAULT_BOOK_SPEED,
    ) -> None:
        super().__init__()
        self.feed = feed or BinanceDeliveryStream(url=stream_url)
        self.rest = rest or BinanceDeliveryPublicRest(base_url=rest_url)
        self.symbols = symbols
        self.book_levels = book_levels
        self.book_speed = book_speed

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.feed.connect()
        await self.rest.connect()
        self._connected = True
        logger.info("BinanceDelivery public connected")

    async def close(self) -> None:
        self._connected = False
        await self.feed.close()
        await self.rest.close()

    # --- snapshots (REST) --------------------------------------------------

    async def fetch_instruments(self):
        """Tradeable perpetuals, mapped for the plane."""
        self._ensure_connected()
        return await self.rest.fetch_instruments()

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        native = await self._resolve(ticker)
        return await self.rest.fetch_ticker(native, ticker=ticker)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 10
    ) -> OrderBook:
        self._ensure_connected()
        native = await self._resolve(ticker)
        return await self.rest.fetch_order_book(native, ticker=ticker, depth=depth)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int = 100
    ) -> list[Kline]:
        """Recent candles, oldest first, in canonical symbol and interval.

        Refuses when the plane has no ``contract_size``: dapi's volume
        columns cannot be read as a linear bar.
        """
        self._ensure_connected()
        canonical = normalize_interval(interval)
        native_interval = venue_interval(canonical)
        native = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            native,
            native_interval,
            ticker=ticker,
            quote_per_contract=await self._quote_per_contract(ticker),
            limit=limit,
        )
        return [kline.model_copy(update={"interval": canonical}) for kline in klines]

    # --- streams -----------------------------------------------------------

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        self._ensure_connected()
        return self._tickers(ticker)

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        self._ensure_connected()
        return self._trades(ticker)

    def stream_agg_trades(self, ticker: UniversalTicker) -> AsyncIterator[AggTrade]:
        self._ensure_connected()
        return self._agg_trades(ticker)

    def stream_order_book(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        return self._order_books(ticker)

    def stream_kline(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        self._ensure_connected()
        return self._klines(ticker, interval)

    def stream_best_quote(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        self._ensure_connected()
        return self._best_quotes(ticker)

    def stream_liquidation(self, ticker: UniversalTicker) -> AsyncIterator[Liquidation]:
        self._ensure_connected()
        return self._liquidations(ticker)

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``@markPrice@1s`` — refused on books that do not fund.

        A dated future settles at expiry. Checked here, before the iterator
        runs, so MD's subscribe fails the same way a missing ``stream_*``
        does rather than starting a pump that never yields.
        """
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        if ticker.category not in FUNDING_CATEGORIES:
            raise ValueError(
                f"{self.name} {ticker.category} serves no funding rate stream; "
                f"supported: {', '.join(sorted(FUNDING_CATEGORIES))}"
            )
        return self._funding_rates(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        """``@ticker`` for the stats, ``@bookTicker`` for the quote it lacks.

        Both subscriptions on the same socket: dapi still has one combined
        stream. Nothing is emitted until a quote has arrived.
        """
        native = await self._resolve(ticker)
        quotes = await self.feed.subscribe_book_tickers(native)
        stats = await self.feed.subscribe_tickers(native)
        bid: Decimal | None = None
        ask: Decimal | None = None
        try:
            async for kind, row in _merge(quote=quotes, stat=stats):
                if row.symbol != native:
                    continue
                if kind == "quote":
                    bid, ask = row.bid, row.ask
                    continue
                if bid is None or ask is None:
                    continue
                yield row.to_ticker(ticker, bid=bid, ask=ask)
        finally:
            quotes.close()
            stats.close()

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        """``@aggTrade`` read as a plain tape — dapi publishes no other."""
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_agg_trades(native)
        async for row in self._rows(stream):
            if row.s != native:
                continue
            yield row.to_trade(ticker)

    async def _agg_trades(self, ticker: UniversalTicker) -> AsyncIterator[AggTrade]:
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_agg_trades(native)
        async for row in self._rows(stream):
            if row.s != native:
                continue
            yield row.to_agg_trade(ticker)

    async def _order_books(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_order_book(
            native, levels=self.book_levels, speed=self.book_speed
        )
        async for row in self._rows(stream):
            if row.s != native:
                continue
            yield row.to_order_book(ticker)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        canonical = normalize_interval(interval)
        native_interval = venue_interval(canonical)
        native = await self._resolve(ticker)
        size = await self._quote_per_contract(ticker)
        stream = await self.feed.subscribe_klines(native_interval, native)
        async for row in self._rows(stream):
            if row.s != native or row.interval != native_interval:
                continue
            yield row.to_kline(ticker, quote_per_contract=size).model_copy(
                update={"interval": canonical}
            )

    async def _best_quotes(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_book_tickers(native)
        async for row in self._rows(stream):
            if row.s != native:
                continue
            yield row.to_best_quote(ticker)

    async def _liquidations(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        """``@forceOrder`` — other accounts being closed out, sampled."""
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_liquidations(native)
        async for row in self._rows(stream):
            if row.symbol != native:
                continue
            yield row.to_liquidation(ticker)

    async def _funding_rates(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``@markPrice@1s`` — the still-moving prediction for the next settlement."""
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_mark_prices(native)
        async for row in self._rows(stream):
            if row.symbol != native:
                continue
            funding = row.to_funding_rate(ticker)
            if funding is None:
                continue
            yield funding

    # --- stream plumbing ---------------------------------------------------

    @staticmethod
    async def _rows(stream: EventStream[T]) -> AsyncIterator[T]:
        """Iterate a subscription and unhook it when the consumer stops."""
        try:
            async for row in stream:
                yield row
        finally:
            stream.close()

    # --- symbols -----------------------------------------------------------

    async def _resolve(self, ticker: UniversalTicker) -> str:
        """The venue's spelling of one instrument."""
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)

    async def _quote_per_contract(self, ticker: UniversalTicker) -> Decimal:
        """USD per contract, or refuse — the kline columns cannot be guessed."""
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise ValueError(f"no contract_size for {ticker}")
        return size


async def _merge(**streams: EventStream[Any]) -> AsyncIterator[tuple[str, Any]]:
    """Read several subscriptions at once, tagging each item with its stream.

    Needed because the ticker is assembled from two of Binance's feeds. On
    dapi they share one socket; reading them in sequence would still block
    the quote behind the stats.
    """
    pending: dict[asyncio.Task[Any], tuple[str, EventStream[Any]]] = {}
    try:
        for name, stream in streams.items():
            pending[asyncio.ensure_future(anext(stream))] = (name, stream)
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                name, stream = pending.pop(task)
                try:
                    item = task.result()
                except StopAsyncIteration:
                    continue
                pending[asyncio.ensure_future(anext(stream))] = (name, stream)
                yield name, item
    finally:
        for task in pending:
            task.cancel()


__all__ = [
    "BINANCE_DELIVERY_INTERVALS",
    "DEFAULT_BOOK_LEVELS",
    "DEFAULT_BOOK_SPEED",
    "FUNDING_CATEGORIES",
    "BinanceDeliveryPublicClient",
    "venue_interval",
]

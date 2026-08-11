"""The Binance USDⓈ-M futures market-data connector.

Not an implementation of a shared public interface — there is none; see
:mod:`mft.exchange.base`. The methods here resemble the spot connector's
because the same questions get asked of every venue, and where futures differs
the difference shows up in this file rather than being flattened away.

Composes two transports:

* :class:`~mft.exchange.binance.future.feed.BinanceFutureStream` — every live
  feed, across the two stream endpoints Binance splits them over.
* :class:`~mft.exchange.binance.future.rest.BinanceFuturePublicRest` — the
  on-demand reads. **REST, where the spot connector uses a socket**: the
  futures WebSocket API serves no ``klines`` and no ``exchangeInfo`` at all, so
  a socket here would answer one of the three questions this client is asked.

Three differences from spot are worth knowing before reading the streams:

* **There is no raw tape.** Futures publishes ``@aggTrade`` and nothing else,
  so :meth:`stream_trades` and :meth:`stream_agg_trades` read one stream. They
  stay two methods because they answer two models — a consumer that wants the
  match range asks for it by name — but the ids are aggregate ids on both.
* **The 24h ticker carries no quote**, so :meth:`stream_ticker` reads
  ``@bookTicker`` alongside it and pairs the two. A ``Ticker`` whose bid and
  ask were both the last price would be a number this adapter made up.
* **Liquidations exist here**, which is the one feed the spot venue has no
  concept of.

Symbols cross this boundary canonical (``BTCUSDT``) and are resolved to
Binance's spelling through the symbol plane, never by string surgery — see
:mod:`mft.exchange.symbols`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any, TypeVar

from mft.exchange.base import BaseClient
from mft.exchange.binance.future.feed import (
    DEFAULT_BOOK_LEVELS,
    DEFAULT_BOOK_SPEED,
    BinanceFutureStream,
)
from mft.exchange.binance.future.protocol import (
    BINANCE_FUTURE_MARKET_STREAM_URL,
    BINANCE_FUTURE_PUBLIC_STREAM_URL,
    BINANCE_FUTURE_REST_URL,
)
from mft.exchange.binance.future.rest import BinanceFuturePublicRest
from mft.exchange.intervals import InvalidIntervalError, normalize_interval
from mft.exchange.models import (
    AggTrade,
    BestQuote,
    Instrument,
    Kline,
    Liquidation,
    OrderBook,
    Ticker,
    Trade,
)
from mft.exchange.stream import EventStream
from mft.exchange.symbols import SymbolResolver
from mft.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Canonical interval → Binance's futures spelling.
#:
#: Almost the spot table, minus one entry that matters: futures serves **no
#: one-second candles**. Asking for them on spot works and asking here is an
#: error, so the difference is refused before the round trip rather than after
#: it. ``1M`` for the month is Binance's spelling of the one window
#: :mod:`mft.exchange.intervals` refuses to accept from above, because it
#: differs from ``1m`` by case alone.
BINANCE_FUTURE_INTERVALS: dict[str, str] = {
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
    """Canonical interval → Binance futures', or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = BINANCE_FUTURE_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"Binance futures serves no {canonical} candles; "
            f"supported: {sorted(BINANCE_FUTURE_INTERVALS)}"
        )
    return found


class BinanceFuturePublicClient(BaseClient):
    """Binance USDⓈ-M futures market data for MD.

    All six feeds are live — the five every venue has, plus liquidations::

        client = BinanceFuturePublicClient(symbols=symbol_client)
        async with client:
            ticker = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")
            async for book in client.stream_order_book(ticker):
                ...

    Reads take a :class:`~mft.exchange.tickers.UniversalTicker` rather than a
    symbol, as every connector here does. This venue trades one category, so it
    could have got by on the symbol alone — but a connector interface that only
    works for single-market venues is one MD would have to branch around.
    """

    name = "BinanceFuture"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        public_stream_url: str = BINANCE_FUTURE_PUBLIC_STREAM_URL,
        market_stream_url: str = BINANCE_FUTURE_MARKET_STREAM_URL,
        rest_url: str = BINANCE_FUTURE_REST_URL,
        feed: BinanceFutureStream | None = None,
        rest: BinanceFuturePublicRest | None = None,
        book_levels: int = DEFAULT_BOOK_LEVELS,
        book_speed: str = DEFAULT_BOOK_SPEED,
    ) -> None:
        super().__init__()
        # No credentials on either transport: every read used here is open, and
        # requiring keys for public data would mean MD could not run a feed
        # without a trading account.
        self.feed = feed or BinanceFutureStream(
            public_url=public_stream_url, market_url=market_stream_url
        )
        self.rest = rest or BinanceFuturePublicRest(base_url=rest_url)
        self.symbols = symbols
        self.book_levels = book_levels
        self.book_speed = book_speed

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.feed.connect()
        await self.rest.connect()
        self._connected = True
        logger.info("BinanceFuture public connected")

    async def close(self) -> None:
        self._connected = False
        await self.feed.close()
        await self.rest.close()

    # --- snapshots (REST) --------------------------------------------------

    async def fetch_instruments(self) -> list[Instrument]:
        """Tradeable perpetuals, in Binance's own spelling.

        Left native on purpose: this is what the symbol plane ingests to
        *build* the canonical mapping, so it cannot depend on that mapping
        existing.
        """
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

        The interval is translated on the way down and stamped back on the way
        up, so what a caller passes in is what comes back out — Binance answers
        in its own ``1M`` vocabulary and none of it escapes this method.
        """
        self._ensure_connected()
        canonical = normalize_interval(interval)
        native_interval = venue_interval(canonical)
        native = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            native, native_interval, ticker=ticker, limit=limit
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

    def stream_liquidation(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        self._ensure_connected()
        return self._liquidations(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        """``@ticker`` for the stats, ``@bookTicker`` for the quote it lacks.

        Two subscriptions on two sockets, because the futures 24h ticker has no
        bid and no ask at all and a :class:`~mft.exchange.models.Ticker`
        promises both. The alternative — publishing the last price as the quote
        — reads as a flat, crossable book to anything comparing venues, which
        is exactly what reads this feed.

        Nothing is emitted until a quote has arrived. A ticker with a stale
        quote is a real ticker a moment late; one with an invented quote is not
        a ticker at all.
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
        """``@aggTrade`` read as a plain tape — futures publishes no other.

        Spot keeps ``trade`` and ``aggtrade`` on separate streams precisely so
        that a ``Trade``'s id is a trade id. Here there is no such stream to
        read, so this feed's ids are aggregate ids and a consumer that needs
        the distinction should take :meth:`stream_agg_trades`, which says so in
        its type.
        """
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
            # Dated by Binance, unlike spot's partial depth: futures stamps
            # ``E``/``T`` on the push, so nothing has to guess arrival time.
            yield row.to_order_book(ticker)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        canonical = normalize_interval(interval)
        native_interval = venue_interval(canonical)
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_klines(native_interval, native)
        async for row in self._rows(stream):
            if row.s != native or row.interval != native_interval:
                continue
            yield row.to_kline(ticker).model_copy(update={"interval": canonical})

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
        """``@forceOrder`` — other accounts being closed out, sampled.

        Binance pushes at most the largest liquidation per symbol per second,
        so this feed is a sample of the flow and not a record of it.
        """
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_liquidations(native)
        async for row in self._rows(stream):
            if row.symbol != native:
                continue
            yield row.to_liquidation(ticker)

    # --- stream plumbing ---------------------------------------------------

    @staticmethod
    async def _rows(stream: EventStream[T]) -> AsyncIterator[T]:
        """Iterate a subscription and unhook it when the consumer stops.

        MD ends a feed by cancelling its pump task, which leaves the stream
        registered on the socket still receiving pushes. Closing it here drops
        it from the socket's fan-out on the way out.
        """
        try:
            async for row in stream:
                yield row
        finally:
            stream.close()

    # --- symbols -----------------------------------------------------------

    async def _resolve(self, ticker: UniversalTicker) -> str:
        """The venue's spelling of one instrument.

        Resolved once per stream rather than per message: the symbol we
        subscribed with is the symbol every message we keep carries, so there
        is nothing left to look up on the hot path. The ticker itself needs no
        resolving — it is what every payload out of here is stamped with.
        """
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)


async def _merge(**streams: EventStream[Any]) -> AsyncIterator[tuple[str, Any]]:
    """Read several subscriptions at once, tagging each item with its stream.

    Needed because one feed here is assembled from two of Binance's, on two
    different sockets. Reading them in sequence would block the quote behind
    the stats or the other way round; reading them into one queue keeps both
    live and says which is which.

    Ends when every stream has, and cancels the reads still outstanding on the
    way out — including when the consumer stops early, which is how MD ends a
    feed.
    """
    pending: dict[asyncio.Task[Any], tuple[str, EventStream[Any]]] = {}
    try:
        for name, stream in streams.items():
            pending[asyncio.ensure_future(anext(stream))] = (name, stream)
        while pending:
            done, _ = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
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
    "BINANCE_FUTURE_INTERVALS",
    "DEFAULT_BOOK_LEVELS",
    "DEFAULT_BOOK_SPEED",
    "BinanceFuturePublicClient",
    "venue_interval",
]

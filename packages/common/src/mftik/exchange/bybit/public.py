"""The Bybit market-data connector.

Not an implementation of a shared public interface — there is none; see
:mod:`mftik.exchange.base`. The methods here resemble Gate's and Binance's
because the same questions get asked of every venue, and where Bybit differs
the difference shows up in this file rather than being flattened away.

Composes two transports:

* :class:`~mftik.exchange.bybit.feed.BybitPublicStream` — the live feeds. **One
  socket per category**, so this connector opens them lazily and keeps one per
  category it is asked for. A client streaming spot and perps holds two.
* :class:`~mftik.exchange.bybit.rest.BybitPublicRest` — the on-demand reads. A
  caller asking for "the book right now" cannot wait for the next push, and one
  asking for the last 500 candles cannot wait at all: ``kline`` pushes the
  window in progress and never what came before it.

**The category comes off the ticker, not the constructor.** Bybit is a unified
venue, so ``Bybit_Spot_BTCUSDT`` and ``Bybit_Perp_BTCUSDT`` are two
instruments one client serves — which is the case
:class:`~mftik.exchange.tickers.UniversalTicker` exists for, and the reason every
read here takes a ticker rather than a symbol.

Symbols cross this boundary canonical (``BTCUSDT``) and are resolved to Bybit's
spelling through the symbol plane, never by string surgery — see
:mod:`mftik.exchange.symbols`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from typing import TypeVar

from mftik.exchange.base import BaseClient
from mftik.exchange.bybit.feed import DEFAULT_BOOK_DEPTH, BybitPublicStream
from mftik.exchange.bybit.protocol import (
    BYBIT_REST_URL,
    INVERSE,
    LINEAR,
    product_of,
)
from mftik.exchange.bybit.rest import BybitPublicRest
from mftik.exchange.intervals import InvalidIntervalError, normalize_interval
from mftik.exchange.models import (
    BestQuote,
    FundingRate,
    Kline,
    Liquidation,
    OpenInterest,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.stream import EventStream
from mftik.exchange.symbols import SymbolResolver
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Canonical interval → Bybit's spelling.
#:
#: Not an identity map anywhere: Bybit names its minute windows by the number
#: of minutes alone, so an hour is ``60`` and four hours ``240``, and only the
#: day, week and month get a letter. This table is also the only place that
#: knows which windows Bybit serves at all — an interval absent here is refused
#: before the round trip rather than after an error.
BYBIT_INTERVALS: dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "4h": "240",
    "6h": "360",
    "12h": "720",
    "1d": "D",
    "1w": "W",
    "1mo": "M",
}

#: Products that carry ``allLiquidation``. Spot has no liquidations; options
#: are not traded here yet, so a subscribe on those books is refused locally
#: rather than left hanging on a socket that never pushes.
LIQUIDATION_PRODUCTS = frozenset({LINEAR, INVERSE})

#: Categories that pay a funding hook. Checked on the category rather than the
#: product because a dated future settles at expiry instead of funding, and
#: :func:`~mftik.exchange.bybit.protocol.product_of` maps it onto ``linear``
#: alongside the perps — the product alone cannot tell the two apart. Bybit's
#: inverse perps arrive as ``Perp`` too; ``inverse`` is a product, not one of
#: our categories.
FUNDING_CATEGORIES = frozenset({Category.PERP})

#: Categories that publish open interest. Spot has none. A dated
#: future does — unlike funding, which only a perpetual settles.
OPEN_INTEREST_CATEGORIES = frozenset({Category.PERP, Category.FUTURE})


def venue_interval(interval: str) -> str:
    """Canonical interval → Bybit's, or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = BYBIT_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"Bybit serves no {canonical} candles; "
            f"supported: {sorted(BYBIT_INTERVALS)}"
        )
    return found


class BybitPublicClient(BaseClient):
    """Bybit market data for MD, across every category.

    ::

        client = BybitPublicClient(symbols=symbol_client)
        async with client:
            ticker = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
            async for book in client.stream_order_book(ticker):
                ...

    Feeds are opened per category on first use and closed together. Nothing is
    connected eagerly: a client that only ever reads spot should not hold a
    socket to the perp book open for the life of the process.
    """

    name = "Bybit"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest_url: str = BYBIT_REST_URL,
        testnet: bool = False,
        rest: BybitPublicRest | None = None,
        feeds: dict[str, BybitPublicStream] | None = None,
        book_depth: int = DEFAULT_BOOK_DEPTH,
    ) -> None:
        super().__init__()
        # No credentials on either transport: every call used here is open, and
        # requiring keys for public data would mean MD could not run a feed
        # without a trading account.
        self.symbols = symbols
        self.rest = rest or BybitPublicRest(base_url=rest_url)
        self.testnet = testnet
        self.book_depth = book_depth
        #: product → its live socket. Pre-seeded ones (a test's fakes) are used
        #: as they are and never replaced.
        self._feeds: dict[str, BybitPublicStream] = dict(feeds or {})

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.rest.connect()
        for feed in self._feeds.values():
            await feed.connect()
        self._connected = True
        logger.info("Bybit public connected")

    async def close(self) -> None:
        self._connected = False
        for feed in self._feeds.values():
            await feed.close()
        self._feeds.clear()
        await self.rest.close()

    async def feed_for(self, product: str) -> BybitPublicStream:
        """The socket carrying one category, opening it if this is the first ask.

        Bybit has no single market-data endpoint: ``spot`` and ``linear`` are
        different hosts' worth of connection, so "the feed" is really one per
        book and a client that streams both holds both.
        """
        feed = self._feeds.get(product)
        if feed is None:
            feed = BybitPublicStream(product=product, testnet=self.testnet)
            self._feeds[product] = feed
        if not feed.connected:
            await feed.connect()
        return feed

    # --- snapshots (REST) --------------------------------------------------

    async def fetch_instruments(self, product: str | None = None):
        """Tradeable symbols, mapped for the plane.

        ``product`` defaults to spot because a caller that wants the
        contract books says so — they are different instrument universes, not
        pages of one.
        """
        self._ensure_connected()
        return await self.rest.fetch_instruments(product or "spot")

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        native, product = await self._resolve(ticker)
        return await self.rest.fetch_ticker(product, native, ticker=ticker)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 50
    ) -> OrderBook:
        self._ensure_connected()
        native, product = await self._resolve(ticker)
        return await self.rest.fetch_order_book(
            product, native, ticker=ticker, depth=depth
        )

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int = 100
    ) -> list[Kline]:
        """Recent candles, oldest first, in canonical symbol and interval.

        The interval is translated on the way down and stamped back on the way
        up, so what a caller passes in is what comes back out — Bybit answers
        in its own ``60``/``D`` vocabulary and none of it escapes this method.
        The reversal from newest-first happens one layer down, in
        :meth:`~mftik.exchange.bybit.rest.BybitPublicRest.fetch_klines`.
        """
        self._ensure_connected()
        canonical = normalize_interval(interval)
        native_interval = venue_interval(canonical)
        native, product = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            product, native, native_interval, ticker=ticker, limit=limit
        )
        return [
            kline.model_copy(update={"interval": canonical}) for kline in klines
        ]

    # --- streams (public sockets) ------------------------------------------

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        self._ensure_connected()
        return self._tickers(ticker)

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        self._ensure_connected()
        return self._trades(ticker)

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
        """``allLiquidation`` — refused on books Bybit does not liquidate.

        Checked here, before the iterator runs, so MD's subscribe fails the
        same way a missing ``stream_*`` does rather than starting a pump that
        never yields.
        """
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        product = product_of(ticker.category)
        if product not in LIQUIDATION_PRODUCTS:
            raise ValueError(
                f"Bybit {product} serves no liquidation stream; "
                f"supported: {', '.join(sorted(LIQUIDATION_PRODUCTS))}"
            )
        return self._liquidations(ticker)

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``tickers`` — refused on books Bybit does not fund.

        Shares the ticker wire. A late joiner is silent until the next
        ``fundingRate``-bearing delta; nothing is REST-filled. Checked here,
        before the iterator runs, so MD's subscribe fails the same way a
        missing ``stream_*`` does rather than starting a pump that never
        yields.
        """
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        if ticker.category not in FUNDING_CATEGORIES:
            raise ValueError(
                f"Bybit {ticker.category} serves no funding rate stream; "
                f"supported: {', '.join(sorted(FUNDING_CATEGORIES))}"
            )
        return self._funding_rates(ticker)

    def stream_open_interest(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        """``tickers`` — refused on spot, before the iterator runs.

        Shares the ticker wire with ``stream_ticker`` / ``stream_funding_rate``.
        A late joiner is silent until the next ``openInterest``-bearing
        delta; nothing is REST-filled. A dated future is answered.
        """
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        if ticker.category not in OPEN_INTEREST_CATEGORIES:
            names = ", ".join(
                sorted(c.value for c in OPEN_INTEREST_CATEGORIES)
            )
            raise ValueError(
                f"Bybit {ticker.category} serves no open interest stream; "
                f"supported: {names}"
            )
        return self._open_interests(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        """``tickers`` — skipping the deltas that carry no price.

        On the contract books this topic pushes only what changed, so a message
        can be a funding-rate update with no quote in it. Publishing that as a
        :class:`~mftik.exchange.models.Ticker` would mean inventing prices; it is
        dropped instead.
        """
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_tickers(native)
        async for row, _ts in self._rows(stream):
            if row.symbol != native or not row.quoted:
                continue
            yield row.to_ticker(ticker, ts=time.time())

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_trades(native)
        async for row in self._rows(stream):
            if row.symbol != native:
                continue
            yield row.to_trade(ticker)

    async def _order_books(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        """Whole books, folded from Bybit's snapshot-then-deltas by the feed."""
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_order_book(native, depth=self.book_depth)
        async for snapshot in self._rows(stream):
            if snapshot.symbol != native:
                continue
            yield snapshot.to_order_book(ticker)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        canonical = normalize_interval(interval)
        native_interval = venue_interval(canonical)
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_klines(native_interval, native)
        async for pushed_symbol, row in self._rows(stream):
            if pushed_symbol != native or row.interval != native_interval:
                continue
            yield row.to_kline(ticker).model_copy(
                update={"interval": canonical}
            )

    async def _best_quotes(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        """``orderbook.1`` — Bybit's top of book, which is a snapshot each time.

        A one-sided push means that side of the book is empty, and a quote with
        a hole in it is not something a caller can act on, so it is skipped
        rather than filled in.
        """
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_best_quote(native)
        async for row in self._rows(stream):
            if row.symbol != native:
                continue
            quote = row.to_best_quote(ticker, ts=time.time())
            if quote is None:
                continue
            yield quote

    async def _liquidations(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        """``allLiquidation`` — forced closes on the contract books."""
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_liquidations(native)
        async for row in self._rows(stream):
            if row.symbol != native:
                continue
            yield row.to_liquidation(ticker)

    async def _funding_rates(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``tickers`` — yield when the delta names a rate."""
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_tickers(native)
        async for row, ts in self._rows(stream):
            if row.symbol != native:
                continue
            funding = row.to_funding_rate(ticker, ts=ts)
            if funding is None:
                continue
            yield funding

    async def _open_interests(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        """``tickers`` — yield when the delta names a size."""
        native, product = await self._resolve(ticker)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_tickers(native)
        async for row, ts in self._rows(stream):
            if row.symbol != native:
                continue
            interest = row.to_open_interest(ticker, ts=ts)
            if interest is None:
                continue
            yield interest

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

    async def _resolve(self, ticker: UniversalTicker) -> tuple[str, str]:
        """``(venue symbol, product)`` for one instrument.

        Resolved once per stream rather than per message: the symbol we
        subscribed with is the symbol every message we keep carries, so there
        is nothing left to look up on the hot path. The ticker itself needs no
        resolving — it is what every payload out of here is stamped with.
        """
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        return (
            await self.symbols.exch_ticker(ticker),
            product_of(ticker.category),
        )


__all__ = [
    "BYBIT_INTERVALS",
    "FUNDING_CATEGORIES",
    "LIQUIDATION_PRODUCTS",
    "OPEN_INTEREST_CATEGORIES",
    "BybitPublicClient",
    "venue_interval",
]

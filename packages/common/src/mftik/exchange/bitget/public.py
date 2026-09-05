"""The Bitget market-data connector.

One public client, sockets opened per Bitget ``instType`` on first use
(V4). The map is keyed on Bitget's ``category`` string
(``SPOT`` / ``USDT-FUTURES`` / ``USDC-FUTURES``), not on
:class:`~mftik.exchange.tickers.Category`. A USDC perp must not ride the
USDT socket.

**V5:** funding and open interest ride the ticker row. They are a second
pump on a shared wire identity (MDS-1), not a second ``SUBSCRIBE``. Spot
has neither; those methods raise before the iterator runs.

There is no aggregated tape.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from typing import TypeVar

from mftik.exchange.base import BaseClient
from mftik.exchange.bitget.feed import DEFAULT_BOOK_CHANNEL, BitgetPublicStream
from mftik.exchange.bitget.models import kline_from_row
from mftik.exchange.bitget.protocol import (
    BITGET_REST_URL,
    SPOT,
    USDC_FUTURES,
    USDT_FUTURES,
    inst_type_of,
    product_of,
    public_url,
)
from mftik.exchange.bitget.rest import BitgetPublicRest
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

#: Canonical interval → Bitget's ``interval``. Hours and longer are capitalised.
#: There is no 2h window.
BITGET_INTERVALS: dict[str, str] = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1mo": "1M",
}

LIQUIDATION_PRODUCTS = frozenset({USDT_FUTURES, USDC_FUTURES})
FUNDING_CATEGORIES = frozenset({Category.PERP})
OPEN_INTEREST_CATEGORIES = frozenset({Category.PERP})


def venue_interval(interval: str) -> str:
    """Canonical interval → Bitget's, or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = BITGET_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"Bitget serves no {canonical} candles; "
            f"supported: {sorted(BITGET_INTERVALS)}"
        )
    return found


class BitgetPublicClient(BaseClient):
    """Bitget market data for MD, across every book this epic lists.

    Feeds are opened per Bitget ``category`` on first use and closed
    together. A client that only ever reads spot never holds a futures
    socket.
    """

    name = "Bitget"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest_url: str = BITGET_REST_URL,
        demo: bool = False,
        rest: BitgetPublicRest | None = None,
        feeds: dict[str, BitgetPublicStream] | None = None,
    ) -> None:
        super().__init__()
        self.symbols = symbols
        self.demo = demo
        self.rest = rest or BitgetPublicRest(base_url=rest_url, demo=demo)
        self._feeds: dict[str, BitgetPublicStream] = dict(feeds or {})

    async def connect(self) -> None:
        await self.rest.connect()
        for feed in self._feeds.values():
            await feed.connect()
        self._connected = True
        logger.info("Bitget public connected")

    async def close(self) -> None:
        self._connected = False
        for feed in self._feeds.values():
            await feed.close()
        self._feeds.clear()
        await self.rest.close()

    async def feed_for(self, product: str) -> BitgetPublicStream:
        """The socket carrying one Bitget ``category``, opened on first use.

        Keyed on ``SPOT`` / ``USDT-FUTURES`` / ``USDC-FUTURES`` (V4). USDC
        is a third ``instType`` and does not share the USDT socket.
        """
        feed = self._feeds.get(product)
        if feed is None:
            feed = BitgetPublicStream(
                public_url(demo=self.demo),
                inst_type=inst_type_of(product),
            )
            self._feeds[product] = feed
        if not feed.connected:
            await feed.connect()
        return feed

    # --- snapshots ---------------------------------------------------------

    async def fetch_instruments(self, product: str | None = None):
        self._ensure_connected()
        return await self.rest.fetch_instruments(product or SPOT)

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
        self._ensure_connected()
        canonical = normalize_interval(interval)
        bar = venue_interval(canonical)
        native, product = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            product, native, bar, ticker=ticker, limit=limit
        )
        return [kline.model_copy(update={"interval": canonical}) for kline in klines]

    # --- streams -----------------------------------------------------------

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
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        product = product_of(ticker)
        if product not in LIQUIDATION_PRODUCTS:
            raise ValueError(
                f"Bitget {product} serves no liquidation stream; "
                f"supported: {', '.join(sorted(LIQUIDATION_PRODUCTS))}"
            )
        return self._liquidations(ticker)

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """Ticker fields — refused on books Bitget does not fund (V5)."""
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        if ticker.category not in FUNDING_CATEGORIES:
            raise ValueError(
                f"Bitget {ticker.category} serves no funding rate stream; "
                f"supported: {', '.join(sorted(c.value for c in FUNDING_CATEGORIES))}"
            )
        return self._funding_rates(ticker)

    def stream_open_interest(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        """Ticker fields — refused on spot, before the iterator runs (V5)."""
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        if ticker.category not in OPEN_INTEREST_CATEGORIES:
            names = ", ".join(sorted(c.value for c in OPEN_INTEREST_CATEGORIES))
            raise ValueError(
                f"Bitget {ticker.category} serves no open interest stream; "
                f"supported: {names}"
            )
        return self._open_interests(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_tickers(inst, native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            if not row.quoted:
                continue
            yield row.to_ticker(ticker, ts=time.time())

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_trades(inst, native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            yield row.to_trade(ticker)

    async def _order_books(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_order_book(
            inst, native, topic=DEFAULT_BOOK_CHANNEL
        )
        async for snapshot in self._rows(stream):
            if snapshot.symbol != native:
                continue
            yield snapshot.to_order_book(ticker)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        canonical = normalize_interval(interval)
        bar = venue_interval(canonical)
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_klines(inst, native, bar)
        async for row in self._rows(stream):
            if not isinstance(row, list):
                continue
            candle = kline_from_row(row, ticker, bar)
            yield candle.model_copy(update={"interval": canonical})

    async def _best_quotes(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_best_quote(inst, native)
        async for row in self._rows(stream):
            quote = row.to_best_quote(ticker, ts=time.time())
            if quote is None:
                continue
            yield quote

    async def _liquidations(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_liquidations(inst)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            yield row.to_liquidation(ticker)

    async def _funding_rates(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``ticker`` — yield when the delta names a rate (V5)."""
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_tickers(inst, native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            funding = row.to_funding_rate(ticker)
            if funding is None:
                continue
            yield funding

    async def _open_interests(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        """``ticker`` — yield when the delta names a size (V5)."""
        native, product = await self._resolve(ticker)
        inst = inst_type_of(product)
        feed = await self.feed_for(product)
        stream = await feed.subscribe_tickers(inst, native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            interest = row.to_open_interest(ticker)
            if interest is None:
                continue
            yield interest

    @staticmethod
    async def _rows(stream: EventStream[T]) -> AsyncIterator[T]:
        try:
            async for row in stream:
                yield row
        finally:
            stream.close()

    async def _resolve(self, ticker: UniversalTicker) -> tuple[str, str]:
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker), product_of(ticker)


__all__ = [
    "BITGET_INTERVALS",
    "FUNDING_CATEGORIES",
    "LIQUIDATION_PRODUCTS",
    "OPEN_INTEREST_CATEGORIES",
    "BitgetPublicClient",
    "venue_interval",
]

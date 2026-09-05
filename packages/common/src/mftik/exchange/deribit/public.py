"""The Deribit market-data connector.

One public client, one public socket (V4). Spot and linear perps share
it; the channel names the instrument.

**V5:** funding and open interest ride the ticker row. They are a second
pump on a shared wire identity (MDS-1), not a second ``SUBSCRIBE``. Spot
has neither; those methods raise before the iterator runs.

There is no aggregated tape and no liquidation channel.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from typing import TypeVar

from mftik.exchange.base import BaseClient
from mftik.exchange.deribit.feed import DeribitPublicStream
from mftik.exchange.deribit.models import kline_from_tick
from mftik.exchange.deribit.protocol import (
    DERIBIT_REST_URL,
    DERIBIT_WS_URL,
    KIND_FUTURE,
    KIND_SPOT,
)
from mftik.exchange.deribit.rest import DeribitPublicRest
from mftik.exchange.intervals import InvalidIntervalError, normalize_interval
from mftik.exchange.models import (
    BestQuote,
    FundingRate,
    Kline,
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

#: Canonical interval → Deribit's ``resolution``. Hours are minutes; the
#: day is ``1D``.
DERIBIT_INTERVALS: dict[str, str] = {
    "1m": "1",
    "3m": "3",
    "5m": "5",
    "15m": "15",
    "30m": "30",
    "1h": "60",
    "2h": "120",
    "3h": "180",
    "6h": "360",
    "12h": "720",
    "1d": "1D",
}

FUNDING_CATEGORIES = frozenset({Category.PERP})
OPEN_INTEREST_CATEGORIES = frozenset({Category.PERP})


def venue_interval(interval: str) -> str:
    """Canonical interval → Deribit's, or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = DERIBIT_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"Deribit serves no {canonical} candles; "
            f"supported: {sorted(DERIBIT_INTERVALS)}"
        )
    return found


class DeribitPublicClient(BaseClient):
    """Deribit market data for MD, on spot and the linear perpetual books."""

    name = "Deribit"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest_url: str = DERIBIT_REST_URL,
        ws_url: str = DERIBIT_WS_URL,
        rest: DeribitPublicRest | None = None,
        feed: DeribitPublicStream | None = None,
    ) -> None:
        super().__init__()
        self.symbols = symbols
        self._ws_url = ws_url
        self.rest = rest or DeribitPublicRest(base_url=rest_url)
        self._feed = feed

    async def connect(self) -> None:
        await self.rest.connect()
        if self._feed is not None:
            await self._feed.connect()
        self._connected = True
        logger.info("Deribit public connected")

    async def close(self) -> None:
        self._connected = False
        if self._feed is not None:
            await self._feed.close()
            self._feed = None
        await self.rest.close()

    async def feed(self) -> DeribitPublicStream:
        """The one public socket, opened on first use."""
        if self._feed is None:
            self._feed = DeribitPublicStream(self._ws_url)
        if not self._feed.connected:
            await self._feed.connect()
        return self._feed

    # --- snapshots ---------------------------------------------------------

    async def fetch_instruments(self, category: Category | None = None):
        self._ensure_connected()
        wanted = category or Category.SPOT
        kind = KIND_SPOT if wanted is Category.SPOT else KIND_FUTURE
        return await self.rest.fetch_instruments(kind)

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        native = await self._resolve(ticker)
        return await self.rest.fetch_ticker(native, ticker=ticker)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 20
    ) -> OrderBook:
        self._ensure_connected()
        native = await self._resolve(ticker)
        return await self.rest.fetch_order_book(native, ticker=ticker, depth=depth)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int = 100
    ) -> list[Kline]:
        self._ensure_connected()
        canonical = normalize_interval(interval)
        resolution = venue_interval(canonical)
        native = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            native,
            resolution,
            ticker=ticker,
            interval=canonical,
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

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """Ticker fields — refused on books Deribit does not fund (V5)."""
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        if ticker.category not in FUNDING_CATEGORIES:
            raise ValueError(
                f"Deribit {ticker.category} serves no funding rate stream; "
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
                f"Deribit {ticker.category} serves no open interest stream; "
                f"supported: {names}"
            )
        return self._open_interests(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_tickers(native)
        async for row in self._rows(stream):
            if row.instrument_name and row.instrument_name != native:
                continue
            if not row.quoted:
                continue
            yield row.to_ticker(ticker, ts=time.time())

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_trades(native)
        async for row in self._rows(stream):
            if row.instrument_name and row.instrument_name != native:
                continue
            yield row.to_trade(ticker)

    async def _order_books(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_order_book(native)
        async for snapshot in self._rows(stream):
            if snapshot.instrument != native:
                continue
            yield snapshot.to_order_book(ticker)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        canonical = normalize_interval(interval)
        resolution = venue_interval(canonical)
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_klines(native, resolution)
        async for row in self._rows(stream):
            if not isinstance(row, dict):
                continue
            candle = kline_from_tick(row, ticker, canonical)
            if candle is None:
                continue
            yield candle

    async def _best_quotes(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_best_quote(native)
        async for row in self._rows(stream):
            quote = row.to_best_quote(ticker, ts=time.time())
            if quote is None:
                continue
            yield quote

    async def _funding_rates(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``ticker`` — yield when the delta names a rate (V5)."""
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_tickers(native)
        async for row in self._rows(stream):
            if row.instrument_name and row.instrument_name != native:
                continue
            funding = row.to_funding_rate(ticker)
            if funding is None:
                continue
            yield funding

    async def _open_interests(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        """``ticker`` — yield when the delta names a size (V5)."""
        native = await self._resolve(ticker)
        feed = await self.feed()
        stream = await feed.subscribe_tickers(native)
        async for row in self._rows(stream):
            if row.instrument_name and row.instrument_name != native:
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

    async def _resolve(self, ticker: UniversalTicker) -> str:
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker)


__all__ = [
    "DERIBIT_INTERVALS",
    "FUNDING_CATEGORIES",
    "OPEN_INTEREST_CATEGORIES",
    "DeribitPublicClient",
    "venue_interval",
]

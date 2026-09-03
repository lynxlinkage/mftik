"""The OKX market-data connector.

Composes two transports:

* :class:`~mftik.exchange.okx.feed.OkxPublicStream` — the live feeds. Public
  books/trades/tickers on one socket; candles on the business socket. Both
  open lazily.
* :class:`~mftik.exchange.okx.rest.OkxPublicRest` — the on-demand reads.

**The category comes off the ticker, not the constructor.** OKX is a unified
venue, so ``Okx_Spot_BTCUSDT`` and ``Okx_Perp_BTCUSDT`` are two instruments
one client serves.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import TypeVar

from mftik.exchange.base import BaseClient
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
from mftik.exchange.okx.feed import DEFAULT_BOOK_CHANNEL, OkxPublicStream
from mftik.exchange.okx.models import kline_from_row
from mftik.exchange.okx.protocol import (
    FUTURES,
    OKX_REST_URL,
    SPOT,
    SWAP,
    business_url,
    product_of,
    public_url,
)
from mftik.exchange.okx.rest import OkxPublicRest
from mftik.exchange.stream import EventStream
from mftik.exchange.symbols import SymbolResolver
from mftik.exchange.tickers import Category, UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Canonical interval → OKX's ``bar``. Hours and longer are capitalised.
OKX_INTERVALS: dict[str, str] = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
    "1w": "1W",
    "1mo": "1M",
}

LIQUIDATION_PRODUCTS = frozenset({SWAP})

#: Products that pay a funding hook. The same set as the liquidation one
#: today, spelled separately because the two answer different questions: a
#: venue that starts liquidating dated futures still would not fund them.
FUNDING_PRODUCTS = frozenset({SWAP})

#: Contract books that publish open interest. Spot has none. Dated
#: futures do — unlike funding, which only a perpetual settles.
OPEN_INTEREST_PRODUCTS = frozenset({SWAP, FUTURES})


def venue_interval(interval: str) -> str:
    """Canonical interval → OKX's, or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = OKX_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"OKX serves no {canonical} candles; "
            f"supported: {sorted(OKX_INTERVALS)}"
        )
    return found


class OkxPublicClient(BaseClient):
    """OKX market data for MD, across every category."""

    name = "Okx"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest_url: str = OKX_REST_URL,
        demo: bool = False,
        rest: OkxPublicRest | None = None,
        public: OkxPublicStream | None = None,
        business: OkxPublicStream | None = None,
    ) -> None:
        super().__init__()
        self.symbols = symbols
        self.demo = demo
        self.rest = rest or OkxPublicRest(base_url=rest_url, demo=demo)
        self._public = public
        self._business = business

    async def connect(self) -> None:
        await self.rest.connect()
        if self._public is not None:
            await self._public.connect()
        if self._business is not None:
            await self._business.connect()
        self._connected = True
        logger.info("OKX public connected")

    async def close(self) -> None:
        self._connected = False
        if self._public is not None:
            await self._public.close()
        if self._business is not None:
            await self._business.close()
        self._public = None
        self._business = None
        await self.rest.close()

    async def public_feed(self) -> OkxPublicStream:
        if self._public is None:
            self._public = OkxPublicStream(public_url(demo=self.demo))
        if not self._public.connected:
            await self._public.connect()
        return self._public

    async def business_feed(self) -> OkxPublicStream:
        if self._business is None:
            self._business = OkxPublicStream(business_url(demo=self.demo))
        if not self._business.connected:
            await self._business.connect()
        return self._business

    # --- snapshots ---------------------------------------------------------

    async def fetch_instruments(self, product: str | None = None):
        self._ensure_connected()
        return await self.rest.fetch_instruments(product or SPOT)

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        native, _ = await self._resolve(ticker)
        return await self.rest.fetch_ticker(native, ticker=ticker)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 50
    ) -> OrderBook:
        self._ensure_connected()
        native, _ = await self._resolve(ticker)
        return await self.rest.fetch_order_book(
            native,
            ticker=ticker,
            depth=depth,
            contract_size=await self._multiplier(ticker),
        )

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int = 100
    ) -> list[Kline]:
        self._ensure_connected()
        canonical = normalize_interval(interval)
        bar = venue_interval(canonical)
        native, _ = await self._resolve(ticker)
        klines = await self.rest.fetch_klines(
            native,
            bar,
            ticker=ticker,
            limit=limit,
            contract_size=await self._multiplier(ticker),
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
        product = product_of(ticker.category)
        if product not in LIQUIDATION_PRODUCTS:
            raise ValueError(
                f"OKX {product} serves no liquidation stream; "
                f"supported: {', '.join(sorted(LIQUIDATION_PRODUCTS))}"
            )
        return self._liquidations(ticker)

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``funding-rate`` — refused on books OKX does not fund.

        Checked here, before the iterator runs, so MD's subscribe fails the
        same way a missing ``stream_*`` does rather than starting a pump
        that never yields.
        """
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        product = product_of(ticker.category)
        if product not in FUNDING_PRODUCTS:
            raise ValueError(
                f"OKX {product} serves no funding rate stream; "
                f"supported: {', '.join(sorted(FUNDING_PRODUCTS))}"
            )
        return self._funding_rates(ticker)

    def stream_open_interest(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OpenInterest]:
        """``open-interest`` — refused on spot, before the iterator runs.

        A dated future is answered. Checked here so MD's subscribe fails
        the same way a missing ``stream_*`` does rather than starting a
        pump that never yields.
        """
        self._ensure_connected()
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        product = product_of(ticker.category)
        if product not in OPEN_INTEREST_PRODUCTS:
            raise ValueError(
                f"OKX {product} serves no open interest stream; "
                f"supported: {', '.join(sorted(OPEN_INTEREST_PRODUCTS))}"
            )
        return self._open_interests(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        native, _ = await self._resolve(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_tickers(native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            if not row.quoted:
                continue
            yield row.to_ticker(ticker, ts=time.time())

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        native, _ = await self._resolve(ticker)
        scale = await self._multiplier(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_trades(native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            yield row.to_trade(ticker, contract_size=scale)

    async def _order_books(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        native, _ = await self._resolve(ticker)
        scale = await self._multiplier(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_order_book(native, channel=DEFAULT_BOOK_CHANNEL)
        async for snapshot in self._rows(stream):
            if snapshot.symbol != native:
                continue
            yield snapshot.to_order_book(ticker, contract_size=scale)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        canonical = normalize_interval(interval)
        bar = venue_interval(canonical)
        native, _ = await self._resolve(ticker)
        scale = await self._multiplier(ticker)
        feed = await self.business_feed()
        stream = await feed.subscribe_klines(native, bar)
        async for row in self._rows(stream):
            if not isinstance(row, list):
                continue
            candle = kline_from_row(row, ticker, bar, contract_size=scale)
            yield candle.model_copy(update={"interval": canonical})

    async def _best_quotes(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        native, _ = await self._resolve(ticker)
        scale = await self._multiplier(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_best_quote(native)
        async for row in self._rows(stream):
            quote = row.to_best_quote(
                ticker, ts=time.time(), contract_size=scale
            )
            if quote is None:
                continue
            yield quote

    async def _liquidations(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        native, product = await self._resolve(ticker)
        scale = await self._multiplier(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_liquidations(product)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            for event in row.to_liquidations(ticker, contract_size=scale):
                yield event

    async def _funding_rates(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``funding-rate`` — one instrument, SWAP only."""
        native, _ = await self._resolve(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_funding_rate(native)
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
        """``open-interest`` — one instrument, contracts converted to base."""
        native, _ = await self._resolve(ticker)
        scale = await self._multiplier(ticker)
        feed = await self.public_feed()
        stream = await feed.subscribe_open_interest(native)
        async for row in self._rows(stream):
            if row.symbol and row.symbol != native:
                continue
            interest = row.to_open_interest(ticker, contract_size=scale)
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

    async def _multiplier(self, ticker: UniversalTicker) -> Decimal | None:
        if ticker.category is not Category.PERP:
            return None
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise ValueError(f"no contract_size for {ticker}")
        return size

    async def _resolve(self, ticker: UniversalTicker) -> tuple[str, str]:
        if ticker.venue != self.name:
            raise ValueError(
                f"{self.name} client was handed a {ticker.venue} ticker: {ticker}"
            )
        return await self.symbols.exch_ticker(ticker), product_of(ticker.category)


__all__ = [
    "FUNDING_PRODUCTS",
    "LIQUIDATION_PRODUCTS",
    "OPEN_INTEREST_PRODUCTS",
    "OKX_INTERVALS",
    "OkxPublicClient",
    "venue_interval",
]

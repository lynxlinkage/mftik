"""The Binance spot market-data connector.

Not an implementation of a shared public interface — there is none; see
:mod:`mftik.exchange.base`. The methods here resemble Gate's because the same
questions get asked of every venue, not because anything enforces it, and where
Binance differs the difference shows up in this file rather than being
flattened away.

Composes the two sockets Binance actually offers:

* :class:`~mftik.exchange.binance.spot.feed.BinanceSpotStream` — every live feed.
  Binance pushes tape, book, candles, ticker and best quote from one host.
* :class:`~mftik.exchange.binance.spot.client.BinanceSpotWsApi` — the on-demand
  reads, unauthenticated. A caller asking for "the book right now" cannot wait
  for the depth stream's next tick, and one asking for the last 500 candles
  cannot wait at all: ``@kline`` pushes the window in progress and never what
  came before it.

**Binance answers those reads over a socket, not REST** — that is the shape of
this adapter's main departure from Gate's. It means one envelope, one error
type and one signing story across the whole venue instead of a REST plane with
its own error body, which is worth an extra socket in the market-data path.

Symbols cross this boundary canonical (``BTCUSDT``) and are resolved to
Binance's spelling through the symbol plane, never by string surgery — see
:mod:`mftik.exchange.symbols`. Binance's spelling for spot happens to *be*
``BTCUSDT``, and that coincidence is exactly why the lookup still goes through
the plane: it is true for spot pairs today and is not a rule.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from typing import TypeVar

from mftik.exchange.base import BaseClient
from mftik.exchange.binance.spot import streams as st
from mftik.exchange.binance.spot.client import BinanceSpotWsApi
from mftik.exchange.binance.spot.feed import BinanceSpotStream
from mftik.exchange.binance.spot.protocol import (
    BINANCE_SPOT_STREAM_URL,
    BINANCE_SPOT_WS_API_URL,
)
from mftik.exchange.intervals import InvalidIntervalError, normalize_interval
from mftik.exchange.models import (
    AggTrade,
    BestQuote,
    Kline,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.stream import EventStream
from mftik.exchange.symbols import SymbolResolver
from mftik.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: ``@depth<levels>@<speed>`` arguments. Binance caps this stream at 5/10/20
#: levels and pushes a whole book every tick, which is what MD wants. The diff
#: stream would need snapshot folding and sequence tracking to produce the same
#: thing.
DEFAULT_BOOK_LEVELS = 20
DEFAULT_BOOK_SPEED = st.SPEED_100MS

#: Canonical interval → Binance's spelling, for the windows Binance serves.
#:
#: Almost an identity map, and deliberately written out anyway. Binance takes
#: the canonical spelling for everything but the month, where it wants ``1M`` —
#: the one spelling :mod:`mftik.exchange.intervals` refuses to accept from above,
#: because it differs from ``1m`` by case alone. Translating it here, once, is
#: the entire reason that rule is affordable.
#:
#: This table is also the only place that knows which windows Binance serves at
#: all: an interval absent here is refused before the round trip rather than
#: after an error.
BINANCE_INTERVALS: dict[str, str] = {
    "1s": "1s",
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
    """Canonical interval → Binance's, or refuse before any round trip."""
    canonical = normalize_interval(interval)
    found = BINANCE_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"Binance serves no {canonical} candles; "
            f"supported: {sorted(BINANCE_INTERVALS)}"
        )
    return found


class BinanceSpotPublicClient(BaseClient):
    """Binance spot market data for MD.

    All five feeds are live::

        client = BinanceSpotPublicClient(symbols=symbol_client)
        async with client:
            ticker = UniversalTicker.parse("Binance_Spot_BTCUSDT")
            async for book in client.stream_order_book(ticker):
                ...

    Reads take a :class:`~mftik.exchange.tickers.UniversalTicker` rather than a
    symbol. Binance spot has one category and could have got by on the symbol
    alone, but the market is the instrument's property on a unified venue, and
    a connector interface that only works for classic ones is one MD would have
    to branch around.
    """

    name = "Binance"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        stream_url: str = BINANCE_SPOT_STREAM_URL,
        ws_api_url: str = BINANCE_SPOT_WS_API_URL,
        feed: BinanceSpotStream | None = None,
        api: BinanceSpotWsApi | None = None,
        book_levels: int = DEFAULT_BOOK_LEVELS,
        book_speed: str = DEFAULT_BOOK_SPEED,
    ) -> None:
        super().__init__()
        # No credentials on either socket: every method used here is open, and
        # requiring keys for public data would mean MD could not run a feed
        # without a trading account.
        self.feed = feed or BinanceSpotStream(url=stream_url)
        self.api = api or BinanceSpotWsApi(url=ws_api_url)
        self.symbols = symbols
        self.book_levels = book_levels
        self.book_speed = book_speed

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.feed.connect()
        await self.api.connect()
        self._connected = True
        logger.info("Binance public connected")

    async def close(self) -> None:
        self._connected = False
        await self.feed.close()
        await self.api.close()

    # --- snapshots (WebSocket API) -----------------------------------------

    async def fetch_instruments(self):
        """Tradeable symbols, mapped for the plane."""
        self._ensure_connected()
        return await self.api.fetch_instruments()

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        native = await self._resolve(ticker)
        return await self.api.fetch_ticker(native, ticker=ticker)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 10
    ) -> OrderBook:
        self._ensure_connected()
        native = await self._resolve(ticker)
        return await self.api.fetch_order_book(
            native, ticker=ticker, depth=depth
        )

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
        klines = await self.api.fetch_klines(
            native, native_interval, ticker=ticker, limit=limit
        )
        return [
            kline.model_copy(update={"interval": canonical}) for kline in klines
        ]

    # --- streams (market streams socket) -----------------------------------

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

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_tickers(native)
        async for row in self._rows(stream):
            if row.s != native:
                continue
            yield row.to_ticker(ticker)

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        """``@trade`` — one message per match, ids being the venue's own.

        Binance also publishes the coalesced tape, and this used to read it:
        same volume at a fraction of the message rate, which is the better
        default for anything that only wants price and size. But it is now a
        feed of its own (:meth:`stream_agg_trades`), and two topics reading one
        stream would mean ``trade`` and ``aggtrade`` differed in name only —
        while ``trade`` handed out aggregate ids under a field documented as
        the venue's trade id. A strategy that wants the cheaper feed should ask
        for it by name.
        """
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_trades(native)
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
        async for pushed_symbol, row in self._rows(stream):
            if pushed_symbol != native:
                continue
            # Binance dates neither the stream payload nor the call reply, so
            # arrival is the only timestamp there is. Stamped here rather than
            # left to default so every book in the feed is dated the same way.
            yield row.to_order_book(ticker, ts=time.time())

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
            yield row.to_kline(ticker).model_copy(
                update={"interval": canonical}
            )

    async def _best_quotes(self, ticker: UniversalTicker) -> AsyncIterator[BestQuote]:
        native = await self._resolve(ticker)
        stream = await self.feed.subscribe_book_tickers(native)
        async for row in self._rows(stream):
            if row.s != native:
                continue
            yield BestQuote(
                universal_ticker=str(ticker),
                bid=row.bid,
                bid_qty=row.bid_size,
                ask=row.ask,
                ask_qty=row.ask_size,
                # ``@bookTicker`` carries no timestamp — only ``u``, which
                # orders the messages without dating them.
                ts=time.time(),
            )

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


__all__ = [
    "BINANCE_INTERVALS",
    "DEFAULT_BOOK_LEVELS",
    "DEFAULT_BOOK_SPEED",
    "BinanceSpotPublicClient",
    "venue_interval",
]

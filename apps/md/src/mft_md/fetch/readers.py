"""Per-venue readers, composed here rather than behind a shared interface.

Each venue gets its own reader, assembled from the pieces its exchange module
offers. Gate needs REST and only REST: ``spot.candlesticks`` pushes the window
in progress and never what came before it, and ``spot.order_book`` pushes on a
timer rather than on demand — so both reads have to be asked for, and asking
has nothing to do with the socket. A venue that answers either over a socket
would compose that instead, in its own reader, without anything else having to
know.

That is why the composition lives on this side. ``mft.exchange.<venue>``
publishes connectors, not a contract — see :mod:`mft.exchange.base` — and the
shape below is what the fetch plane needs of them, stated by the fetch plane.

Nothing here opens a feed. A reader is built the first time its venue is asked
for and kept for the life of the process, so a query never waits on a
subscription and a venue nothing streams is queryable all the same.
"""

from __future__ import annotations

import logging
from typing import Protocol

from mft.exchange import venues
from mft.exchange.gate.spot.public import GATE_INTERVALS
from mft.exchange.gate.spot.rest import GATE_SPOT_REST_URL, GateSpotPublicRest
from mft.exchange.intervals import InvalidIntervalError, normalize_interval
from mft.exchange.models import BestQuote, Kline, OrderBook
from mft.exchange.symbols import SymbolResolver
from mft.exchange.tickers import UniversalTicker
from mft.symbols import SymbolClient

logger = logging.getLogger(__name__)


class VenueReader(Protocol):
    """What the fetch plane needs of a venue to answer a query.

    Every read is optional but ``connect``/``close``. A venue that cannot serve
    one has no method for it, and the plane refuses the query naming the venue
    rather than calling something that raises — same rule as the feeds.
    """

    venue: str

    async def connect(self) -> None: ...

    async def close(self) -> None: ...


class GateSpotReader:
    """Gate spot reads over REST, in canonical symbol and interval.

    Composes only :class:`GateSpotPublicRest`. The Gate connector that pairs it
    with a WebSocket exists for feeds and is not used here — connecting a
    socket to ask one REST question is the coupling this plane was built to
    avoid.
    """

    venue = "Gate"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        rest: GateSpotPublicRest | None = None,
        rest_url: str = GATE_SPOT_REST_URL,
    ) -> None:
        self.symbols = symbols
        self.rest = rest or GateSpotPublicRest(base_url=rest_url)

    async def connect(self) -> None:
        await self.rest.connect()

    async def close(self) -> None:
        await self.rest.close()

    async def _pair(self, ticker: UniversalTicker) -> tuple[str, str]:
        """``(canonical symbol, Gate pair)`` — resolved through the plane."""
        if ticker.venue != self.venue:
            raise ValueError(
                f"{self.venue} reader was handed a {ticker.venue} ticker: {ticker}"
            )
        return ticker.symbol, await self.symbols.exch_ticker(ticker)

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int
    ) -> list[Kline]:
        """Recent candles, oldest first, answering in the caller's spelling.

        Both the symbol and the interval are translated on the way down and
        stamped back on the way up, so Gate's ``BTC_USDT`` / ``30d`` vocabulary
        does not escape this method.
        """
        canonical_interval = normalize_interval(interval)
        gate_interval = GATE_INTERVALS.get(canonical_interval)
        if gate_interval is None:
            raise InvalidIntervalError(
                f"{self.venue} serves no {canonical_interval} candles; "
                f"supported: {sorted(GATE_INTERVALS)}"
            )
        symbol, pair = await self._pair(ticker)
        klines = await self.rest.fetch_klines(pair, gate_interval, limit=limit)
        return [
            kline.model_copy(
                update={"symbol": symbol, "interval": canonical_interval}
            )
            for kline in klines
        ]

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int
    ) -> OrderBook:
        """``GET /spot/order_book`` — a whole book, capped at ``depth``.

        Gate's reply carries no pair, so the caller's canonical one is stamped
        back on.
        """
        symbol, pair = await self._pair(ticker)
        book = await self.rest.fetch_order_book(pair, depth=depth)
        return book.model_copy(update={"symbol": symbol})

    async def fetch_best_quote(
        self, ticker: UniversalTicker
    ) -> BestQuote | None:
        """Top of book with sizes, or None when a side is empty.

        The same REST read as :meth:`fetch_order_book` at depth 1 — Gate serves
        no endpoint for the touch alone, and a second call to get two numbers
        out of a book we already have would be a round trip for nothing.

        None rather than zeros when a side has nothing resting. A caller asking
        for the touch is almost always checking whether its own price can rest
        against it, and a zero bid would answer that question wrongly rather
        than declining to answer it.
        """
        book = await self.fetch_order_book(ticker, depth=1)
        if not book.bids or not book.asks:
            return None
        bid, ask = book.bids[0], book.asks[0]
        return BestQuote(
            symbol=book.symbol,
            bid=bid.price,
            bid_qty=bid.qty,
            ask=ask.price,
            ask_qty=ask.qty,
            ts=book.ts,
        )


class ReaderFactory(Protocol):
    """Builds the reader for a venue name."""

    async def create(self, venue: str) -> VenueReader:
        """Build (but do not connect) a reader, or raise if the venue has none."""


class NoReaderError(Exception):
    """Nothing here can answer reads for this venue.

    Distinct from an empty answer, and raised before any call: a caller has to
    be able to tell "cannot ask" from "asked, and there is none".
    """


class VenueReaderFactory:
    """Venue name → reader. The only place the fetch plane names a venue.

    Every difference between venues is settled by which reader gets built, so
    nothing downstream branches on the venue again.
    """

    def __init__(self, symbols: SymbolClient) -> None:
        self._symbols = symbols

    async def create(self, venue: str) -> VenueReader:
        if venue == venues.GATE.name:
            return GateSpotReader(symbols=self._symbols)
        if venue == venues.PAPER.name:
            # The paper engine's book lives in another process and its prices
            # are invented tick by tick; nothing here can be read out of band.
            raise NoReaderError("the paper venue serves no on-demand reads")
        raise NoReaderError(f"no reader for venue {venue!r}")

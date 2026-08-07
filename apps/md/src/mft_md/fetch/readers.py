"""Per-venue candle readers, composed here rather than behind an interface.

Each venue gets its own reader, assembled from the pieces its exchange module
offers. Gate needs REST and only REST: ``spot.candlesticks`` pushes the window
in progress and never what came before it, so history has to be asked for, and
asking has nothing to do with the socket. A venue whose history *does* arrive
over a socket would compose that instead, in its own reader, without anything
else having to know.

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

from mft.exchange.gate.spot.public import GATE_INTERVALS
from mft.exchange.gate.spot.rest import GATE_SPOT_REST_URL, GateSpotPublicRest
from mft.exchange.intervals import InvalidIntervalError, normalize_interval
from mft.exchange.models import Kline
from mft.exchange.symbols import SymbolResolver, canonical
from mft.symbols import SymbolClient

logger = logging.getLogger(__name__)


class KlineReader(Protocol):
    """What the fetch plane needs of a venue to answer a candle query."""

    venue: str

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def fetch_klines(
        self, symbol: str, interval: str, *, limit: int
    ) -> list[Kline]: ...


class GateSpotKlineReader:
    """Gate spot candles over REST, in canonical symbol and interval.

    Composes only :class:`GateSpotPublicRest`. The Gate connector that pairs it
    with a WebSocket exists for feeds and is not used here — connecting a
    socket to ask one REST question is the coupling this plane was built to
    avoid.
    """

    venue = "gate_spot"

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

    async def fetch_klines(
        self, symbol: str, interval: str, *, limit: int
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
                f"gate_spot serves no {canonical_interval} candles; "
                f"supported: {sorted(GATE_INTERVALS)}"
            )
        symbol = canonical(symbol)
        pair = await self.symbols.exch_ticker(self.venue, symbol, category="spot")
        klines = await self.rest.fetch_klines(pair, gate_interval, limit=limit)
        return [
            kline.model_copy(
                update={"symbol": symbol, "interval": canonical_interval}
            )
            for kline in klines
        ]


class ReaderFactory(Protocol):
    """Builds the candle reader for a venue name."""

    async def create(self, venue: str) -> KlineReader:
        """Build (but do not connect) a reader, or raise if the venue has none."""


class NoHistoryError(Exception):
    """The venue keeps no candle history at all.

    Distinct from an empty answer, and raised before any call: the paper engine
    invents prices tick by tick and has no past to return, which a caller has to
    be able to tell from "asked, and there is none that far back".
    """


class VenueReaderFactory:
    """Venue name → reader. The only place the fetch plane names a venue.

    Every difference between venues is settled by which reader gets built, so
    nothing downstream branches on the venue again.
    """

    def __init__(self, symbols: SymbolClient) -> None:
        self._symbols = symbols

    async def create(self, venue: str) -> KlineReader:
        if venue == "gate_spot":
            return GateSpotKlineReader(symbols=self._symbols)
        if venue == "paper":
            raise NoHistoryError(
                "the paper engine invents prices tick by tick and keeps no past"
            )
        raise NoHistoryError(f"no candle reader for venue {venue!r}")

"""The gate_spot market-data connector.

Not an implementation of a shared public interface — there is none; see
:mod:`mft.exchange.base`. The methods here resemble the other venues' because
the same questions get asked of every venue, not because anything enforces it,
and where Gate differs the difference shows up in this file rather than being
flattened away.

Composes the two transports, same split as the private client:

* **WebSocket** — every live feed. Gate pushes ticker, tape, book snapshots,
  candles and best quote on one socket, so all five streams share it.
* **REST** — the on-demand reads. A caller asking for "the book right now"
  cannot wait for the channel's next scheduled push, and one asking for the
  last 500 candles cannot wait at all: ``spot.candlesticks`` pushes the window
  in progress and never what came before it. Gate has no request-reply for any
  of these over the WebSocket.

Gate serves all five feeds MD knows about. Most venues cover a different
subset, and a venue that does not publish one simply has no method for it here
rather than a stub that raises.

Symbols cross this boundary canonical (``BTCUSDT``) and are resolved to Gate's
``BTC_USDT`` through the symbol plane, never by string surgery — see
:mod:`mft.exchange.symbols`.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TypeVar

from mft.exchange.base import BaseClient
from mft.exchange.gate.spot.client import GATE_SPOT_WS_URL, GateSpotWebSocket
from mft.exchange.gate.spot.rest import GATE_SPOT_REST_URL, GateSpotPublicRest
from mft.exchange.intervals import InvalidIntervalError, normalize_interval
from mft.exchange.models import (
    BestQuote,
    Instrument,
    Kline,
    OrderBook,
    Ticker,
    Trade,
)
from mft.exchange.stream import EventStream
from mft.exchange.symbols import SymbolResolver, canonical

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: ``spot.order_book`` arguments. Gate caps this channel at 5/10/20/50/100
#: levels on a fixed timer, and every push is a whole book — which is what MD
#: wants. The diff channel (``spot.order_book_update``) would need snapshot
#: folding and sequence tracking to produce the same thing.
DEFAULT_BOOK_LEVEL = "20"
DEFAULT_BOOK_INTERVAL = "1000ms"

#: Canonical interval → Gate's spelling, for the candle windows Gate serves.
#:
#: Nearly an identity map, and deliberately written out anyway. Gate happens to
#: accept the canonical spelling for all but the month — it takes ``7d`` and
#: ``1w`` alike, but rejects ``1M`` and wants ``30d`` — and this table is what
#: keeps that "happens to" from becoming an assumption. It is also the only
#: place that knows which windows Gate serves at all: an interval absent here
#: is refused before the round trip rather than after a 400.
GATE_INTERVALS: dict[str, str] = {
    "1s": "1s",
    "10s": "10s",
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
    "1mo": "30d",
}


class GateSpotPublicClient(BaseClient):
    """Gate spot market data for MD.

    All five feeds are live::

        client = GateSpotPublicClient(symbols=symbol_client)
        async with client:
            async for book in client.stream_order_book("BTCUSDT"):
                ...
    """

    name = "gate_spot"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        ws_url: str = GATE_SPOT_WS_URL,
        rest_url: str = GATE_SPOT_REST_URL,
        ws: GateSpotWebSocket | None = None,
        rest: GateSpotPublicRest | None = None,
        book_level: str = DEFAULT_BOOK_LEVEL,
        book_interval: str = DEFAULT_BOOK_INTERVAL,
    ) -> None:
        super().__init__()
        # No credentials: every channel used here is public, and the socket
        # only needs keys for private subscribes and trading calls.
        self.ws = ws or GateSpotWebSocket(url=ws_url)
        self.rest = rest or GateSpotPublicRest(base_url=rest_url)
        self.symbols = symbols
        self.book_level = book_level
        self.book_interval = book_interval

    # --- lifecycle ---------------------------------------------------------

    async def connect(self) -> None:
        await self.ws.connect()
        await self.rest.connect()
        self._connected = True
        logger.info("gate_spot public connected")

    async def close(self) -> None:
        self._connected = False
        await self.ws.close()
        await self.rest.close()

    # --- snapshots (REST) --------------------------------------------------

    async def fetch_instruments(self) -> list[Instrument]:
        """Tradeable pairs, in Gate's own ``BTC_USDT`` spelling.

        Left native on purpose: this is what the symbol plane ingests to *build*
        the canonical mapping, so it cannot depend on that mapping existing.
        """
        self._ensure_connected()
        return await self.rest.fetch_instruments()

    async def fetch_ticker(self, symbol: str) -> Ticker:
        self._ensure_connected()
        symbol, pair = await self._resolve(symbol)
        ticker = await self.rest.fetch_ticker(pair)
        return ticker.model_copy(update={"symbol": symbol})

    async def fetch_order_book(self, symbol: str, *, depth: int = 10) -> OrderBook:
        self._ensure_connected()
        symbol, pair = await self._resolve(symbol)
        book = await self.rest.fetch_order_book(pair, depth=depth)
        return book.model_copy(update={"symbol": symbol})

    async def fetch_klines(
        self, symbol: str, interval: str, *, limit: int = 100
    ) -> list[Kline]:
        """Recent candles, oldest first, in canonical symbol and interval.

        Both spellings are translated on the way down and stamped back on the
        way up, so what a caller passes in is what comes back out — REST
        answers in Gate's ``BTC_USDT`` / ``30d`` vocabulary and none of it
        escapes this method.
        """
        self._ensure_connected()
        canonical_interval = normalize_interval(interval)
        gate_interval = GATE_INTERVALS.get(canonical_interval)
        if gate_interval is None:
            raise InvalidIntervalError(
                f"gate_spot serves no {canonical_interval} candles; "
                f"supported: {sorted(GATE_INTERVALS)}"
            )
        symbol, pair = await self._resolve(symbol)
        klines = await self.rest.fetch_klines(pair, gate_interval, limit=limit)
        return [
            kline.model_copy(
                update={"symbol": symbol, "interval": canonical_interval}
            )
            for kline in klines
        ]

    # --- streams (WebSocket) -----------------------------------------------

    def stream_ticker(self, symbol: str) -> AsyncIterator[Ticker]:
        self._ensure_connected()
        return self._tickers(symbol)

    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        self._ensure_connected()
        return self._trades(symbol)

    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        return self._order_books(symbol)

    def stream_kline(self, symbol: str, interval: str) -> AsyncIterator[Kline]:
        self._ensure_connected()
        return self._klines(symbol, interval)

    def stream_best_quote(self, symbol: str) -> AsyncIterator[BestQuote]:
        self._ensure_connected()
        return self._best_quotes(symbol)

    async def _tickers(self, symbol: str) -> AsyncIterator[Ticker]:
        symbol, pair = await self._resolve(symbol)
        stream = await self.ws.subscribe_tickers(pair)
        async for row in self._rows(stream):
            if row.currency_pair != pair:
                continue
            yield row.to_ticker().model_copy(update={"symbol": symbol})

    async def _trades(self, symbol: str) -> AsyncIterator[Trade]:
        symbol, pair = await self._resolve(symbol)
        stream = await self.ws.subscribe_trades(pair)
        async for row in self._rows(stream):
            if row.currency_pair != pair:
                continue
            yield row.to_trade().model_copy(update={"symbol": symbol})

    async def _order_books(self, symbol: str) -> AsyncIterator[OrderBook]:
        symbol, pair = await self._resolve(symbol)
        stream = await self.ws.subscribe_order_book(
            pair, level=self.book_level, interval=self.book_interval
        )
        async for row in self._rows(stream):
            if row.s != pair:
                continue
            yield row.to_order_book().model_copy(update={"symbol": symbol})

    async def _klines(self, symbol: str, interval: str) -> AsyncIterator[Kline]:
        symbol, pair = await self._resolve(symbol)
        stream = await self.ws.subscribe_candlesticks(interval, pair)
        async for row in self._rows(stream):
            if row.currency_pair != pair or row.interval != interval:
                continue
            yield Kline(
                symbol=symbol,
                interval=interval,
                # ``t`` is the window's open time, in seconds.
                open_time=float(row.t),
                open=row.o,
                high=row.h,
                low=row.low,
                close=row.c,
                # Gate splits the two: ``a`` is base amount, ``v`` is turnover
                # in the quote currency.
                volume=row.a,
                quote_volume=row.v,
                closed=row.w,
            )

    async def _best_quotes(self, symbol: str) -> AsyncIterator[BestQuote]:
        symbol, pair = await self._resolve(symbol)
        stream = await self.ws.subscribe_book_ticker(pair)
        async for row in self._rows(stream):
            if row.s != pair:
                continue
            yield BestQuote(
                symbol=symbol,
                bid=row.bid,
                bid_qty=row.bid_size,
                ask=row.ask,
                ask_qty=row.ask_size,
                ts=row.ts,
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

    async def _resolve(self, symbol: str) -> tuple[str, str]:
        """``(canonical, venue pair)`` for one instrument.

        Resolved once per stream rather than per message: the pair we
        subscribed with is the pair every message we keep carries, so there is
        nothing left to look up on the hot path.
        """
        symbol = canonical(symbol)
        pair = await self.symbols.exch_ticker(self.name, symbol, category="spot")
        return symbol, pair


__all__ = ["GateSpotPublicClient"]

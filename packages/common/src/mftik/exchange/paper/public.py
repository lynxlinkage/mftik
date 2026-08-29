"""Paper exchange public client — market data req-reply + streams."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mftik.exchange.base import BaseClient
from mftik.exchange.models import OrderBook, Ticker, Trade
from mftik.exchange.paper.models import PaperListed
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import Category, UniversalTicker

if TYPE_CHECKING:
    from mftik.exchange.paper.engine import PaperExchange


class PaperPublicClient(BaseClient):
    """Fake public venue client backed by :class:`PaperExchange`.

    Reads take a universal ticker, like every other public connector; the
    engine underneath only knows symbols, so the category is checked and then
    dropped.
    """

    name = "Paper"
    category = Category.SPOT

    def __init__(self, exchange: PaperExchange) -> None:
        super().__init__()
        self._exchange = exchange
        self._streams: list[EventStream[Any]] = []

    async def connect(self) -> None:
        await self._exchange.start()
        self._connected = True

    async def close(self) -> None:
        for stream in self._streams:
            await stream.aclose()
        self._streams.clear()
        self._connected = False

    # --- request-reply -----------------------------------------------------

    async def fetch_instruments(self) -> list[PaperListed]:
        self._ensure_connected()
        return self._exchange.list_instruments()

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        return self._exchange.get_ticker(self._symbol(ticker))

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 10
    ) -> OrderBook:
        self._ensure_connected()
        return self._exchange.get_order_book(self._symbol(ticker), depth=depth)

    # --- streams -----------------------------------------------------------

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        self._ensure_connected()
        return self._track(self._exchange.subscribe_ticker(self._symbol(ticker)))

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        self._ensure_connected()
        return self._track(self._exchange.subscribe_trades(self._symbol(ticker)))

    def stream_order_book(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        return self._track(
            self._exchange.subscribe_order_book(self._symbol(ticker))
        )

    def _track(self, stream: EventStream[Any]) -> EventStream[Any]:
        """Hold the stream so :meth:`close` can shut it down."""
        self._streams.append(stream)
        return stream

    def _symbol(self, ticker: UniversalTicker) -> str:
        if (ticker.venue, ticker.category) != (self.name, self.category):
            raise ValueError(f"{self.name} client was handed {ticker}")
        return ticker.symbol

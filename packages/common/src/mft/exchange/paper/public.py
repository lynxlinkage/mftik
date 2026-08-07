"""Paper exchange public client — market data req-reply + streams."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from mft.exchange.base import BaseClient
from mft.exchange.models import Instrument, OrderBook, Ticker, Trade
from mft.exchange.stream import EventStream

if TYPE_CHECKING:
    from mft.exchange.paper.engine import PaperExchange


class PaperPublicClient(BaseClient):
    """Fake public venue client backed by :class:`PaperExchange`."""

    name = "paper"

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

    async def fetch_instruments(self) -> list[Instrument]:
        self._ensure_connected()
        return self._exchange.list_instruments()

    async def fetch_ticker(self, symbol: str) -> Ticker:
        self._ensure_connected()
        return self._exchange.get_ticker(symbol)

    async def fetch_order_book(self, symbol: str, *, depth: int = 10) -> OrderBook:
        self._ensure_connected()
        return self._exchange.get_order_book(symbol, depth=depth)

    # --- streams -----------------------------------------------------------

    def stream_ticker(self, symbol: str) -> AsyncIterator[Ticker]:
        self._ensure_connected()
        stream = self._exchange.subscribe_ticker(symbol)
        self._streams.append(stream)
        return stream

    def stream_trades(self, symbol: str) -> AsyncIterator[Trade]:
        self._ensure_connected()
        stream = self._exchange.subscribe_trades(symbol)
        self._streams.append(stream)
        return stream

    def stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        stream = self._exchange.subscribe_order_book(symbol)
        self._streams.append(stream)
        return stream

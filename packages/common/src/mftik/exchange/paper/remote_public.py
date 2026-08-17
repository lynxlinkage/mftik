"""Redis-backed paper public client — talks to the paper-engine service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from mftik.broker import Broker
from mftik.exchange.base import BaseClient
from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import Instrument, OrderBook, Ticker, Trade
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.protocol import (
    PAPER_ERROR,
    PAPER_FETCH_INSTRUMENTS,
    PAPER_FETCH_ORDER_BOOK,
    PAPER_FETCH_TICKER,
    PAPER_ORDER_BOOK,
    Envelope,
    PaperFetchOrderBookRequest,
    PaperFetchTickerRequest,
    Topics,
    UntypedEnvelope,
)


class PaperRemotePublicClient(BaseClient):
    """The paper connector for other processes: engine RPC + pub/sub streams."""

    name = "Paper"
    category = Category.SPOT

    def __init__(self, broker: Broker) -> None:
        super().__init__()
        self._broker = broker
        self._stream_stops: list[asyncio.Event] = []

    async def connect(self) -> None:
        # Public MD needs no auth; probe instruments to confirm paper is up.
        reply = await self._rpc(
            PAPER_FETCH_INSTRUMENTS,
            UntypedEnvelope.wrap({}, type=PAPER_FETCH_INSTRUMENTS, source="md"),
        )
        self._raise_if_error(reply)
        self._connected = True

    async def close(self) -> None:
        for stop in self._stream_stops:
            stop.set()
        self._stream_stops.clear()
        self._connected = False

    async def fetch_instruments(self) -> list[Instrument]:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_FETCH_INSTRUMENTS,
            UntypedEnvelope.wrap({}, type=PAPER_FETCH_INSTRUMENTS, source="md"),
        )
        self._raise_if_error(reply)
        rows = reply.payload.get("instruments", [])
        return [Instrument.model_validate(r) for r in rows]

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_FETCH_TICKER,
            Envelope[PaperFetchTickerRequest].wrap(
                PaperFetchTickerRequest(symbol=self._symbol(ticker)),
                type=PAPER_FETCH_TICKER,
                source="md",
            ),
        )
        self._raise_if_error(reply)
        return Ticker.model_validate(reply.payload)

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 10
    ) -> OrderBook:
        self._ensure_connected()
        reply = await self._rpc(
            PAPER_FETCH_ORDER_BOOK,
            Envelope[PaperFetchOrderBookRequest].wrap(
                PaperFetchOrderBookRequest(
                    symbol=self._symbol(ticker), depth=depth
                ),
                type=PAPER_FETCH_ORDER_BOOK,
                source="md",
            ),
        )
        self._raise_if_error(reply)
        return OrderBook.model_validate(reply.payload)

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        raise NotImplementedError("paper remote ticker stream not wired yet")

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        raise NotImplementedError("paper remote trades stream not wired yet")

    def stream_order_book(self, ticker: UniversalTicker) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        return self._stream_order_book(self._symbol(ticker))

    def _symbol(self, ticker: UniversalTicker) -> str:
        if (ticker.venue, ticker.category) != (self.name, self.category):
            raise ExchangeError(f"{self.name} client was handed {ticker}")
        return ticker.symbol

    async def _stream_order_book(self, symbol: str) -> AsyncIterator[OrderBook]:
        stop = asyncio.Event()
        self._stream_stops.append(stop)
        topic = Topics.paper_order_book(symbol)
        async for env in self._broker.subscribe(topic, stop=stop):
            if env.type == PAPER_ORDER_BOOK:
                yield OrderBook.model_validate(env.payload)

    async def _rpc(self, _type: str, envelope: Envelope[Any]) -> UntypedEnvelope:
        return await self._broker.request(Topics.PAPER, envelope)

    def _raise_if_error(self, reply: UntypedEnvelope) -> None:
        if reply.type != PAPER_ERROR:
            return
        message = str(reply.payload.get("message", "paper engine error"))
        raise ExchangeError(message)

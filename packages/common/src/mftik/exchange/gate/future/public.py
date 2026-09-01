"""Gate USDT-perpetual market-data connector.

One public socket plus REST for on-demand reads. Liquidations exist here;
aggregated tape does not — there is no ``stream_agg_trades``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import TypeVar

from mftik.exchange.base import BaseClient
from mftik.exchange.gate.future.client import (
    GATE_FUTURES_WS_URL,
    GateFuturesWebSocket,
)
from mftik.exchange.gate.future.rest import (
    GATE_FUTURES_REST_URL,
    GateFuturesPublicRest,
)
from mftik.exchange.intervals import InvalidIntervalError, normalize_interval
from mftik.exchange.models import (
    BestQuote,
    FundingRate,
    Kline,
    Liquidation,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.stream import EventStream
from mftik.exchange.symbols import SymbolResolver
from mftik.exchange.tickers import UniversalTicker

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_BOOK_LEVEL = "20"
DEFAULT_BOOK_INTERVAL = "1000ms"

#: Canonical interval → Gate futures spelling.
GATE_FUTURES_INTERVALS: dict[str, str] = {
    "10s": "10s",
    "30s": "30s",
    "1m": "1m",
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
    "2d": "2d",
    "1w": "7d",
    "1mo": "30d",
}


def venue_interval(interval: str) -> str:
    canonical = normalize_interval(interval)
    found = GATE_FUTURES_INTERVALS.get(canonical)
    if found is None:
        raise InvalidIntervalError(
            f"GateFutures serves no {canonical} candles; "
            f"supported: {sorted(GATE_FUTURES_INTERVALS)}"
        )
    return found


class GateFuturesPublicClient(BaseClient):
    """Gate USDT-perpetual market data for MD."""

    name = "GateFutures"

    def __init__(
        self,
        *,
        symbols: SymbolResolver,
        ws_url: str = GATE_FUTURES_WS_URL,
        rest_url: str = GATE_FUTURES_REST_URL,
        ws: GateFuturesWebSocket | None = None,
        rest: GateFuturesPublicRest | None = None,
        book_level: str = DEFAULT_BOOK_LEVEL,
        book_interval: str = DEFAULT_BOOK_INTERVAL,
    ) -> None:
        super().__init__()
        self.ws = ws or GateFuturesWebSocket(url=ws_url)
        self.rest = rest or GateFuturesPublicRest(base_url=rest_url)
        self.symbols = symbols
        self.book_level = book_level
        self.book_interval = book_interval

    async def connect(self) -> None:
        await self.ws.connect()
        await self.rest.connect()
        self._connected = True
        logger.info("GateFutures public connected")

    async def close(self) -> None:
        self._connected = False
        await self.ws.close()
        await self.rest.close()

    async def fetch_ticker(self, ticker: UniversalTicker) -> Ticker:
        self._ensure_connected()
        return await self.rest.fetch_ticker(
            await self._resolve(ticker), ticker=ticker
        )

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 10
    ) -> OrderBook:
        self._ensure_connected()
        return await self.rest.fetch_order_book(
            await self._resolve(ticker),
            ticker=ticker,
            contract_size=await self._multiplier(ticker),
            depth=depth,
        )

    async def fetch_klines(
        self, ticker: UniversalTicker, interval: str, *, limit: int = 100
    ) -> list[Kline]:
        self._ensure_connected()
        canonical = normalize_interval(interval)
        gate_interval = venue_interval(canonical)
        rows = await self.rest.fetch_klines(
            await self._resolve(ticker),
            gate_interval,
            ticker=ticker,
            contract_size=await self._multiplier(ticker),
            limit=limit,
        )
        return [row.model_copy(update={"interval": canonical}) for row in rows]

    def stream_ticker(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        self._ensure_connected()
        return self._tickers(ticker)

    def stream_trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        self._ensure_connected()
        return self._trades(ticker)

    def stream_order_book(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OrderBook]:
        self._ensure_connected()
        return self._order_books(ticker)

    def stream_kline(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        self._ensure_connected()
        return self._klines(ticker, interval)

    def stream_best_quote(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[BestQuote]:
        self._ensure_connected()
        return self._best_quotes(ticker)

    def stream_liquidation(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        self._ensure_connected()
        return self._liquidations(ticker)

    def stream_funding_rate(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        """``futures.tickers`` — one contract, shared with the quote.

        A late joiner is silent until the next ``funding_rate``-bearing
        push; nothing is REST-filled. The subscribe payload is exactly
        that one contract so the wire key matches ``stream_ticker``.
        """
        self._ensure_connected()
        return self._funding_rates(ticker)

    async def _tickers(self, ticker: UniversalTicker) -> AsyncIterator[Ticker]:
        pair = await self._resolve(ticker)
        stream = await self.ws.subscribe_tickers(pair)
        async for row, _ts in self._rows(stream):
            if row.contract != pair:
                continue
            yield row.to_ticker(ticker)

    async def _trades(self, ticker: UniversalTicker) -> AsyncIterator[Trade]:
        pair = await self._resolve(ticker)
        size = await self._multiplier(ticker)
        stream = await self.ws.subscribe_trades(pair)
        async for row in self._rows(stream):
            if row.contract != pair:
                continue
            yield row.to_trade(ticker, size)

    async def _order_books(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[OrderBook]:
        pair = await self._resolve(ticker)
        size = await self._multiplier(ticker)
        stream = await self.ws.subscribe_order_book(
            pair, level=self.book_level, interval=self.book_interval
        )
        async for row in self._rows(stream):
            if row.contract and row.contract != pair:
                continue
            yield row.to_order_book(ticker, size)

    async def _klines(
        self, ticker: UniversalTicker, interval: str
    ) -> AsyncIterator[Kline]:
        pair = await self._resolve(ticker)
        size = await self._multiplier(ticker)
        gate_interval = venue_interval(interval)
        stream = await self.ws.subscribe_candlesticks(gate_interval, pair)
        async for row in self._rows(stream):
            if row.contract and row.contract != pair:
                continue
            if row.interval and row.interval != gate_interval:
                continue
            yield row.to_kline(ticker, normalize_interval(interval), size)

    async def _best_quotes(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[BestQuote]:
        pair = await self._resolve(ticker)
        size = await self._multiplier(ticker)
        stream = await self.ws.subscribe_book_ticker(pair)
        async for row in self._rows(stream):
            contract = row.contract
            if contract and contract != pair:
                continue
            yield row.to_best_quote(ticker, size)

    async def _liquidations(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[Liquidation]:
        pair = await self._resolve(ticker)
        size = await self._multiplier(ticker)
        stream = await self.ws.subscribe_liquidations(pair)
        async for row in self._rows(stream):
            if row.contract != pair:
                continue
            yield row.to_liquidation(ticker, size)

    async def _funding_rates(
        self, ticker: UniversalTicker
    ) -> AsyncIterator[FundingRate]:
        pair = await self._resolve(ticker)
        stream = await self.ws.subscribe_tickers(pair)
        async for row, ts in self._rows(stream):
            if row.contract != pair:
                continue
            funding = row.to_funding_rate(ticker, ts=ts)
            if funding is None:
                continue
            yield funding

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

    async def _multiplier(self, ticker: UniversalTicker) -> Decimal:
        size = await self.symbols.contract_size(ticker)
        if size is None or size <= 0:
            raise ValueError(f"no contract_size for {ticker}")
        return size


__all__ = [
    "GATE_FUTURES_INTERVALS",
    "GateFuturesPublicClient",
    "venue_interval",
]

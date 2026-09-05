"""Bitget public streams — one socket per ``instType`` (V4).

Public data only. ``spot``, ``usdt-futures`` and ``usdc-futures`` are
different connections carrying the same topic names. :class:`BitgetPublicClient`
opens them on first use.

**The order book is a fold.** Bitget sends one ``snapshot`` and then
``update`` pushes. :class:`BitgetBook` applies them; a zero quantity
deletes a level.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange.bitget import channels as ch
from mftik.exchange.bitget.models import (
    BitgetLiquidation,
    BitgetOrderBook,
    BitgetPublicTrade,
    BitgetTicker,
)
from mftik.exchange.bitget.protocol import (
    SUBSCRIBE,
    BitgetResponse,
    subscribe_frame,
)
from mftik.exchange.bitget.socket import DEFAULT_PING_INTERVAL, BitgetSocket
from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import UniversalTicker
from mftik.exchange.wire import WireLedger

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_BOOK_CHANNEL = ch.BOOKS

ArgKey = tuple[str, str, str, str]
Parse = Callable[[BitgetResponse, dict[str, Any] | list[Any]], Any]


class BitgetBookSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    symbol: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts: float = Field(default_factory=time.time)

    def to_order_book(self, ticker: UniversalTicker) -> OrderBook:
        return OrderBook(
            universal_ticker=str(ticker),
            bids=self.bids,
            asks=self.asks,
            ts=self.ts,
        )


@dataclass
class _Sub:
    args: tuple[dict[str, Any], ...]
    stream: EventStream[Any]
    parse: Parse
    index: frozenset[ArgKey] = field(default_factory=frozenset)
    folder: bool = False


class BitgetBook:
    """A local book folded from one channel's snapshot and updates."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.stale = True

    def apply(self, payload: BitgetOrderBook, action: str) -> bool:
        if action == "snapshot" or (self.stale and action != "update"):
            self._reset(payload)
            return True
        if self.stale:
            return False
        self._merge(self._bids, payload.bid_levels())
        self._merge(self._asks, payload.ask_levels())
        return True

    def _reset(self, payload: BitgetOrderBook) -> None:
        self._bids = {level.price: level.qty for level in payload.bid_levels()}
        self._asks = {level.price: level.qty for level in payload.ask_levels()}
        self.stale = False

    @staticmethod
    def _merge(side: dict[Decimal, Decimal], levels: list[BookLevel]) -> None:
        for level in levels:
            if level.qty <= 0:
                side.pop(level.price, None)
            else:
                side[level.price] = level.qty

    def snapshot(self, *, ts: float = 0.0) -> BitgetBookSnapshot:
        return BitgetBookSnapshot(
            symbol=self.symbol,
            bids=[
                BookLevel(price=price, qty=qty)
                for price, qty in sorted(self._bids.items(), reverse=True)
            ],
            asks=[
                BookLevel(price=price, qty=qty)
                for price, qty in sorted(self._asks.items())
            ],
            ts=ts or time.time(),
        )


class BitgetPublicStream(BitgetSocket):
    """Bitget market-data pushes for one public ``instType`` socket."""

    name = "bitget.public"

    def __init__(
        self,
        url: str,
        *,
        inst_type: str = "",
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
        )
        self.inst_type = inst_type
        self._subs: list[_Sub] = []
        self._books: dict[ArgKey, BitgetBook] = {}
        self._ledger: WireLedger[ArgKey] = WireLedger()

    async def subscribe_trades(self, inst_type: str, symbol: str):
        return await self._subscribe(
            (ch.public_trade(inst_type, symbol),),
            lambda _resp, row: BitgetPublicTrade.model_validate(
                {**row, "symbol": row.get("symbol") or symbol}
            ),
        )

    async def subscribe_tickers(self, inst_type: str, symbol: str):
        return await self._subscribe(
            (ch.ticker(inst_type, symbol),),
            lambda _resp, row: BitgetTicker.model_validate(
                {**row, "symbol": row.get("symbol") or symbol}
            ),
        )

    async def subscribe_best_quote(self, inst_type: str, symbol: str):
        return await self._subscribe(
            (ch.books(inst_type, symbol, topic=ch.BOOKS1),),
            lambda _resp, row: BitgetOrderBook.model_validate(
                {**row, "symbol": row.get("symbol") or symbol}
            ),
        )

    async def subscribe_klines(self, inst_type: str, symbol: str, interval: str):
        return await self._subscribe(
            (ch.kline(inst_type, symbol, interval),),
            lambda _resp, row: row,
        )

    async def subscribe_liquidations(self, inst_type: str):
        return await self._subscribe(
            (ch.liquidation(inst_type),),
            lambda _resp, row: BitgetLiquidation.model_validate(row),
        )

    async def subscribe_order_book(
        self, inst_type: str, symbol: str, *, topic: str = DEFAULT_BOOK_CHANNEL
    ):
        self._ensure_connected()
        arg = ch.books(inst_type, symbol, topic=topic)
        key = ch.arg_key(arg)
        by_key = {key: arg}

        async def send(keys: list[ArgKey]) -> None:
            wanted = [by_key[k] for k in keys]
            frame, req_id = subscribe_frame(wanted)
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire([key], send)
        stream: EventStream[BitgetBookSnapshot] = EventStream(on_close=self._drop)
        book = self._books.setdefault(key, BitgetBook(symbol))
        if not book.stale:
            stream.push(book.snapshot())
        self._subs.append(
            _Sub(
                args=(arg,),
                stream=stream,
                parse=self._fold_book,
                index=frozenset((key,)),
                folder=True,
            )
        )
        return stream

    def _fold_book(
        self, resp: BitgetResponse, row: dict[str, Any] | list[Any]
    ) -> BitgetBookSnapshot | None:
        if not isinstance(row, dict):
            return None
        key = ch.arg_key(resp.arg)
        book = self._books.get(key)
        if book is None:
            book = self._books.setdefault(key, BitgetBook(resp.symbol))
        payload = BitgetOrderBook.model_validate(
            {**row, "symbol": row.get("symbol") or resp.symbol}
        )
        action = resp.action or "snapshot"
        if not book.apply(payload, action):
            return None
        return book.snapshot(ts=payload.ts or time.time())

    async def _subscribe(
        self, args: tuple[dict[str, Any], ...], parse: Parse
    ) -> EventStream[T]:
        self._ensure_connected()
        by_key = {ch.arg_key(arg): arg for arg in args}

        async def send(keys: list[ArgKey]) -> None:
            wanted = [by_key[key] for key in keys]
            frame, req_id = subscribe_frame(wanted)
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire([ch.arg_key(arg) for arg in args], send)
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _Sub(
                args=args,
                stream=stream,
                parse=parse,
                index=frozenset(ch.arg_key(arg) for arg in args),
            )
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]
        live = {key for sub in self._subs if sub.folder for key in sub.index}
        for key in list(self._books):
            if key not in live:
                self._books.pop(key)

    def _wanted(self) -> list[dict[str, Any]]:
        seen: dict[ArgKey, dict[str, Any]] = {}
        for sub in self._subs:
            for arg in sub.args:
                seen.setdefault(ch.arg_key(arg), arg)
        return list(seen.values())

    async def _restore(self) -> None:
        args = self._wanted()
        self._ledger.clear()
        if not args:
            return
        for key, book in list(self._books.items()):
            self._books[key] = BitgetBook(book.symbol)
        by_key = {ch.arg_key(arg): arg for arg in args}

        async def send(keys: list[ArgKey]) -> None:
            wanted = [by_key[key] for key in keys]
            frame, req_id = subscribe_frame(wanted)
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire([ch.arg_key(arg) for arg in args], send)
        logger.info("%s resubscribed %s channels", self.name, len(args))

    def _push(self, resp: BitgetResponse) -> None:
        key = ch.arg_key(resp.arg)
        if resp.topic == ch.KLINE:
            data = resp.data if isinstance(resp.data, list) else []
            for sub in [s for s in self._subs if key in s.index]:
                for row in data:
                    try:
                        parsed = sub.parse(resp, row)
                    except Exception:
                        logger.exception(
                            "%s failed to parse kline payload: %r", self.name, row
                        )
                        continue
                    if parsed is not None:
                        sub.stream.push(parsed)
            return
        rows = resp.rows()
        if not rows:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if key in s.index]:
            for row in rows:
                try:
                    parsed = sub.parse(resp, row)
                except Exception:
                    logger.exception(
                        "%s failed to parse %s payload: %r",
                        self.name,
                        resp.topic,
                        row,
                    )
                    continue
                if parsed is not None:
                    sub.stream.push(parsed)

    def _teardown(self) -> None:
        self._ledger.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()
        self._books.clear()


__all__ = [
    "DEFAULT_BOOK_CHANNEL",
    "BitgetBook",
    "BitgetBookSnapshot",
    "BitgetPublicStream",
]

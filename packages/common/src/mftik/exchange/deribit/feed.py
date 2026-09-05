"""Deribit public streams — one socket for every book (V4).

Public data only. Spot and linear perps share the connection; the
channel carries ``instrument_name``. :class:`DeribitPublicClient` opens
it on first use.

**The order book is a fold.** The first frame (or a frame with no
``prev_change_id``) is a snapshot. Later frames are incremental and
must chain ``prev_change_id`` onto the last ``change_id``. A gap marks
the book stale and resubscribes; it does not invent levels.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from mftik.exchange.deribit import channels as ch
from mftik.exchange.deribit.models import (
    DeribitOrderBook,
    DeribitPublicTrade,
    DeribitQuote,
    DeribitTicker,
)
from mftik.exchange.deribit.protocol import DeribitResponse, rpc_frame
from mftik.exchange.deribit.socket import DEFAULT_PING_INTERVAL, DeribitSocket
from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import UniversalTicker
from mftik.exchange.wire import WireLedger, first_seen

logger = logging.getLogger(__name__)

T = TypeVar("T")

Parse = Callable[[DeribitResponse, dict[str, Any] | list[Any]], Any]


class DeribitBookSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    instrument: str
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
    channels: tuple[str, ...]
    stream: EventStream[Any]
    parse: Parse
    index: frozenset[str] = field(default_factory=frozenset)
    folder: bool = False


class DeribitBook:
    """A local book folded from one channel's snapshot and updates."""

    def __init__(self, instrument: str) -> None:
        self.instrument = instrument
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.change_id: int | None = None
        self.stale = True
        self.resyncing = False

    def apply(self, payload: DeribitOrderBook) -> bool:
        snapshot = payload.prev_change_id is None or self.stale
        if snapshot:
            self._reset(payload)
            return True
        if (
            self.change_id is not None
            and payload.prev_change_id != self.change_id
        ):
            self.stale = True
            return False
        self._merge(self._bids, payload.bid_levels())
        self._merge(self._asks, payload.ask_levels())
        self.change_id = payload.change_id
        return True

    def _reset(self, payload: DeribitOrderBook) -> None:
        self._bids = {level.price: level.qty for level in payload.bid_levels()}
        self._asks = {level.price: level.qty for level in payload.ask_levels()}
        self.change_id = payload.change_id
        self.stale = False
        self.resyncing = False

    @staticmethod
    def _merge(side: dict[Decimal, Decimal], levels: list[BookLevel]) -> None:
        for level in levels:
            if level.qty <= 0:
                side.pop(level.price, None)
            else:
                side[level.price] = level.qty

    def snapshot(self, *, ts: float = 0.0) -> DeribitBookSnapshot:
        return DeribitBookSnapshot(
            instrument=self.instrument,
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


class DeribitPublicStream(DeribitSocket):
    """Deribit market-data pushes for the one public socket."""

    name = "deribit.public"

    def __init__(
        self,
        url: str,
        *,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        heartbeat: int = 0,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
            heartbeat=heartbeat,
        )
        self._subs: list[_Sub] = []
        self._books: dict[str, DeribitBook] = {}
        self._ledger: WireLedger[str] = WireLedger()

    async def _on_open(self) -> None:
        await self._enable_heartbeat()

    async def subscribe_trades(self, instrument: str):
        return await self._subscribe(
            (ch.trades(instrument),),
            lambda _resp, row: DeribitPublicTrade.model_validate(
                {**row, "instrument_name": row.get("instrument_name") or instrument}
            ),
        )

    async def subscribe_tickers(self, instrument: str):
        return await self._subscribe(
            (ch.ticker(instrument),),
            lambda _resp, row: DeribitTicker.model_validate(
                {**row, "instrument_name": row.get("instrument_name") or instrument}
            ),
        )

    async def subscribe_best_quote(self, instrument: str):
        return await self._subscribe(
            (ch.quote(instrument),),
            lambda _resp, row: DeribitQuote.model_validate(
                {**row, "instrument_name": row.get("instrument_name") or instrument}
            ),
        )

    async def subscribe_klines(self, instrument: str, resolution: str):
        return await self._subscribe(
            (ch.kline(instrument, resolution),),
            lambda _resp, row: row,
        )

    async def subscribe_order_book(self, instrument: str):
        self._ensure_connected()
        channel = ch.book(instrument)

        async def send(keys: list[str]) -> None:
            frame, req_id = rpc_frame(ch.PUBLIC_SUBSCRIBE, {"channels": list(keys)})
            await self.request(frame, req_id, op=ch.PUBLIC_SUBSCRIBE)

        await self._ledger.acquire([channel], send)
        stream: EventStream[DeribitBookSnapshot] = EventStream(on_close=self._drop)
        book = self._books.setdefault(channel, DeribitBook(instrument))
        if not book.stale:
            stream.push(book.snapshot())
        self._subs.append(
            _Sub(
                channels=(channel,),
                stream=stream,
                parse=self._fold_book,
                index=frozenset((channel,)),
                folder=True,
            )
        )
        return stream

    def _fold_book(
        self, resp: DeribitResponse, row: dict[str, Any] | list[Any]
    ) -> DeribitBookSnapshot | None:
        if not isinstance(row, dict):
            return None
        channel = resp.channel
        book = self._books.get(channel)
        if book is None:
            book = self._books.setdefault(
                channel, DeribitBook(ch.instrument_of(channel))
            )
        payload = DeribitOrderBook.model_validate(row)
        if not book.apply(payload):
            if not book.resyncing:
                book.resyncing = True
                asyncio.create_task(
                    self._resync_book(channel, book),
                    name=f"{self.name}-book-resync",
                )
            return None
        return book.snapshot(ts=payload.timestamp or time.time())

    async def _resync_book(self, channel: str, book: DeribitBook) -> None:
        """Unsubscribe and subscribe again so the venue sends a fresh snapshot."""
        try:
            unsub, req_id = rpc_frame(
                ch.PUBLIC_UNSUBSCRIBE, {"channels": [channel]}
            )
            await self.request(unsub, req_id, op=ch.PUBLIC_UNSUBSCRIBE)
            self._books[channel] = DeribitBook(book.instrument)
            sub, req_id = rpc_frame(ch.PUBLIC_SUBSCRIBE, {"channels": [channel]})
            await self.request(sub, req_id, op=ch.PUBLIC_SUBSCRIBE)
        except Exception:
            logger.exception("%s book resync failed for %s", self.name, channel)
            book.resyncing = False

    async def _subscribe(
        self, channels: tuple[str, ...], parse: Parse
    ) -> EventStream[T]:
        self._ensure_connected()

        async def send(keys: list[str]) -> None:
            frame, req_id = rpc_frame(
                ch.PUBLIC_SUBSCRIBE, {"channels": list(keys)}
            )
            await self.request(frame, req_id, op=ch.PUBLIC_SUBSCRIBE)

        await self._ledger.acquire(list(channels), send)
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _Sub(
                channels=channels,
                stream=stream,
                parse=parse,
                index=frozenset(channels),
            )
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]
        live = {key for sub in self._subs if sub.folder for key in sub.index}
        for key in list(self._books):
            if key not in live:
                self._books.pop(key)

    def _wanted(self) -> list[str]:
        return first_seen(channel for sub in self._subs for channel in sub.channels)

    async def _restore(self) -> None:
        channels = self._wanted()
        self._ledger.clear()
        if not channels:
            return
        for key, book in list(self._books.items()):
            self._books[key] = DeribitBook(book.instrument)

        async def send(keys: list[str]) -> None:
            frame, req_id = rpc_frame(
                ch.PUBLIC_SUBSCRIBE, {"channels": list(keys)}
            )
            await self.request(frame, req_id, op=ch.PUBLIC_SUBSCRIBE)

        await self._ledger.acquire(channels, send)
        logger.info("%s resubscribed %s channels", self.name, len(channels))

    def _push(self, resp: DeribitResponse) -> None:
        key = resp.channel
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
                        key,
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


__all__ = ["DeribitBook", "DeribitBookSnapshot", "DeribitPublicStream"]

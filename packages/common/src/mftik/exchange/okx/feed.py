"""OKX's public streams — ``wss://ws.okx.com:8443/ws/v5/public`` (and business).

Public data only. One socket carries every instrument: the ``instId`` is on
the subscribe arg, not in the URL. Candles live on the business socket —
:class:`OkxPublicClient` opens that one lazily.

**The order book is a fold, not a feed.** OKX sends one ``snapshot`` and then
``update`` pushes against it. :class:`OkxBook` applies them and watches
``seqId`` / ``prevSeqId`` for a gap; on one, the topic is re-subscribed so
the venue sends a fresh snapshot.
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

from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.okx import channels as ch
from mftik.exchange.okx.models import (
    OkxFundingRate,
    OkxLiquidation,
    OkxOpenInterest,
    OkxOrderBook,
    OkxPublicTrade,
    OkxTicker,
)
from mftik.exchange.okx.protocol import (
    SUBSCRIBE,
    UNSUBSCRIBE,
    OkxResponse,
    subscribe_frame,
)
from mftik.exchange.okx.socket import DEFAULT_PING_INTERVAL, OkxSocket
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import UniversalTicker
from mftik.exchange.wire import WireLedger

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_BOOK_CHANNEL = ch.BOOKS

Parse = Callable[[OkxResponse, dict[str, Any]], Any]


class OkxBookSnapshot(BaseModel):
    """One folded book, frozen at the moment a push completed it."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts: float = Field(default_factory=time.time)

    def to_order_book(
        self,
        ticker: UniversalTicker,
        *,
        contract_size: Decimal | None = None,
    ) -> OrderBook:
        bids = self.bids
        asks = self.asks
        if contract_size is not None:
            bids = [
                BookLevel(
                    price=level.price,
                    qty=level.qty * contract_size,
                )
                for level in bids
            ]
            asks = [
                BookLevel(
                    price=level.price,
                    qty=level.qty * contract_size,
                )
                for level in asks
            ]
        return OrderBook(
            universal_ticker=str(ticker),
            bids=bids,
            asks=asks,
            ts=self.ts,
        )


@dataclass
class _Sub:
    args: tuple[dict[str, Any], ...]
    stream: EventStream[Any]
    parse: Parse
    index: frozenset[tuple[str, str, str]] = field(default_factory=frozenset)
    folder: bool = False


class OkxBook:
    """A local book folded from one channel's snapshot and updates.

    * a ``snapshot`` replaces the book outright;
    * an ``update`` sets the levels it names and **deletes** those it names
      with a zero quantity;
    * ``prevSeqId`` of an update must equal the last ``seqId``, or a push
      was missed and every book built afterwards would be wrong.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.seq_id = -1
        self.stale = True
        self.resyncing = False

    def apply(self, payload: OkxOrderBook, action: str) -> bool:
        if action == "snapshot" or (self.stale and action != "update"):
            self._reset(payload)
            return True
        if self.stale:
            return False
        if payload.seq_id == self.seq_id:
            # Two folders share one book; the second ``apply`` is the same push.
            return True
        if payload.prev_seq_id != self.seq_id:
            logger.warning(
                "okx book gap on %s: have seqId=%s, got prevSeqId=%s",
                self.symbol,
                self.seq_id,
                payload.prev_seq_id,
            )
            self._bids.clear()
            self._asks.clear()
            self.stale = True
            return False
        self._merge(self._bids, payload.bid_levels())
        self._merge(self._asks, payload.ask_levels())
        self.seq_id = payload.seq_id
        return True

    def _reset(self, payload: OkxOrderBook) -> None:
        self._bids = {level.price: level.qty for level in payload.bid_levels()}
        self._asks = {level.price: level.qty for level in payload.ask_levels()}
        self.seq_id = payload.seq_id
        self.stale = False
        self.resyncing = False

    @staticmethod
    def _merge(side: dict[Decimal, Decimal], levels: list[BookLevel]) -> None:
        for level in levels:
            if level.qty <= 0:
                side.pop(level.price, None)
            else:
                side[level.price] = level.qty

    def snapshot(self, *, ts: float = 0.0) -> OkxBookSnapshot:
        return OkxBookSnapshot(
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


class OkxPublicStream(OkxSocket):
    """OKX market-data pushes for one socket (public or business)."""

    name = "okx.public"

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
        self._subs: list[_Sub] = []
        self._books: dict[tuple[str, str, str], OkxBook] = {}
        self._ledger: WireLedger[tuple[str, str, str]] = WireLedger()

    async def subscribe_trades(self, inst_id: str) -> EventStream[OkxPublicTrade]:
        return await self._subscribe(
            (ch.trades(inst_id),),
            lambda _resp, row: OkxPublicTrade.model_validate(row),
        )

    async def subscribe_tickers(self, inst_id: str) -> EventStream[OkxTicker]:
        """``tickers`` — a snapshot every push. A late joiner waits for the next."""
        return await self._subscribe(
            (ch.tickers(inst_id),),
            lambda _resp, row: OkxTicker.model_validate(row),
        )

    async def subscribe_best_quote(self, inst_id: str) -> EventStream[OkxOrderBook]:
        """``bbo-tbt`` — top of book, a snapshot every time.

        A late joiner waits for the next push; there is nothing to fold.
        """
        return await self._subscribe(
            (ch.bbo(inst_id),),
            lambda _resp, row: OkxOrderBook.model_validate(row),
        )

    async def subscribe_klines(self, inst_id: str, bar: str) -> EventStream[list[Any]]:
        """``candle<bar>`` — raw rows; the connector stamps the interval."""
        return await self._subscribe(
            (ch.candle(inst_id, bar),),
            lambda _resp, row: row,
        )

    async def subscribe_liquidations(
        self, inst_type: str
    ) -> EventStream[OkxLiquidation]:
        return await self._subscribe(
            (ch.liquidation(inst_type),),
            lambda _resp, row: OkxLiquidation.model_validate(row),
        )

    async def subscribe_funding_rate(
        self, inst_id: str
    ) -> EventStream[OkxFundingRate]:
        """``funding-rate`` — SWAP only at the connector; this is the wire."""
        return await self._subscribe(
            (ch.funding_rate(inst_id),),
            lambda _resp, row: OkxFundingRate.model_validate(row),
        )

    async def subscribe_open_interest(
        self, inst_id: str
    ) -> EventStream[OkxOpenInterest]:
        """``open-interest`` — SWAP (and dated futures) at the connector."""
        return await self._subscribe(
            (ch.open_interest(inst_id),),
            lambda _resp, row: OkxOpenInterest.model_validate(row),
        )

    async def subscribe_order_book(
        self, inst_id: str, *, channel: str = DEFAULT_BOOK_CHANNEL
    ) -> EventStream[OkxBookSnapshot]:
        """Folded books. ``books5`` is a snapshot every push; the 400-level
        channels are a snapshot then updates.

        A late joiner of a live fold is replayed the current book in the
        same step that attaches the stream. ``books5`` joiners can also
        just wait for the next push — every one is complete.
        """
        self._ensure_connected()
        arg = ch.books(inst_id, channel=channel)
        key = ch.arg_key(arg)
        by_key = {key: arg}

        async def send(keys: list[tuple[str, str, str]]) -> None:
            wanted = [by_key[k] for k in keys]
            frame, req_id = subscribe_frame(wanted)
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire([key], send)
        # Replay and append in one step so ``_push`` cannot land first.
        stream: EventStream[OkxBookSnapshot] = EventStream(on_close=self._drop)
        book = self._books.setdefault(key, OkxBook(inst_id))
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
        self, resp: OkxResponse, row: dict[str, Any]
    ) -> OkxBookSnapshot | None:
        key = ch.arg_key(resp.arg)
        book = self._books.get(key)
        if book is None:
            book = self._books.setdefault(key, OkxBook(resp.inst_id))
        payload = OkxOrderBook.model_validate(
            {**row, "instId": row.get("instId") or resp.inst_id}
        )
        action = resp.action or "snapshot"
        if not book.apply(payload, action):
            if not book.resyncing:
                book.resyncing = True
                self._resync(resp.arg)
            return None
        return book.snapshot(ts=payload.ts or time.time())

    def _resync(self, arg: dict[str, Any]) -> None:
        asyncio.create_task(self._force_resubscribe(arg), name=f"{self.name}-resync")

    async def _force_resubscribe(self, arg: dict[str, Any]) -> None:
        """End and restart one channel so the venue sends a fresh snapshot.

        Not a ledger open or close: the identity stays held. ``acquire``
        would no-op (already reserved); ``discard`` would free a
        co-reader's key. Everyone on the topic is blind for this RTT.
        """
        try:
            frame, req_id = subscribe_frame([arg], op=UNSUBSCRIBE)
            await self.request(frame, req_id, op=UNSUBSCRIBE)
            frame, req_id = subscribe_frame([arg])
            await self.request(frame, req_id, op=SUBSCRIBE)
        except Exception:
            logger.exception("%s failed to resync %s", self.name, arg)
            return
        logger.info("%s resynced %s after a book gap", self.name, arg)

    async def _subscribe(
        self, args: tuple[dict[str, Any], ...], parse: Parse
    ) -> EventStream[T]:
        self._ensure_connected()
        by_key = {ch.arg_key(arg): arg for arg in args}

        async def send(keys: list[tuple[str, str, str]]) -> None:
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
        """Live subscribe args, each identity once, in first-seen order."""
        seen: dict[tuple[str, str, str], dict[str, Any]] = {}
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
            self._books[key] = OkxBook(book.symbol)
        by_key = {ch.arg_key(arg): arg for arg in args}

        async def send(keys: list[tuple[str, str, str]]) -> None:
            wanted = [by_key[key] for key in keys]
            frame, req_id = subscribe_frame(wanted)
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire([ch.arg_key(arg) for arg in args], send)
        logger.info("%s resubscribed %s channels", self.name, len(args))

    def _push(self, resp: OkxResponse) -> None:
        key = ch.arg_key(resp.arg)
        rows = (
            resp.rows()
            if resp.channel != ch.BBO
            else (resp.rows() or ([resp.data] if isinstance(resp.data, dict) else []))
        )
        # candle payloads are lists of lists, not dicts
        if resp.channel.startswith("candle"):
            data = resp.data if isinstance(resp.data, list) else []
            for sub in [s for s in self._subs if key in s.index]:
                for row in data:
                    try:
                        parsed = sub.parse(resp, row)
                    except Exception:
                        logger.exception(
                            "%s failed to parse %s payload: %r",
                            self.name,
                            resp.channel,
                            row,
                        )
                        continue
                    if parsed is not None:
                        sub.stream.push(parsed)
            return
        if not rows:
            # bbo-tbt sometimes puts the book on data[0] already handled
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
                        resp.channel,
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
    "OkxBook",
    "OkxBookSnapshot",
    "OkxPublicStream",
]

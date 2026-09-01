"""Bybit's public streams — ``wss://stream.bybit.com/v5/public/<category>``.

Public data only, and **one socket per category**: spot, linear and inverse are
three connections carrying the same topic names for different instruments (see
:func:`~mftik.exchange.bybit.protocol.public_url`). No credential appears in the
protocol, so this class never grows an account.

One ``subscribe_*`` call yields one stream carrying everything the named topics
push. Bybit multiplexes per topic, so listing several symbols in one call is
cheaper than opening a socket each, and every push names the topic it came from
— which is how a message finds its symbol even when the payload has none.

**The order book is a fold, not a feed.** Bybit sends one ``snapshot`` and then
``delta`` pushes against it, so a consumer wanting whole books has to apply
them, and a missed message makes every book after it wrong rather than late.
:class:`BybitBook` does the applying and watches ``u`` for the gap; on one, the
topic is re-subscribed so the venue sends a fresh snapshot, because a book that
silently drifts is worse than a book that briefly stops.
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

from mftik.exchange.bybit import channels as ch
from mftik.exchange.bybit.models import (
    BybitKline,
    BybitLiquidation,
    BybitOrderBook,
    BybitPublicTrade,
    BybitTicker,
)
from mftik.exchange.bybit.protocol import (
    SPOT,
    SUBSCRIBE,
    UNSUBSCRIBE,
    BybitResponse,
    public_url,
    subscribe_frame,
)
from mftik.exchange.bybit.socket import DEFAULT_PING_INTERVAL, BybitSocket
from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.stream import EventStream
from mftik.exchange.tickers import UniversalTicker
from mftik.exchange.wire import WireLedger, assert_last_reader, first_seen

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Book depth to subscribe when a caller names none. 50 levels is Bybit's
#: middle option on every book and deep enough for anything reading shape
#: rather than just the touch.
DEFAULT_BOOK_DEPTH = 50

#: ``(topic, type, payload, envelope_ts_ms) -> model``. The topic is passed
#: because a kline payload carries no symbol and a book delta carries no
#: depth. Envelope ``ts`` is milliseconds; a missing stamp is ``0``.
Parse = Callable[[str, str, dict[str, Any], int], Any]


def _parse_ticker(
    _topic: str, _kind: str, row: dict[str, Any], ts: int
) -> tuple[BybitTicker, float]:
    """Row plus the envelope stamp, falling back to local receive time."""
    stamp = ts / 1000.0 if ts else time.time()
    return BybitTicker.model_validate(row), stamp


class BybitBookSnapshot(BaseModel):
    """One folded book, frozen at the moment a push completed it.

    The fold happens in the socket layer, which knows Bybit's spelling of a
    symbol and nothing else — an :class:`~mftik.exchange.models.OrderBook` states
    the *instrument*, which only the symbol plane can say. So what comes out of
    the feed is this, and the connector turns it into an ``OrderBook`` under
    the ticker it already resolved.

    A copy per push rather than the live :class:`BybitBook`, which is the whole
    reason this type exists: a consumer reads its stream through a queue, so
    handing out the mutable book would let every queued update convert to
    whatever the latest state happened to be by the time it was read — silently
    collapsing a burst of book changes into one.
    """

    model_config = ConfigDict(frozen=True)

    #: Bybit's spelling, off the topic. The connector filters on it.
    symbol: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    ts: float = Field(default_factory=time.time)

    def to_order_book(self, ticker: UniversalTicker) -> OrderBook:
        """The book as the platform states one, under ``ticker``."""
        return OrderBook(
            universal_ticker=str(ticker),
            bids=self.bids,
            asks=self.asks,
            ts=self.ts,
        )


@dataclass
class _Sub:
    """One live subscribe call: its topics, for replay, and its output."""

    topics: tuple[str, ...]
    stream: EventStream[Any]
    parse: Parse
    index: frozenset[str] = field(default_factory=frozenset)
    #: True when this stream is folding the topic into whole books.
    #: ``subscribe_book_deltas`` refuses a topic a folder already holds.
    folder: bool = False


class BybitBook:
    """A local book folded from one topic's snapshot and deltas.

    Bybit's rules, and why each one is here:

    * a ``snapshot`` replaces the book outright — it is the only message that
      can be trusted on its own;
    * a ``delta`` sets the levels it names and **deletes** those it names with
      a zero quantity, which is not the same as setting them to zero;
    * ``u`` increments by one per message on an unbroken stream, so a jump
      means something was missed and every book built afterwards would be
      wrong in a way nothing downstream could detect.

    ``u == 1`` is Bybit's one special case: it marks a service restart, and the
    message carrying it is a snapshot whatever its ``type`` says.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.update_id = 0
        #: True until the first snapshot arrives, and again after a gap.
        self.stale = True
        #: Whether a re-subscribe is already on its way to fixing that. Set by
        #: the feed, cleared here by the snapshot that answers it — without it,
        #: every delta arriving during the round trip would ask again.
        self.resyncing = False

    def apply(self, payload: BybitOrderBook, kind: str) -> bool:
        """Fold one push in. Returns whether the book is now usable.

        ``False`` means a gap was detected and the caller should re-subscribe;
        the book is left empty rather than half-applied, because a book that is
        wrong is worse than one that is absent.
        """
        if kind == "snapshot" or payload.u == 1 or (self.stale and kind != "delta"):
            self._reset(payload)
            return True
        if self.stale:
            # Deltas before the first snapshot have nothing to apply to.
            return False
        if payload.u == self.update_id:
            # Two folders share one book; ``_push`` calls apply once per
            # ``_Sub``. The second call is the same push, not a gap.
            return True
        if payload.u != self.update_id + 1:
            logger.warning(
                "bybit book gap on %s: have u=%s, got u=%s",
                self.symbol,
                self.update_id,
                payload.u,
            )
            self._bids.clear()
            self._asks.clear()
            self.stale = True
            return False
        self._merge(self._bids, payload.bid_levels())
        self._merge(self._asks, payload.ask_levels())
        self.update_id = payload.u
        return True

    def _reset(self, payload: BybitOrderBook) -> None:
        self._bids = {level.price: level.qty for level in payload.bid_levels()}
        self._asks = {level.price: level.qty for level in payload.ask_levels()}
        self.update_id = payload.u
        self.stale = False
        self.resyncing = False

    @staticmethod
    def _merge(side: dict[Decimal, Decimal], levels: list[BookLevel]) -> None:
        for level in levels:
            if level.qty <= 0:
                side.pop(level.price, None)
            else:
                side[level.price] = level.qty

    def snapshot(self, *, ts: float = 0.0) -> BybitBookSnapshot:
        """The folded book, bids high-to-low and asks low-to-high."""
        return BybitBookSnapshot(
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


class BybitPublicStream(BybitSocket):
    """Bybit market-data pushes for one category.

    ::

        async with BybitPublicStream(product="spot") as feed:
            trades = await feed.subscribe_trades("BTCUSDT", "ETHUSDT")
            books = await feed.subscribe_order_book("BTCUSDT")
            async for trade in trades:
                ...

    Symbols are passed in Bybit's uppercase spelling; topic names are built in
    :mod:`.channels`.
    """

    name = "bybit.public"

    def __init__(
        self,
        *,
        product: str = SPOT,
        url: str | None = None,
        testnet: bool = False,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        ping_interval: float = DEFAULT_PING_INTERVAL,
    ) -> None:
        super().__init__(
            url or public_url(product, testnet=testnet),
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            ping_interval=ping_interval,
        )
        #: Which of Bybit's books this socket carries. Not sent anywhere — it
        #: is in the URL — but kept because a book depth is only valid for some
        #: categories and the check is worth making locally.
        self.product = product
        self._subs: list[_Sub] = []
        #: topic → the book being folded for it.
        self._books: dict[str, BybitBook] = {}
        self._ledger: WireLedger[str] = WireLedger()

    # --- raw plumbing ------------------------------------------------------

    async def subscribe_raw(self, *topics: str) -> EventStream[dict[str, Any]]:
        """Subscribe by topic name and yield the raw payloads."""
        return await self._subscribe(topics, lambda _t, _k, row, _ts: row)

    async def unsubscribe(self, *topics: str) -> None:
        """Unsubscribe topics. Last-reader only.

        A co-reader or a wider ``_Sub`` that is only partly covered
        raises. Streams close even if the venue frame fails. The ledger
        key is discarded only after the venue acks — a rejected
        ``UNSUBSCRIBE`` leaves the name held, so the next subscribe
        does not send a duplicate Bybit would refuse.
        """
        wanted = frozenset(topics)
        assert_last_reader(
            {
                topic: [s.index for s in self._subs if topic in s.index]
                for topic in wanted
            }
        )
        try:
            frame, req_id = subscribe_frame(list(topics), op=UNSUBSCRIBE)
            await self.request(frame, req_id, op=UNSUBSCRIBE)
        finally:
            for topic in wanted:
                self._books.pop(topic, None)
            for sub in [s for s in self._subs if s.index <= wanted and s.index]:
                sub.stream.close()
        self._ledger.discard(topics)

    async def _acquire(self, topics: tuple[str, ...]) -> None:
        async def send(missing: list[str]) -> None:
            frame, req_id = subscribe_frame(list(missing))
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire(list(topics), send)

    async def _subscribe(self, topics: tuple[str, ...], parse: Parse) -> EventStream[T]:
        await self._acquire(topics)
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _Sub(
                topics=topics,
                stream=stream,
                parse=parse,
                index=frozenset(topics),
            )
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]
        self._forget_orphaned_books()

    def _forget_orphaned_books(self) -> None:
        """Drop folds nobody is reading, so a later folder does not replay them."""
        live = {topic for sub in self._subs if sub.folder for topic in sub.index}
        for topic in list(self._books):
            if topic not in live:
                self._books.pop(topic)

    def _topics_of(self, *, folder: bool) -> set[str]:
        return {
            topic for sub in self._subs if sub.folder is folder for topic in sub.index
        }

    # --- streams -----------------------------------------------------------

    async def subscribe_trades(self, *symbols: str) -> EventStream[BybitPublicTrade]:
        """``publicTrade.<symbol>`` — the tape, one row per aggressing order."""
        return await self._subscribe(
            tuple(ch.public_trade(s) for s in symbols),
            lambda _t, _k, row, _ts: BybitPublicTrade.model_validate(row),
        )

    async def subscribe_tickers(
        self, *symbols: str
    ) -> EventStream[tuple[BybitTicker, float]]:
        """``tickers.<symbol>`` — 24h stats.

        Snapshots on spot, deltas on the contract books; a delta carries only
        the fields that changed, so a row here can be almost empty. See
        :class:`~mftik.exchange.bybit.models.BybitTicker`.

        A late joiner is silent until the next push that carries the field
        it reads. The quote and the funding rate share this topic; neither
        is REST-filled for a joiner.

        Each yield is ``(row, ts)``. ``ts`` is the envelope stamp in seconds
        when the frame carried one, otherwise local receive time.
        """
        return await self._subscribe(
            tuple(ch.tickers(s) for s in symbols),
            _parse_ticker,
        )

    async def subscribe_klines(
        self, interval: str, *symbols: str
    ) -> EventStream[tuple[str, BybitKline]]:
        """``kline.<interval>.<symbol>`` — ``interval`` in Bybit's spelling.

        Yields ``(symbol, candle)``: the payload names no instrument, and the
        topic is the only place the symbol appears.
        """
        return await self._subscribe(
            tuple(ch.kline(s, interval) for s in symbols),
            lambda topic, _k, row, _ts: (
                ch.symbol_of(topic),
                BybitKline.model_validate(row),
            ),
        )

    async def subscribe_order_book(
        self, *symbols: str, depth: int = DEFAULT_BOOK_DEPTH
    ) -> EventStream[BybitBookSnapshot]:
        """``orderbook.<depth>.<symbol>`` — whole books, folded here.

        What arrives on the wire is a snapshot followed by deltas; what comes
        out of this stream is a complete book per push, because every consumer
        would otherwise have to fold them and only one of them could do it
        correctly per socket. A push that leaves the book unusable — a gap —
        yields nothing and triggers a re-subscribe instead.

        A late joiner of a live fold is replayed the current book in the
        same step that attaches the stream — no ``await`` between the
        replay and ``_subs.append``, so a later ``_push`` cannot land
        first. Joining a topic only a raw consumer holds is allowed: the
        fold starts stale and a deliberate resync draws a snapshot, at the
        cost of one RTT of blindness for everyone on the topic.
        """
        self._check_depth(depth)
        topics = tuple(ch.order_book(s, depth=depth) for s in symbols)
        await self._acquire(topics)
        # Replay, attach, and the optional raw-held resync are one
        # synchronous block. An await here would let ``_push`` enqueue a
        # newer book ahead of the snapshot the joiner is supposed to see first.
        stream: EventStream[BybitBookSnapshot] = EventStream(on_close=self._drop)
        for topic in topics:
            created = topic not in self._books
            book = self._books.setdefault(topic, BybitBook(ch.symbol_of(topic)))
            if not book.stale:
                stream.push(book.snapshot())
            elif created and topic in self._topics_of(folder=False):
                logger.info("%s folding a raw-held topic %s", self.name, topic)
                if not book.resyncing:
                    book.resyncing = True
                    self._resync(topic)
        self._subs.append(
            _Sub(
                topics=topics,
                stream=stream,
                parse=self._fold_book,
                index=frozenset(topics),
                folder=True,
            )
        )
        return stream

    async def subscribe_book_deltas(
        self, *symbols: str, depth: int = DEFAULT_BOOK_DEPTH
    ) -> EventStream[tuple[str, BybitOrderBook]]:
        """The same topic, unfolded — ``(type, payload)`` as Bybit sent it.

        For a caller keeping its own book, or measuring the update stream
        itself. ``type`` is ``snapshot`` or ``delta``, and telling them apart is
        the caller's problem from here on.

        Refuses a topic :meth:`subscribe_order_book` already folds — a
        joiner would see deltas from mid-stream forever and never recover.
        Two unfolded consumers share, with no replay.
        """
        self._check_depth(depth)
        topics = tuple(ch.order_book(s, depth=depth) for s in symbols)
        folders = sorted(self._topics_of(folder=True) & set(topics))
        if folders:
            raise ValueError(
                f"{self.name} subscribe_book_deltas cannot join "
                f"{', '.join(folders)}: that topic is already folded"
            )
        await self._acquire(topics)
        stream: EventStream[tuple[str, BybitOrderBook]] = EventStream(
            on_close=self._drop
        )
        self._subs.append(
            _Sub(
                topics=topics,
                stream=stream,
                parse=lambda _t, kind, row, _ts: (
                    kind,
                    BybitOrderBook.model_validate(row),
                ),
                index=frozenset(topics),
            )
        )
        return stream

    async def subscribe_best_quote(self, *symbols: str) -> EventStream[BybitOrderBook]:
        """``orderbook.1.<symbol>`` — top of book, on every change.

        Bybit's equivalent of a book-ticker feed, and a snapshot every time, so
        nothing is folded: depth 1 needs no history to be complete. A late
        joiner waits for the next push.
        """
        return await self._subscribe(
            tuple(ch.order_book(s, depth=1) for s in symbols),
            lambda _t, _k, row, _ts: BybitOrderBook.model_validate(row),
        )

    async def subscribe_liquidations(
        self, *symbols: str
    ) -> EventStream[BybitLiquidation]:
        """``allLiquidation.<symbol>`` — forced closes on the contract books.

        Spot has none; this topic only pushes on the linear and inverse
        sockets. See :class:`~mftik.exchange.bybit.models.BybitLiquidation`.
        """
        return await self._subscribe(
            tuple(ch.all_liquidation(s) for s in symbols),
            lambda _t, _k, row, _ts: BybitLiquidation.model_validate(row),
        )

    def _check_depth(self, depth: int) -> None:
        allowed = ch.BOOK_DEPTHS.get(self.product)
        if allowed and depth not in allowed:
            raise ValueError(
                f"Bybit {self.product} serves no {depth}-level book; "
                f"supported depths: {', '.join(str(d) for d in allowed)}"
            )

    def _fold_book(
        self, topic: str, kind: str, row: dict[str, Any], _ts: int = 0
    ) -> BybitBookSnapshot | None:
        """Apply one push to the book behind ``topic``, or ask for a new one."""
        book = self._books.get(topic)
        if book is None:
            book = self._books.setdefault(topic, BybitBook(ch.symbol_of(topic)))
        payload = BybitOrderBook.model_validate(row)
        if not book.apply(payload, kind):
            if not book.resyncing:
                book.resyncing = True
                self._resync(topic)
            return None
        return book.snapshot()

    def _resync(self, topic: str) -> None:
        """Re-subscribe a topic whose book has a gap, to draw a fresh snapshot.

        Bybit only sends a snapshot when a subscription starts, so the way back
        from a gap is to end the subscription and start it again. Done in a
        task because this runs inside the read loop, which must not block on a
        round trip it is itself supposed to deliver.

        Everyone on the topic is blind for that RTT — including a raw
        co-reader. That is the cost of one fold per socket, not a reason
        to resync per consumer.
        """
        asyncio.create_task(self._force_resubscribe(topic), name=f"{self.name}-resync")

    async def _force_resubscribe(self, topic: str) -> None:
        """End and restart one topic so the venue sends a fresh snapshot.

        This is not a ledger open or close. The identity stays held — a
        co-reader is still on it, and routing the ``SUBSCRIBE`` through
        ``acquire`` would no-op because the key is already reserved, so
        no snapshot would arrive. ``discard`` on the way out would mark
        the identity free and let the next ``acquire`` double-subscribe.
        """
        try:
            frame, req_id = subscribe_frame([topic], op=UNSUBSCRIBE)
            await self.request(frame, req_id, op=UNSUBSCRIBE)
            frame, req_id = subscribe_frame([topic])
            await self.request(frame, req_id, op=SUBSCRIBE)
        except Exception:
            logger.exception("%s failed to resync %s", self.name, topic)
            return
        logger.info("%s resynced %s after a book gap", self.name, topic)

    # --- socket hooks ------------------------------------------------------

    async def _restore(self) -> None:
        """Re-send every live subscription onto the fresh socket.

        The books are dropped first: whatever they held describes a connection
        that no longer exists, and Bybit opens the new subscription with a
        snapshot anyway.
        """
        topics = first_seen(topic for sub in self._subs for topic in sub.topics)
        self._ledger.clear()
        if not topics:
            return
        for topic in list(self._books):
            self._books[topic] = BybitBook(ch.symbol_of(topic))

        async def send(missing: list[str]) -> None:
            frame, req_id = subscribe_frame(list(missing))
            await self.request(frame, req_id, op=SUBSCRIBE)

        await self._ledger.acquire(topics, send)
        logger.info("%s resubscribed %s topics", self.name, len(topics))

    def _push(self, resp: BybitResponse) -> None:
        rows = resp.rows()
        if not rows:
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if resp.topic in s.index]:
            for row in rows:
                try:
                    parsed = sub.parse(resp.topic, resp.type, row, resp.ts)
                except Exception:
                    logger.exception(
                        "%s failed to parse %s payload: %r",
                        self.name,
                        resp.topic,
                        row,
                    )
                    continue
                # A fold that produced nothing is a gap being recovered from,
                # not a message to hand on.
                if parsed is not None:
                    sub.stream.push(parsed)

    def _teardown(self) -> None:
        self._ledger.clear()
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()
        self._books.clear()


__all__ = [
    "DEFAULT_BOOK_DEPTH",
    "BybitBook",
    "BybitBookSnapshot",
    "BybitPublicStream",
]

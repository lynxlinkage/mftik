"""Binance futures market streams — the push half of the venue.

Public data only: no credential appears anywhere in this protocol, so these
sockets have no notion of an account and never grow one. The account feed is a
listen key on a socket of its own (:mod:`.user`).

**One connection per traffic class, not one per venue.** Since Binance retired
``fstream.binance.com/ws`` in April 2026 there is no endpoint that carries
every feed: the book streams answer on ``/public`` and the tape, candles,
stats, mark price and liquidations on ``/market`` (see :mod:`.protocol`). A
subscribe sent to the wrong one is acknowledged and then silently never pushes,
which is the failure this class exists to make impossible — every stream name
goes through :func:`~.streams.group_of`, and the socket it belongs to is opened
on first use.

Lazily, for the same reason :class:`~mftik.exchange.bybit.public.BybitPublicClient`
opens its sockets per category: a session streaming only order books should not
hold a second connection to a host it never reads.

Each socket is a :class:`~mftik.exchange.binance.feed.BinanceStreamSocket`, so
the subscribe/replay/fan-out machinery — including the reconnect replay — is
the venue-wide one and is not restated here.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, TypeVar

from mftik.exchange.binance.feed import BinanceStreamSocket
from mftik.exchange.binance.feed import Parse as StreamParse
from mftik.exchange.binance.future import streams as st
from mftik.exchange.binance.future.models import (
    BinanceFutureAggTrade,
    BinanceFutureBookTicker,
    BinanceFutureDepthUpdate,
    BinanceFutureKlineEvent,
    BinanceFutureLiquidation,
    BinanceFutureMarkPrice,
    BinanceFutureTicker,
)
from mftik.exchange.binance.future.protocol import (
    BINANCE_FUTURE_MARKET_STREAM_URL,
    BINANCE_FUTURE_PUBLIC_STREAM_URL,
)
from mftik.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Default depth-stream shape. Binance caps the partial book at 5/10/20 levels
#: and pushes a whole book every tick, which is what MD wants; the diff stream
#: would need snapshot folding and sequence tracking to produce the same thing.
DEFAULT_BOOK_LEVELS = 20
DEFAULT_BOOK_SPEED = st.SPEED_100MS


class BinanceFutureStream:
    """Binance futures market-data pushes, across both stream endpoints.

    ::

        async with BinanceFutureStream() as feed:
            trades = await feed.subscribe_agg_trades("BTCUSDT", "ETHUSDT")
            books = await feed.subscribe_order_book("BTCUSDT")
            async for trade in trades:
                ...

    Symbols are passed in Binance's uppercase ``BTCUSDT`` spelling; the
    lowercasing stream names require happens in :mod:`.streams`.

    Not a socket subclass, unlike the spot feed: there is more than one socket
    here, and which one a subscribe lands on is decided per stream name.
    """

    name = "binance.future.stream"

    def __init__(
        self,
        *,
        public_url: str = BINANCE_FUTURE_PUBLIC_STREAM_URL,
        market_url: str = BINANCE_FUTURE_MARKET_STREAM_URL,
        sockets: dict[str, BinanceStreamSocket] | None = None,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        keepalive: float = 20.0,
    ) -> None:
        self._urls = {st.PUBLIC: public_url, st.MARKET: market_url}
        #: group → its live socket. Pre-seeded ones (a test's fakes) are used
        #: as they are and never replaced.
        self._sockets: dict[str, BinanceStreamSocket] = dict(sockets or {})
        self._options: dict[str, Any] = {
            "ack_timeout": ack_timeout,
            "reconnect": reconnect,
            "max_retries": max_retries,
            "retry_backoff": retry_backoff,
            "max_retry_backoff": max_retry_backoff,
            "keepalive": keepalive,
        }
        self._reconnect_cbs: list[Callable[[], Any]] = []
        self._connected = False
        #: Get-or-create for a group must be single-flight: two concurrent
        #: subscribe_* on the same endpoint would otherwise each build a
        #: socket, and the second assignment would orphan the first's pump.
        self._socket_lock = asyncio.Lock()

    # --- lifecycle ---------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        """Open nothing, and be ready to.

        A feed that eagerly dialled both endpoints would hold a connection to
        a host it may never read from — and which of the two it needs is not
        known until something subscribes. Pre-seeded sockets *are* connected
        here, because a caller that supplied one meant it to be used.
        """
        for socket in self._sockets.values():
            if not socket.connected:
                await socket.connect()
        self._connected = True

    async def close(self) -> None:
        self._connected = False
        for socket in self._sockets.values():
            await socket.close()
        self._sockets.clear()

    async def __aenter__(self) -> BinanceFutureStream:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    def on_reconnect(self, callback: Callable[[], Any]) -> None:
        """Hear about a reconnect on any of this feed's sockets.

        Kept as a list rather than handed straight to the sockets, because a
        socket that does not exist yet has to get it too: a feed that only ever
        read the book would otherwise miss the callback on the socket it opens
        later.
        """
        self._reconnect_cbs.append(callback)
        for socket in self._sockets.values():
            socket.on_reconnect(callback)

    async def socket_for(self, group: str) -> BinanceStreamSocket:
        """The socket carrying one traffic class, opening it on first ask."""
        async with self._socket_lock:
            socket = self._sockets.get(group)
            if socket is None:
                socket = BinanceStreamSocket(self._urls[group], **self._options)
                socket.name = f"{self.name}.{group}"
                for callback in self._reconnect_cbs:
                    socket.on_reconnect(callback)
                self._sockets[group] = socket
            if not socket.connected:
                await socket.connect()
            return socket

    # --- raw plumbing ------------------------------------------------------

    async def subscribe_raw(self, *names: str) -> EventStream[dict[str, Any]]:
        """Subscribe by stream name and yield the raw ``data`` payloads.

        Every name must belong to the same endpoint — they arrive on different
        connections otherwise, and one stream cannot read two sockets.
        """
        return await self._subscribe(names, lambda _name, row: row)

    async def unsubscribe(self, *names: str) -> None:
        """Unsubscribe stream names, each on the socket that carries it.

        A group that was never opened is a legal no-op — no ``_Sub`` can
        hold those names. A socket that exists is always asked, even if
        it is mid-reconnect: the local half of ``unsubscribe`` still
        closes the stream so restore cannot resurrect it. One group's
        send failure does not skip the rest — each socket is asked, then
        the first error is re-raised.
        """
        by_group: dict[str, list[str]] = {}
        for name in names:
            by_group.setdefault(st.group_of(name), []).append(name)
        failed: list[Exception] = []
        for group, group_names in by_group.items():
            socket = self._sockets.get(group)
            if socket is None:
                continue
            try:
                await socket.unsubscribe(*group_names)
            except Exception as exc:
                failed.append(exc)
        if failed:
            raise failed[0]

    async def _subscribe(
        self, names: tuple[str, ...], parse: StreamParse
    ) -> EventStream[T]:
        if not names:
            raise ValueError("subscribe needs at least one stream name")
        groups = {st.group_of(name) for name in names}
        if len(groups) > 1:
            raise ValueError(
                f"streams {list(names)} span {sorted(groups)} endpoints; "
                f"one subscription cannot read two sockets"
            )
        socket = await self.socket_for(groups.pop())
        return await socket.subscribe(names, parse)

    # --- public streams ----------------------------------------------------

    async def subscribe_agg_trades(
        self, *symbols: str
    ) -> EventStream[BinanceFutureAggTrade]:
        """``<symbol>@aggTrade`` — the tape, and futures' only one."""
        return await self._subscribe(
            tuple(st.agg_trade(s) for s in symbols),
            lambda _name, row: BinanceFutureAggTrade.model_validate(row),
        )

    async def subscribe_mark_prices(
        self, *symbols: str, speed: str = st.MARK_SPEED_1S
    ) -> EventStream[BinanceFutureMarkPrice]:
        """``<symbol>@markPrice`` — mark, index and the funding schedule."""
        return await self._subscribe(
            tuple(st.mark_price(s, speed=speed) for s in symbols),
            lambda _name, row: BinanceFutureMarkPrice.model_validate(row),
        )

    async def subscribe_klines(
        self, interval: str, *symbols: str
    ) -> EventStream[BinanceFutureKlineEvent]:
        """``<symbol>@kline_<interval>`` — ``interval`` in Binance's spelling."""
        return await self._subscribe(
            tuple(st.kline(s, interval) for s in symbols),
            lambda _name, row: BinanceFutureKlineEvent.model_validate(row),
        )

    async def subscribe_tickers(
        self, *symbols: str
    ) -> EventStream[BinanceFutureTicker]:
        """``<symbol>@ticker`` — rolling 24h stats, with no quote in them."""
        return await self._subscribe(
            tuple(st.ticker(s) for s in symbols),
            lambda _name, row: BinanceFutureTicker.model_validate(row),
        )

    async def subscribe_book_tickers(
        self, *symbols: str
    ) -> EventStream[BinanceFutureBookTicker]:
        """``<symbol>@bookTicker`` — best bid/ask on every change.

        Every push is a complete quote, so a late joiner waits for the
        next print. ``ticker`` and ``bestquote`` share this identity; that
        is enough, there is nothing to replay.
        """
        return await self._subscribe(
            tuple(st.book_ticker(s) for s in symbols),
            lambda _name, row: BinanceFutureBookTicker.model_validate(row),
        )

    async def subscribe_liquidations(
        self, *symbols: str
    ) -> EventStream[BinanceFutureLiquidation]:
        """``<symbol>@forceOrder`` — public liquidations, sampled once a second."""
        return await self._subscribe(
            tuple(st.force_order(s) for s in symbols),
            lambda _name, row: BinanceFutureLiquidation.model_validate(row),
        )

    async def subscribe_order_book(
        self,
        *symbols: str,
        levels: int = DEFAULT_BOOK_LEVELS,
        speed: str = DEFAULT_BOOK_SPEED,
    ) -> EventStream[BinanceFutureDepthUpdate]:
        """``<symbol>@depth<levels>`` — capped-depth snapshots on a timer.

        The payload is a ``depthUpdate``, the same shape the diff stream uses,
        and off *this* stream its sides are the book rather than changes to it
        — see :class:`~.models.BinanceFutureDepthUpdate`. It carries its own
        symbol, unlike spot's partial depth, so nothing has to be read off the
        stream name.
        """
        return await self._subscribe(
            tuple(st.depth(s, levels=levels, speed=speed) for s in symbols),
            lambda _name, row: BinanceFutureDepthUpdate.model_validate(row),
        )

    async def subscribe_depth_updates(
        self, *symbols: str, speed: str = DEFAULT_BOOK_SPEED
    ) -> EventStream[BinanceFutureDepthUpdate]:
        """``<symbol>@depth`` — depth diffs; apply in ``pu``/``u`` order."""
        return await self._subscribe(
            tuple(st.depth_diff(s, speed=speed) for s in symbols),
            lambda _name, row: BinanceFutureDepthUpdate.model_validate(row),
        )


__all__ = [
    "DEFAULT_BOOK_LEVELS",
    "DEFAULT_BOOK_SPEED",
    "BinanceFutureStream",
]

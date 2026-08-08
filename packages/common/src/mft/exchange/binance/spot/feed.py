"""Binance spot market streams — the push half of the venue.

Public data only. Binance keeps its market pushes on their own host with no
credential anywhere in the protocol, so this socket has no notion of an
account and never grows one; order entry and account pushes are the WebSocket
API's job (:mod:`.client`).

Always the **combined** endpoint (``/stream``). Binance offers a raw one where
the payload arrives bare, and it is unusable for anything with more than a
single subscription: partial depth carries no symbol at all, and telling two
symbols' books apart would mean one socket per symbol. The combined endpoint
wraps every push as ``{"stream": <name>, "data": {...}}``, so the stream name —
and with it the symbol, the window and the update speed — is on every message.

One ``subscribe_*`` call yields one stream carrying everything the named
streams push. Binance multiplexes per stream name rather than per symbol, so
listing several symbols in one call is cheaper than opening a stream each, and
each message says which symbol it is.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from mft.exchange.binance.spot import streams as st
from mft.exchange.binance.spot.models import (
    BinanceAggTrade,
    BinanceBookTicker,
    BinanceDepth,
    BinanceDepthUpdate,
    BinanceKlineEvent,
    BinanceTicker,
    BinanceTrade,
)
from mft.exchange.binance.spot.protocol import (
    BINANCE_SPOT_STREAM_URL,
    BinanceResponse,
    subscribe_frame,
)
from mft.exchange.binance.spot.socket import BinanceSocket
from mft.exchange.stream import EventStream

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: ``(stream name, payload) -> model``. The name is passed because partial
#: depth has no symbol of its own and would otherwise arrive anonymous.
Parse = Callable[[str, dict[str, Any]], Any]


@dataclass
class _Sub:
    """One live subscribe call: its stream names, for replay, and its output."""

    names: tuple[str, ...]
    stream: EventStream[Any]
    parse: Parse
    #: Set of names, for the per-message routing test.
    index: frozenset[str] = field(default_factory=frozenset)


class BinanceSpotStream(BinanceSocket):
    """Binance spot market-data pushes.

    ::

        async with BinanceSpotStream() as feed:
            trades = await feed.subscribe_agg_trades("BTCUSDT", "ETHUSDT")
            books = await feed.subscribe_order_book("BTCUSDT")
            async for trade in trades:
                ...

    Symbols are passed in Binance's uppercase ``BTCUSDT`` spelling; the
    lowercasing stream names require happens in :mod:`.streams`.
    """

    name = "binance.spot.stream"

    def __init__(
        self,
        *,
        url: str = BINANCE_SPOT_STREAM_URL,
        ack_timeout: float = 10.0,
        reconnect: bool = True,
        max_retries: int = 10,
        retry_backoff: float = 1.0,
        max_retry_backoff: float = 30.0,
        keepalive: float = 20.0,
    ) -> None:
        super().__init__(
            url,
            ack_timeout=ack_timeout,
            reconnect=reconnect,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            max_retry_backoff=max_retry_backoff,
            keepalive=keepalive,
        )
        self._subs: list[_Sub] = []

    # --- raw plumbing ------------------------------------------------------

    async def subscribe_raw(self, *names: str) -> EventStream[dict[str, Any]]:
        """Subscribe by stream name and yield the raw ``data`` payloads."""
        return await self._subscribe(names, lambda _name, row: row)

    async def unsubscribe(self, *names: str) -> None:
        """Unsubscribe stream names and close every stream reading them."""
        frame, req_id = subscribe_frame(st.UNSUBSCRIBE, list(names))
        await self.request(frame, req_id, method=st.UNSUBSCRIBE)
        wanted = frozenset(names)
        for sub in [s for s in self._subs if s.index & wanted]:
            # close() fires on_close → _drop, which does the removal.
            sub.stream.close()

    async def list_subscriptions(self) -> list[str]:
        """What this socket is currently subscribed to, per Binance."""
        frame, req_id = subscribe_frame(st.LIST_SUBSCRIPTIONS, [])
        resp = await self.request(frame, req_id, method=st.LIST_SUBSCRIPTIONS)
        return list(resp.result or [])

    async def _subscribe(
        self,
        names: tuple[str, ...],
        parse: Parse,
    ) -> EventStream[T]:
        frame, req_id = subscribe_frame(st.SUBSCRIBE, list(names))
        await self.request(frame, req_id, method=st.SUBSCRIBE)
        stream: EventStream[T] = EventStream(on_close=self._drop)
        self._subs.append(
            _Sub(
                names=names,
                stream=stream,
                parse=parse,
                index=frozenset(names),
            )
        )
        return stream

    def _drop(self, stream: EventStream[Any]) -> None:
        self._subs = [s for s in self._subs if s.stream is not stream]

    # --- public streams ----------------------------------------------------

    async def subscribe_agg_trades(
        self, *symbols: str
    ) -> EventStream[BinanceAggTrade]:
        """``<symbol>@aggTrade`` — the tape, same-price fills coalesced.

        The default tape for a feed: it carries the same volume as ``@trade``
        at a fraction of the message rate, and nothing above this layer
        distinguishes one match from several at one price.
        """
        return await self._subscribe(
            tuple(st.agg_trade(s) for s in symbols),
            lambda _name, row: BinanceAggTrade.model_validate(row),
        )

    async def subscribe_trades(self, *symbols: str) -> EventStream[BinanceTrade]:
        """``<symbol>@trade`` — the raw tape, one message per match."""
        return await self._subscribe(
            tuple(st.trade(s) for s in symbols),
            lambda _name, row: BinanceTrade.model_validate(row),
        )

    async def subscribe_klines(
        self, interval: str, *symbols: str
    ) -> EventStream[BinanceKlineEvent]:
        """``<symbol>@kline_<interval>`` — ``interval`` in Binance's spelling."""
        return await self._subscribe(
            tuple(st.kline(s, interval) for s in symbols),
            lambda _name, row: BinanceKlineEvent.model_validate(row),
        )

    async def subscribe_tickers(self, *symbols: str) -> EventStream[BinanceTicker]:
        """``<symbol>@ticker`` — rolling 24h stats."""
        return await self._subscribe(
            tuple(st.ticker(s) for s in symbols),
            lambda _name, row: BinanceTicker.model_validate(row),
        )

    async def subscribe_book_tickers(
        self, *symbols: str
    ) -> EventStream[BinanceBookTicker]:
        """``<symbol>@bookTicker`` — best bid/ask on every change."""
        return await self._subscribe(
            tuple(st.book_ticker(s) for s in symbols),
            lambda _name, row: BinanceBookTicker.model_validate(row),
        )

    async def subscribe_order_book(
        self,
        *symbols: str,
        levels: int = 20,
        speed: str = st.SPEED_100MS,
    ) -> EventStream[tuple[str, BinanceDepth]]:
        """``<symbol>@depth<levels>`` — capped-depth snapshots on a timer.

        Yields ``(symbol, book)`` rather than the book alone, because a partial
        depth payload names no instrument — see :class:`.models.BinanceDepth`.
        The symbol comes off the stream name, which the combined endpoint puts
        on every message.
        """
        return await self._subscribe(
            tuple(st.depth(s, levels=levels, speed=speed) for s in symbols),
            lambda name, row: (
                st.symbol_of(name),
                BinanceDepth.model_validate(row),
            ),
        )

    async def subscribe_depth_updates(
        self, *symbols: str, speed: str = st.SPEED_100MS
    ) -> EventStream[BinanceDepthUpdate]:
        """``<symbol>@depth`` — depth diffs; apply in ``U``/``u`` order."""
        return await self._subscribe(
            tuple(st.depth_diff(s, speed=speed) for s in symbols),
            lambda _name, row: BinanceDepthUpdate.model_validate(row),
        )

    # --- socket hooks ------------------------------------------------------

    async def _restore(self) -> None:
        """Re-send every live subscription onto the fresh socket.

        One frame for all of them: Binance takes a list, and a rejection that
        applies to one name fails the batch loudly rather than leaving a feed
        silently dead. The ack is awaited — :meth:`.socket.BinanceSocket.request`
        reads it inline here, because the read loop has not resumed yet.
        """
        names = [name for sub in self._subs for name in sub.names]
        if not names:
            return
        frame, req_id = subscribe_frame(st.SUBSCRIBE, names)
        await self.request(frame, req_id, method=st.SUBSCRIBE)
        logger.info("%s resubscribed %s streams", self.name, len(names))

    def _push(self, resp: BinanceResponse) -> None:
        if not resp.stream or not isinstance(resp.data, dict):
            logger.debug("%s ignoring push %r", self.name, resp)
            return
        for sub in [s for s in self._subs if resp.stream in s.index]:
            try:
                sub.stream.push(sub.parse(resp.stream, resp.data))
            except Exception:
                logger.exception(
                    "%s failed to parse %s payload: %r",
                    self.name,
                    resp.stream,
                    resp.data,
                )

    def _teardown(self) -> None:
        for sub in list(self._subs):
            sub.stream.close()
        self._subs.clear()


__all__ = ["BinanceSpotStream"]

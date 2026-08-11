"""Binance spot market streams — the push half of the venue.

Public data only. Binance keeps its market pushes on their own host with no
credential anywhere in the protocol, so this socket has no notion of an
account and never grows one; order entry and account pushes are the WebSocket
API's job (:mod:`.client`).

The subscribe/replay/fan-out machinery is
:class:`~mft.exchange.binance.feed.BinanceStreamSocket`, shared with futures —
including the reason both connect to the **combined** endpoint. What is spot's
own is below: which streams exist and what their payloads parse into.

One ``subscribe_*`` call yields one stream carrying everything the named
streams push. Binance multiplexes per stream name rather than per symbol, so
listing several symbols in one call is cheaper than opening a stream each, and
each message says which symbol it is.
"""

from __future__ import annotations

import logging

from mft.exchange.binance.feed import BinanceStreamSocket
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
from mft.exchange.binance.spot.protocol import BINANCE_SPOT_STREAM_URL
from mft.exchange.stream import EventStream

logger = logging.getLogger(__name__)


class BinanceSpotStream(BinanceStreamSocket):
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

    # --- public streams ----------------------------------------------------

    async def subscribe_agg_trades(
        self, *symbols: str
    ) -> EventStream[BinanceAggTrade]:
        """``<symbol>@aggTrade`` — the tape, same-price fills coalesced.

        The default tape for a feed: it carries the same volume as ``@trade``
        at a fraction of the message rate, and nothing above this layer
        distinguishes one match from several at one price.
        """
        return await self.subscribe(
            tuple(st.agg_trade(s) for s in symbols),
            lambda _name, row: BinanceAggTrade.model_validate(row),
        )

    async def subscribe_trades(self, *symbols: str) -> EventStream[BinanceTrade]:
        """``<symbol>@trade`` — the raw tape, one message per match."""
        return await self.subscribe(
            tuple(st.trade(s) for s in symbols),
            lambda _name, row: BinanceTrade.model_validate(row),
        )

    async def subscribe_klines(
        self, interval: str, *symbols: str
    ) -> EventStream[BinanceKlineEvent]:
        """``<symbol>@kline_<interval>`` — ``interval`` in Binance's spelling."""
        return await self.subscribe(
            tuple(st.kline(s, interval) for s in symbols),
            lambda _name, row: BinanceKlineEvent.model_validate(row),
        )

    async def subscribe_tickers(self, *symbols: str) -> EventStream[BinanceTicker]:
        """``<symbol>@ticker`` — rolling 24h stats."""
        return await self.subscribe(
            tuple(st.ticker(s) for s in symbols),
            lambda _name, row: BinanceTicker.model_validate(row),
        )

    async def subscribe_book_tickers(
        self, *symbols: str
    ) -> EventStream[BinanceBookTicker]:
        """``<symbol>@bookTicker`` — best bid/ask on every change."""
        return await self.subscribe(
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
        return await self.subscribe(
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
        return await self.subscribe(
            tuple(st.depth_diff(s, speed=speed) for s in symbols),
            lambda _name, row: BinanceDepthUpdate.model_validate(row),
        )


__all__ = ["BinanceSpotStream"]

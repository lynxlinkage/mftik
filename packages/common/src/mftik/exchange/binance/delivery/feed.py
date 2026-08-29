"""Binance COIN-M market streams — the push half of the venue.

Public data only. Binance keeps its market pushes on their own host with no
credential anywhere in the protocol, so this socket has no notion of an
account and never grows one; order entry and the account feed wait for the
private client.

The subscribe/replay/fan-out machinery is
:class:`~mftik.exchange.binance.feed.BinanceStreamSocket`, shared with spot
and USD-M. What is this market's own is below: which streams exist and
what their payloads parse into.

**One combined socket.** dapi was not part of the 2026 ``fstream`` split,
so every name answers on ``dstream.binance.com/stream``. There is no
public-vs-market table and no second connection to open.

One ``subscribe_*`` call yields one stream carrying everything the named
streams push. Symbols are passed in Binance's uppercase ``BTCUSD_PERP``
spelling; the lowercasing stream names require happens in :mod:`.streams`.
"""

from __future__ import annotations

from mftik.exchange.binance.delivery import streams as st
from mftik.exchange.binance.delivery.models import (
    BinanceDeliveryAggTrade,
    BinanceDeliveryBookTicker,
    BinanceDeliveryDepthUpdate,
    BinanceDeliveryKlineEvent,
    BinanceDeliveryLiquidation,
    BinanceDeliveryMarkPrice,
    BinanceDeliveryTicker,
)
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_STREAM_URL
from mftik.exchange.binance.feed import BinanceStreamSocket
from mftik.exchange.stream import EventStream

#: Default depth-stream shape. Binance caps the partial book at 5/10/20
#: levels and pushes a whole book every tick, which is what MD wants.
DEFAULT_BOOK_LEVELS = 20
DEFAULT_BOOK_SPEED = st.SPEED_100MS


class BinanceDeliveryStream(BinanceStreamSocket):
    """Binance COIN-M market-data pushes.

    ::

        async with BinanceDeliveryStream() as feed:
            trades = await feed.subscribe_agg_trades("BTCUSD_PERP")
            books = await feed.subscribe_order_book("BTCUSD_PERP")
            async for trade in trades:
                ...
    """

    name = "binance.delivery.stream"

    def __init__(
        self,
        *,
        url: str = BINANCE_DELIVERY_STREAM_URL,
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

    async def subscribe_agg_trades(
        self, *symbols: str
    ) -> EventStream[BinanceDeliveryAggTrade]:
        """``<symbol>@aggTrade`` — the tape, and this market's only one."""
        return await self.subscribe(
            tuple(st.agg_trade(s) for s in symbols),
            lambda _name, row: BinanceDeliveryAggTrade.model_validate(row),
        )

    async def subscribe_mark_prices(
        self, *symbols: str, speed: str = st.MARK_SPEED_1S
    ) -> EventStream[BinanceDeliveryMarkPrice]:
        """``<symbol>@markPrice`` — mark, index and the funding schedule.

        On the feed for later. MD does not subscribe this.
        """
        return await self.subscribe(
            tuple(st.mark_price(s, speed=speed) for s in symbols),
            lambda _name, row: BinanceDeliveryMarkPrice.model_validate(row),
        )

    async def subscribe_klines(
        self, interval: str, *symbols: str
    ) -> EventStream[BinanceDeliveryKlineEvent]:
        """``<symbol>@kline_<interval>`` — ``interval`` in Binance's spelling."""
        return await self.subscribe(
            tuple(st.kline(s, interval) for s in symbols),
            lambda _name, row: BinanceDeliveryKlineEvent.model_validate(row),
        )

    async def subscribe_tickers(
        self, *symbols: str
    ) -> EventStream[BinanceDeliveryTicker]:
        """``<symbol>@ticker`` — rolling 24h stats, with no quote in them."""
        return await self.subscribe(
            tuple(st.ticker(s) for s in symbols),
            lambda _name, row: BinanceDeliveryTicker.model_validate(row),
        )

    async def subscribe_book_tickers(
        self, *symbols: str
    ) -> EventStream[BinanceDeliveryBookTicker]:
        """``<symbol>@bookTicker`` — best bid/ask on every change."""
        return await self.subscribe(
            tuple(st.book_ticker(s) for s in symbols),
            lambda _name, row: BinanceDeliveryBookTicker.model_validate(row),
        )

    async def subscribe_liquidations(
        self, *symbols: str
    ) -> EventStream[BinanceDeliveryLiquidation]:
        """``<symbol>@forceOrder`` — public liquidations, sampled once a second."""
        return await self.subscribe(
            tuple(st.force_order(s) for s in symbols),
            lambda _name, row: BinanceDeliveryLiquidation.model_validate(row),
        )

    async def subscribe_order_book(
        self,
        *symbols: str,
        levels: int = DEFAULT_BOOK_LEVELS,
        speed: str = DEFAULT_BOOK_SPEED,
    ) -> EventStream[BinanceDeliveryDepthUpdate]:
        """``<symbol>@depth<levels>`` — capped-depth snapshots on a timer.

        The payload is a ``depthUpdate`` that carries its own symbol, and
        off *this* stream its sides are the book rather than changes to it.
        """
        return await self.subscribe(
            tuple(st.depth(s, levels=levels, speed=speed) for s in symbols),
            lambda _name, row: BinanceDeliveryDepthUpdate.model_validate(row),
        )


__all__ = [
    "DEFAULT_BOOK_LEVELS",
    "DEFAULT_BOOK_SPEED",
    "BinanceDeliveryStream",
]

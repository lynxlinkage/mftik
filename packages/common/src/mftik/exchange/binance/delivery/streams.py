"""Binance COIN-M market stream names.

A stream is named, not parameterised: the symbol, the window and the update
speed are all baked into one lowercase string, and subscribing means listing
those strings::

    {"method": "SUBSCRIBE", "params": ["btcusd_perp@aggTrade"], "id": "..."}

Every builder here lowercases the symbol. Binance rejects
``BTCUSD_PERP@aggTrade`` outright — stream names are case-sensitive even
though REST takes the symbol uppercase — so this is the one place that
difference is handled.

**One host, no routing table.** dapi was not part of the 2026 ``fstream``
split: every name below answers on ``dstream.binance.com/stream``. There is
no ``group_of``. A subscribe sent to that combined endpoint is the whole
story.
"""

from __future__ import annotations

from mftik.exchange.binance.feed import (
    LIST_SUBSCRIPTIONS,
    SUBSCRIBE,
    UNSUBSCRIBE,
)

#: Update speeds the depth streams accept. ``100ms`` is what a book consumer
#: wants and what this adapter asks for; Binance also serves ``250ms`` /
#: ``500ms``.
SPEED_100MS = "100ms"
SPEED_250MS = "250ms"
SPEED_500MS = "500ms"

#: Update speeds ``@markPrice`` accepts. Left off, Binance pushes every three
#: seconds.
MARK_SPEED_1S = "1s"
MARK_SPEED_3S = "3s"

#: Level counts ``@depth<levels>`` accepts. Anything else is a silent no-op:
#: Binance acknowledges the subscribe and then never pushes.
DEPTH_LEVELS = (5, 10, 20)


def agg_trade(symbol: str) -> str:
    """``btcusd_perp@aggTrade`` — the tape, same-price fills coalesced.

    The only tape this market publishes. There is no ``@trade`` stream.
    """
    return f"{symbol.lower()}@aggTrade"


def mark_price(symbol: str, *, speed: str = MARK_SPEED_1S) -> str:
    """``btcusd_perp@markPrice@1s`` — mark, index, funding rate, next funding.

    Named for a later consumer. MD does not subscribe this.
    """
    return f"{symbol.lower()}@markPrice@{speed}"


def kline(symbol: str, interval: str) -> str:
    """``btcusd_perp@kline_1m`` — ``interval`` is Binance's spelling, not ours."""
    return f"{symbol.lower()}@kline_{interval}"


def ticker(symbol: str) -> str:
    """``btcusd_perp@ticker`` — rolling 24h stats, and **no quote**.

    Same gap as USD-M: there is no bid and no ask. Anything needing a quote
    reads :func:`book_ticker`.
    """
    return f"{symbol.lower()}@ticker"


def book_ticker(symbol: str) -> str:
    """``btcusd_perp@bookTicker`` — best bid/ask on every change."""
    return f"{symbol.lower()}@bookTicker"


def force_order(symbol: str) -> str:
    """``btcusd_perp@forceOrder`` — public liquidations.

    One message per symbol per second at most: Binance pushes the largest
    liquidation in each 1000ms window and drops the rest.
    """
    return f"{symbol.lower()}@forceOrder"


def depth(symbol: str, *, levels: int = 20, speed: str = SPEED_100MS) -> str:
    """``btcusd_perp@depth20@100ms`` — a whole book, capped at ``levels``."""
    return f"{symbol.lower()}@depth{levels}@{speed}"


def symbol_of(stream: str) -> str:
    """The symbol a stream name was built for, uppercased.

    ``btcusd_perp@depth20@100ms`` → ``BTCUSD_PERP``.
    """
    return stream.split("@", 1)[0].upper()


__all__ = [
    "DEPTH_LEVELS",
    "LIST_SUBSCRIPTIONS",
    "MARK_SPEED_1S",
    "MARK_SPEED_3S",
    "SPEED_100MS",
    "SPEED_250MS",
    "SPEED_500MS",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "agg_trade",
    "book_ticker",
    "depth",
    "force_order",
    "kline",
    "mark_price",
    "symbol_of",
    "ticker",
]

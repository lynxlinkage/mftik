"""Binance spot market stream names.

A stream is named, not parameterised: the symbol, the window and the update
speed are all baked into one lowercase string, and subscribing means listing
those strings::

    {"method": "SUBSCRIBE", "params": ["btcusdt@aggTrade"], "id": "..."}

Every builder here lowercases the symbol. Binance rejects ``BTCUSDT@aggTrade``
outright — stream names are case-sensitive even though the REST and WebSocket
API take the symbol uppercase — so this is the one place that difference is
handled rather than something each caller remembers.

**Partial depth is the reason we always connect to the combined endpoint.**
Its payload is ``{"lastUpdateId", "bids", "asks"}`` with no symbol in it, so on
a raw socket a book arrives with no way to tell which instrument it belongs to.
The combined endpoint wraps every push as ``{"stream": <name>, "data": {...}}``,
which puts the name — and therefore the symbol — back on the message. See
:class:`~mftik.exchange.binance.spot.feed.BinanceSpotStream`.
"""

from __future__ import annotations

# The three subscribe verbs are the same on every Binance market-streams
# socket, so they are defined once with the plumbing that sends them and
# re-exported here, where a caller building stream names looks for them.
from mftik.exchange.binance.feed import (
    LIST_SUBSCRIPTIONS,
    SUBSCRIBE,
    UNSUBSCRIBE,
)

#: Update speeds the depth streams accept. ``1000ms`` is the default for the
#: diff stream, ``100ms`` for partial book depth.
SPEED_100MS = "100ms"
SPEED_1000MS = "1000ms"

#: Level counts ``@depth<levels>`` accepts. Anything else is a silent no-op:
#: Binance acknowledges the subscribe and then never pushes.
DEPTH_LEVELS = (5, 10, 20)


def agg_trade(symbol: str) -> str:
    """``btcusdt@aggTrade`` — the tape, with same-price fills coalesced."""
    return f"{symbol.lower()}@aggTrade"


def trade(symbol: str) -> str:
    """``btcusdt@trade`` — the raw tape, one message per match."""
    return f"{symbol.lower()}@trade"


def kline(symbol: str, interval: str) -> str:
    """``btcusdt@kline_1m`` — ``interval`` is Binance's spelling, not ours."""
    return f"{symbol.lower()}@kline_{interval}"


def ticker(symbol: str) -> str:
    """``btcusdt@ticker`` — rolling 24h stats, pushed about once a second."""
    return f"{symbol.lower()}@ticker"


def book_ticker(symbol: str) -> str:
    """``btcusdt@bookTicker`` — best bid/ask on every change."""
    return f"{symbol.lower()}@bookTicker"


def depth(symbol: str, *, levels: int = 20, speed: str = SPEED_100MS) -> str:
    """``btcusdt@depth20@100ms`` — a whole book, capped at ``levels``.

    A snapshot on a timer, which is what a consumer wanting "the book" means.
    The diff stream below is the other thing.
    """
    return f"{symbol.lower()}@depth{levels}@{speed}"


def depth_diff(symbol: str, *, speed: str = SPEED_100MS) -> str:
    """``btcusdt@depth@100ms`` — depth diffs, sequenced by ``U``/``u``.

    Only meaningful applied to a REST/WS-API snapshot in sequence order; a diff
    on its own is not a book.
    """
    return f"{symbol.lower()}@depth@{speed}"


def symbol_of(stream: str) -> str:
    """The symbol a stream name was built for, uppercased.

    ``btcusdt@depth20@100ms`` → ``BTCUSDT``. Needed because partial depth
    payloads carry no symbol of their own; everything else echoes it back in
    ``s`` and this is only a cross-check.
    """
    return stream.split("@", 1)[0].upper()


__all__ = [
    "DEPTH_LEVELS",
    "LIST_SUBSCRIPTIONS",
    "SPEED_100MS",
    "SPEED_1000MS",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "agg_trade",
    "book_ticker",
    "depth",
    "depth_diff",
    "kline",
    "symbol_of",
    "ticker",
    "trade",
]

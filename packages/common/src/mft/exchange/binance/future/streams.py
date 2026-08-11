"""Binance futures market stream names, and which endpoint each one lives on.

A stream is named, not parameterised: the symbol, the window and the update
speed are all baked into one lowercase string, and subscribing means listing
those strings::

    {"method": "SUBSCRIBE", "params": ["btcusdt@aggTrade"], "id": "..."}

Every builder here lowercases the symbol, for the reason spot's does: stream
names are case-sensitive even though the WebSocket API takes the symbol
uppercase.

**What is new on futures is that the name also decides the endpoint.** Since
the 2026 split there is no single market-streams host — the book feeds answer
on ``/public`` and everything else on ``/market`` (see :mod:`.protocol`) — and a
subscribe on the wrong one is acknowledged and then never pushes. So each name
is registered in a group here, :func:`group_of` reads it back, and the feed
opens one socket per group it is actually asked for. Nothing above this module
names an endpoint.
"""

from __future__ import annotations

from mft.exchange.binance.feed import (
    LIST_SUBSCRIPTIONS,
    SUBSCRIBE,
    UNSUBSCRIBE,
)
from mft.exchange.errors import ExchangeError

#: High-frequency book data.
PUBLIC = "public"
#: Everything else Binance calls market data — tape, candles, stats, mark
#: price, liquidations.
MARKET = "market"

#: Update speeds the depth streams accept. Futures adds ``250ms`` to spot's
#: pair and makes it the default; ``100ms`` is what a book consumer wants and
#: what this adapter asks for.
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


class UnknownStreamError(ExchangeError):
    """A stream name with no endpoint group — so nowhere to send it.

    Raised rather than defaulted. A default would put the subscribe on one of
    two hosts by coin flip, and the wrong one accepts it silently: the feed
    would look subscribed and never push.
    """


def agg_trade(symbol: str) -> str:
    """``btcusdt@aggTrade`` — the tape, same-price fills coalesced.

    The *only* tape futures publishes. Unlike spot there is no ``@trade``
    stream, so this is what a trade feed reads and its ids are aggregate ids.
    """
    return f"{symbol.lower()}@aggTrade"


def mark_price(symbol: str, *, speed: str = MARK_SPEED_1S) -> str:
    """``btcusdt@markPrice@1s`` — mark, index, funding rate and next funding.

    Has no spot equivalent because the concept does not exist there: the mark
    price is what a perpetual's margin and liquidations are computed against,
    and it is deliberately *not* the last traded price.
    """
    return f"{symbol.lower()}@markPrice@{speed}"


def kline(symbol: str, interval: str) -> str:
    """``btcusdt@kline_1m`` — ``interval`` is Binance's spelling, not ours."""
    return f"{symbol.lower()}@kline_{interval}"


def ticker(symbol: str) -> str:
    """``btcusdt@ticker`` — rolling 24h stats, pushed about twice a second.

    Carries **no bid or ask**, unlike spot's. Anything needing a quote reads
    :func:`book_ticker`; see
    :class:`~mft.exchange.binance.future.models.BinanceFutureTicker`.
    """
    return f"{symbol.lower()}@ticker"


def mini_ticker(symbol: str) -> str:
    """``btcusdt@miniTicker`` — the same window, four prices and two volumes."""
    return f"{symbol.lower()}@miniTicker"


def book_ticker(symbol: str) -> str:
    """``btcusdt@bookTicker`` — best bid/ask on every change."""
    return f"{symbol.lower()}@bookTicker"


def force_order(symbol: str) -> str:
    """``btcusdt@forceOrder`` — public liquidations.

    One message per symbol per second at most: Binance pushes the largest
    liquidation in each 1000ms window and drops the rest, so this is a sample
    of the flow rather than all of it. Nothing downstream can recover what was
    dropped, which is worth knowing before treating a count of these as a
    count of liquidations.
    """
    return f"{symbol.lower()}@forceOrder"


def depth(symbol: str, *, levels: int = 20, speed: str = SPEED_100MS) -> str:
    """``btcusdt@depth20@100ms`` — a whole book, capped at ``levels``.

    A snapshot on a timer, which is what a consumer wanting "the book" means.
    The diff stream below is the other thing.
    """
    return f"{symbol.lower()}@depth{levels}@{speed}"


def depth_diff(symbol: str, *, speed: str = SPEED_100MS) -> str:
    """``btcusdt@depth@100ms`` — depth diffs, sequenced by ``U``/``u``/``pu``.

    Only meaningful applied to a snapshot in sequence order. Futures adds
    ``pu`` — the previous message's ``u`` — which makes a dropped message
    detectable without a snapshot to compare against; see
    :meth:`~mft.exchange.binance.future.models.BinanceFutureDepthUpdate.follows`.
    """
    return f"{symbol.lower()}@depth@{speed}"


def symbol_of(stream: str) -> str:
    """The symbol a stream name was built for, uppercased.

    ``btcusdt@depth20@100ms`` → ``BTCUSDT``.
    """
    return stream.split("@", 1)[0].upper()


def channel_of(stream: str) -> str:
    """The channel part of a stream name, without symbol or speed.

    ``btcusdt@depth20@100ms`` → ``depth20``; ``btcusdt@kline_1m`` → ``kline``.
    The speed suffix is dropped because it never changes which endpoint a
    stream belongs to, and the kline interval likewise.
    """
    parts = stream.split("@")
    if len(parts) < 2:
        return ""
    channel = parts[1]
    return channel.split("_", 1)[0] if channel.startswith("kline_") else channel


#: Channel → endpoint group. Written out rather than inferred: this table is
#: the whole reason a subscribe reaches the host that answers it, and a rule
#: guessed from a prefix would put a new channel on the wrong one silently.
GROUPS: dict[str, str] = {
    "bookTicker": PUBLIC,
    "depth": PUBLIC,
    "depth5": PUBLIC,
    "depth10": PUBLIC,
    "depth20": PUBLIC,
    "aggTrade": MARKET,
    "markPrice": MARKET,
    "kline": MARKET,
    "ticker": MARKET,
    "miniTicker": MARKET,
    "forceOrder": MARKET,
}


def group_of(stream: str) -> str:
    """Which endpoint a stream name has to be subscribed on.

    Raises :class:`UnknownStreamError` for a name this module did not build —
    the alternative is a subscribe that goes out, is accepted, and never
    pushes.
    """
    found = GROUPS.get(channel_of(stream))
    if found is None:
        raise UnknownStreamError(
            f"unknown Binance futures stream {stream!r}; known channels: "
            f"{', '.join(sorted(GROUPS))}"
        )
    return found


__all__ = [
    "DEPTH_LEVELS",
    "GROUPS",
    "LIST_SUBSCRIPTIONS",
    "MARK_SPEED_1S",
    "MARK_SPEED_3S",
    "MARKET",
    "PUBLIC",
    "SPEED_100MS",
    "SPEED_250MS",
    "SPEED_500MS",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "UnknownStreamError",
    "agg_trade",
    "book_ticker",
    "channel_of",
    "depth",
    "depth_diff",
    "force_order",
    "group_of",
    "kline",
    "mark_price",
    "mini_ticker",
    "symbol_of",
    "ticker",
]

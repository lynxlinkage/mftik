"""Gate spot WebSocket v4 channel names and subscribe payload shapes.

Payload arity differs per channel and Gate is positional about it (the
``payload`` is a bare string list, not an object), so each channel gets a
builder here rather than leaving callers to remember the order.
"""

from __future__ import annotations

PING = "spot.ping"
PONG = "spot.pong"

TICKERS = "spot.tickers"
TRADES = "spot.trades"
CANDLESTICKS = "spot.candlesticks"
ORDER_BOOK = "spot.order_book"
ORDER_BOOK_UPDATE = "spot.order_book_update"
BOOK_TICKER = "spot.book_ticker"

ORDERS = "spot.orders"
USER_TRADES = "spot.usertrades"
BALANCES = "spot.balances"

# Trading calls — ``event: "api"``, not subscriptions.
LOGIN = "spot.login"
ORDER_PLACE = "spot.order_place"
ORDER_CANCEL = "spot.order_cancel"
ORDER_CANCEL_IDS = "spot.order_cancel_ids"
ORDER_CANCEL_CP = "spot.order_cancel_cp"
ORDER_AMEND = "spot.order_amend"
ORDER_STATUS = "spot.order_status"

#: Channels that require the ``auth`` block on subscribe.
PRIVATE = frozenset({ORDERS, USER_TRADES, BALANCES})

#: Trading calls; these are signed per request instead.
TRADING = frozenset(
    {
        ORDER_PLACE,
        ORDER_CANCEL,
        ORDER_CANCEL_IDS,
        ORDER_CANCEL_CP,
        ORDER_AMEND,
        ORDER_STATUS,
    }
)

API = "api"

#: Subscribe to every currency pair on a private channel.
ALL = "!all"

SUBSCRIBE = "subscribe"
UNSUBSCRIBE = "unsubscribe"
UPDATE = "update"


def tickers(*pairs: str) -> list[str]:
    """``["BTC_USDT", ...]``"""
    return list(pairs)


def trades(*pairs: str) -> list[str]:
    """``["BTC_USDT", ...]``"""
    return list(pairs)


def book_ticker(*pairs: str) -> list[str]:
    """``["BTC_USDT", ...]``"""
    return list(pairs)


def candlesticks(interval: str, pair: str) -> list[str]:
    """``[interval, pair]`` — note interval comes first, e.g. ``["1m", "BTC_USDT"]``.

    The push echoes it back joined as ``n = "1m_BTC_USDT"``.
    """
    return [interval, pair]


def order_book(pair: str, *, level: str = "20", interval: str = "1000ms") -> list[str]:
    """``[pair, level, interval]`` — limited-depth snapshot on a timer."""
    return [pair, level, interval]


def order_book_update(pair: str, *, interval: str = "100ms") -> list[str]:
    """``[pair, interval]`` — incremental diffs, sequenced by ``U``/``u``."""
    return [pair, interval]


def orders(*pairs: str) -> list[str]:
    """``["BTC_USDT", ...]`` or ``["!all"]``."""
    return list(pairs) or [ALL]


def user_trades(*pairs: str) -> list[str]:
    """``["BTC_USDT", ...]`` or ``["!all"]``."""
    return list(pairs) or [ALL]


def balances() -> list[str]:
    """Account-wide; takes no payload."""
    return []

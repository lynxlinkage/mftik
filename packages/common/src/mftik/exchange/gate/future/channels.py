"""Gate futures WebSocket v4 channel names and subscribe payload shapes.

Private channels take ``[uid, contract]`` (or ``[uid, "!all"]``), not the
spot pair-only list. ``uid`` comes from ``futures.login``.
"""

from __future__ import annotations

PING = "futures.ping"
PONG = "futures.pong"

TICKERS = "futures.tickers"
TRADES = "futures.trades"
CANDLESTICKS = "futures.candlesticks"
ORDER_BOOK = "futures.order_book"
BOOK_TICKER = "futures.book_ticker"
PUBLIC_LIQUIDATES = "futures.public_liquidates"

ORDERS = "futures.orders"
USER_TRADES = "futures.usertrades"
POSITIONS = "futures.positions"
BALANCES = "futures.balances"

LOGIN = "futures.login"
ORDER_PLACE = "futures.order_place"
ORDER_CANCEL = "futures.order_cancel"
ORDER_CANCEL_IDS = "futures.order_cancel_ids"
ORDER_CANCEL_CP = "futures.order_cancel_cp"
ORDER_AMEND = "futures.order_amend"
ORDER_STATUS = "futures.order_status"
ORDER_LIST = "futures.order_list"

PRIVATE = frozenset({ORDERS, USER_TRADES, POSITIONS, BALANCES})

TRADING = frozenset(
    {
        ORDER_PLACE,
        ORDER_CANCEL,
        ORDER_CANCEL_IDS,
        ORDER_CANCEL_CP,
        ORDER_AMEND,
        ORDER_STATUS,
        ORDER_LIST,
    }
)

API = "api"
ALL = "!all"
SUBSCRIBE = "subscribe"
UNSUBSCRIBE = "unsubscribe"
UPDATE = "update"


def tickers(*contracts: str) -> list[str]:
    return list(contracts)


def trades(*contracts: str) -> list[str]:
    return list(contracts)


def book_ticker(*contracts: str) -> list[str]:
    return list(contracts)


def candlesticks(interval: str, contract: str) -> list[str]:
    """``[interval, contract]`` — interval first, same as spot."""
    return [interval, contract]


def order_book(
    contract: str, *, level: str = "20", interval: str = "1000ms"
) -> list[str]:
    return [contract, level, interval]


def public_liquidates(*contracts: str) -> list[str]:
    return list(contracts)


def _scoped(uid: str, *contracts: str) -> list[str]:
    return [uid, *(contracts or (ALL,))]


def orders(uid: str, *contracts: str) -> list[str]:
    return _scoped(uid, *contracts)


def user_trades(uid: str, *contracts: str) -> list[str]:
    return _scoped(uid, *contracts)


def positions(uid: str, *contracts: str) -> list[str]:
    return _scoped(uid, *contracts)


def balances(uid: str) -> list[str]:
    return [uid]

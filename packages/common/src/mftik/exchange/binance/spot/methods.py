"""Binance spot WebSocket API method names, and what each one needs.

The WebSocket API is request/reply only — every method here is something you
*ask*, and the answer comes back on the same ``id``. Market-data pushes are a
different socket entirely; their names live in :mod:`.streams`.

Methods split three ways by what authenticates them, and the sets below are
what :class:`~mftik.exchange.binance.spot.client.BinanceSpotWsApi` checks before
sending rather than letting the venue answer ``-2014``:

* **open** — anyone may ask. All of the market data.
* :data:`SIGNED` — needs ``apiKey`` + ``signature``, or a session that has
  already logged on. Trading and account reads.
* :data:`SESSION_ONLY` — needs the logged-on session itself; there is no
  per-call signature that substitutes for it. Only the user data stream, and
  only because Binance ties the push subscription to the connection's identity
  rather than to a request.
"""

from __future__ import annotations

# --- general ---------------------------------------------------------------

PING = "ping"
TIME = "time"
EXCHANGE_INFO = "exchangeInfo"

# --- market data (open) ----------------------------------------------------

DEPTH = "depth"
KLINES = "klines"
UI_KLINES = "uiKlines"
TICKER_24HR = "ticker.24hr"
TICKER_BOOK = "ticker.book"
TICKER_PRICE = "ticker.price"
TRADES_RECENT = "trades.recent"
TRADES_AGGREGATE = "trades.aggregate"
AVG_PRICE = "avgPrice"

# --- session ---------------------------------------------------------------

SESSION_LOGON = "session.logon"
SESSION_STATUS = "session.status"
SESSION_LOGOUT = "session.logout"

# --- trading ---------------------------------------------------------------

ORDER_PLACE = "order.place"
ORDER_TEST = "order.test"
ORDER_CANCEL = "order.cancel"
ORDER_STATUS = "order.status"
ORDER_CANCEL_REPLACE = "order.cancelReplace"
OPEN_ORDERS_STATUS = "openOrders.status"
OPEN_ORDERS_CANCEL_ALL = "openOrders.cancelAll"

# --- account ---------------------------------------------------------------

ACCOUNT_STATUS = "account.status"
MY_TRADES = "myTrades"

# --- user data stream ------------------------------------------------------

USER_DATA_STREAM_SUBSCRIBE = "userDataStream.subscribe"
USER_DATA_STREAM_UNSUBSCRIBE = "userDataStream.unsubscribe"

#: Trading calls. Kept apart from the account reads so a refusal can say which
#: kind of call was refused.
TRADING = frozenset(
    {
        ORDER_PLACE,
        ORDER_TEST,
        ORDER_CANCEL,
        ORDER_STATUS,
        ORDER_CANCEL_REPLACE,
        OPEN_ORDERS_STATUS,
        OPEN_ORDERS_CANCEL_ALL,
    }
)

#: Needs ``apiKey`` + ``signature``, or a session that has logged on.
SIGNED = TRADING | frozenset({ACCOUNT_STATUS, MY_TRADES})

#: Needs the logged-on session; no per-call signature substitutes for it.
SESSION_ONLY = frozenset(
    {USER_DATA_STREAM_SUBSCRIBE, USER_DATA_STREAM_UNSUBSCRIBE}
)

# --- user data event types -------------------------------------------------

#: One order changed state — Binance's only order event. Fills arrive here too,
#: as reports with ``x == "TRADE"``; there is no separate trade stream.
EXECUTION_REPORT = "executionReport"
#: Every balance that moved, as a whole snapshot of the affected assets.
OUTBOUND_ACCOUNT_POSITION = "outboundAccountPosition"
#: A deposit, withdrawal or transfer — a delta, not a balance.
BALANCE_UPDATE = "balanceUpdate"
#: An order list (OCO and friends) changed. Not modelled; we place none.
LIST_STATUS = "listStatus"

#: The execution type that means this report carries a fill.
EXEC_TYPE_TRADE = "TRADE"


__all__ = [
    "ACCOUNT_STATUS",
    "AVG_PRICE",
    "BALANCE_UPDATE",
    "DEPTH",
    "EXCHANGE_INFO",
    "EXECUTION_REPORT",
    "EXEC_TYPE_TRADE",
    "KLINES",
    "LIST_STATUS",
    "MY_TRADES",
    "OPEN_ORDERS_CANCEL_ALL",
    "OPEN_ORDERS_STATUS",
    "ORDER_CANCEL",
    "ORDER_CANCEL_REPLACE",
    "ORDER_PLACE",
    "ORDER_STATUS",
    "ORDER_TEST",
    "OUTBOUND_ACCOUNT_POSITION",
    "PING",
    "SESSION_LOGON",
    "SESSION_LOGOUT",
    "SESSION_ONLY",
    "SESSION_STATUS",
    "SIGNED",
    "TICKER_24HR",
    "TICKER_BOOK",
    "TICKER_PRICE",
    "TIME",
    "TRADES_AGGREGATE",
    "TRADES_RECENT",
    "TRADING",
    "UI_KLINES",
    "USER_DATA_STREAM_SUBSCRIBE",
    "USER_DATA_STREAM_UNSUBSCRIBE",
]

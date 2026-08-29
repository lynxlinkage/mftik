"""Binance COIN-M WebSocket API method names, and what each one needs.

The WebSocket API is request/reply only — every method here is something you
*ask*, and the answer comes back on the same ``id``. Market-data pushes are
the dstream socket; they are not this list.

Two things differ from USD-M and must not be copied across:

* **Account methods have no ``v2/`` prefix.** dapi answers
  ``account.balance`` and ``account.position``. The fapi ``v2/`` names are a
  different host.
* **There is no "list my open orders".** Recon uses signed REST
  ``GET /dapi/v1/openOrders``.

The user data stream is a listen key, same as USD-M: ``userDataStream.start``
hands one out, and the events arrive on ``dstream`` at ``/ws/<listenKey>``.
dapi was not part of the 2026 ``/private`` split.
"""

from __future__ import annotations

from mftik.exchange.binance.protocol import SESSION_LOGON

PING = "ping"
TIME = "time"

SESSION_STATUS = "session.status"
SESSION_LOGOUT = "session.logout"

ORDER_PLACE = "order.place"
ORDER_CANCEL = "order.cancel"
ORDER_STATUS = "order.status"

ACCOUNT_BALANCE = "account.balance"
ACCOUNT_POSITION = "account.position"

USER_DATA_STREAM_START = "userDataStream.start"
USER_DATA_STREAM_PING = "userDataStream.ping"
USER_DATA_STREAM_STOP = "userDataStream.stop"

TRADING = frozenset({ORDER_PLACE, ORDER_CANCEL, ORDER_STATUS})

#: Needs ``apiKey`` + ``signature``, or a session that has logged on.
#: Either way a ``timestamp``: ``session.logon`` replaces the credential on
#: each call, not the clock, and Binance answers ``-1102`` without one.
SIGNED = TRADING | frozenset({ACCOUNT_BALANCE, ACCOUNT_POSITION})

#: Needs the API key named but nothing signed. Binance's ``USER_STREAM``
#: security type: a timestamp is ``-1101``. On a logged-on session these
#: carry no params at all.
API_KEY_ONLY = frozenset(
    {USER_DATA_STREAM_START, USER_DATA_STREAM_PING, USER_DATA_STREAM_STOP}
)

ORDER_TRADE_UPDATE = "ORDER_TRADE_UPDATE"
ACCOUNT_UPDATE = "ACCOUNT_UPDATE"
LISTEN_KEY_EXPIRED = "listenKeyExpired"


__all__ = [
    "ACCOUNT_BALANCE",
    "ACCOUNT_POSITION",
    "ACCOUNT_UPDATE",
    "API_KEY_ONLY",
    "LISTEN_KEY_EXPIRED",
    "ORDER_CANCEL",
    "ORDER_PLACE",
    "ORDER_STATUS",
    "ORDER_TRADE_UPDATE",
    "PING",
    "SESSION_LOGON",
    "SESSION_LOGOUT",
    "SESSION_STATUS",
    "SIGNED",
    "TIME",
    "TRADING",
    "USER_DATA_STREAM_PING",
    "USER_DATA_STREAM_START",
    "USER_DATA_STREAM_STOP",
]

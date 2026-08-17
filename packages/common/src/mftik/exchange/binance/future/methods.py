"""Binance futures WebSocket API method names, and what each one needs.

The WebSocket API is request/reply only — every method here is something you
*ask*, and the answer comes back on the same ``id``. Market-data pushes are
other sockets entirely; their names live in :mod:`.streams`.

Three things differ from the spot list, and each one shapes a client above:

* **There are no candles and no instrument listing.** ``klines`` and
  ``exchangeInfo`` are REST-only on futures, so the adapter carries a REST
  client (:mod:`.rest`) where the spot one needs none.
* **There is no "list my open orders".** Spot answers ``openOrders.status``;
  futures has no equivalent method, and recon cannot do without one — so that
  read is REST too.
* **The user data stream is a listen key.** ``userDataStream.start`` hands one
  out, ``userDataStream.ping`` keeps it alive, and the events arrive on a
  different socket. Those three are :data:`API_KEY_ONLY`: they need the key
  but carry no signature, which is a third auth class spot never needed.
"""

from __future__ import annotations

from mftik.exchange.binance.protocol import SESSION_LOGON

# --- general ---------------------------------------------------------------

PING = "ping"
TIME = "time"

# --- market data (open) ----------------------------------------------------

DEPTH = "depth"
TICKER_BOOK = "ticker.book"
TICKER_PRICE = "ticker.price"

# --- session ---------------------------------------------------------------

SESSION_STATUS = "session.status"
SESSION_LOGOUT = "session.logout"

# --- trading ---------------------------------------------------------------

ORDER_PLACE = "order.place"
ORDER_MODIFY = "order.modify"
ORDER_CANCEL = "order.cancel"
ORDER_STATUS = "order.status"

# --- account ---------------------------------------------------------------

#: v2 where Binance publishes one: the older ``account.balance`` and
#: ``account.position`` still answer, but v2 is the shape Binance maintains and
#: the one whose position rows are limited to symbols the account actually
#: holds — which is the difference between a handful of rows and every listed
#: contract on every recon.
ACCOUNT_BALANCE = "v2/account.balance"
ACCOUNT_POSITION = "v2/account.position"
ACCOUNT_STATUS = "v2/account.status"

# --- user data stream ------------------------------------------------------

USER_DATA_STREAM_START = "userDataStream.start"
USER_DATA_STREAM_PING = "userDataStream.ping"
USER_DATA_STREAM_STOP = "userDataStream.stop"

#: Trading calls. Kept apart from the account reads so a refusal can say which
#: kind of call was refused.
TRADING = frozenset({ORDER_PLACE, ORDER_MODIFY, ORDER_CANCEL, ORDER_STATUS})

#: Needs ``apiKey`` + ``signature``, or a session that has logged on.
SIGNED = TRADING | frozenset({ACCOUNT_BALANCE, ACCOUNT_POSITION, ACCOUNT_STATUS})

#: Needs the API key named but nothing signed. Binance's ``USER_STREAM``
#: security type: the key identifies whose stream is meant, and there is no
#: timestamp and no signature to send. On a logged-on session the key is
#: implied and these carry no params at all.
API_KEY_ONLY = frozenset(
    {USER_DATA_STREAM_START, USER_DATA_STREAM_PING, USER_DATA_STREAM_STOP}
)

# --- user data event types -------------------------------------------------

#: One order changed state — the futures order event. Fills arrive here too, as
#: updates with ``o.x == "TRADE"``; there is no separate trade stream.
ORDER_TRADE_UPDATE = "ORDER_TRADE_UPDATE"
#: Balances and positions that moved, as a snapshot of what changed.
ACCOUNT_UPDATE = "ACCOUNT_UPDATE"
#: The listen key this socket was opened with is dead. Not an error and not a
#: disconnect — the socket stays up and stops carrying anything, which is why
#: it has to be acted on rather than logged.
LISTEN_KEY_EXPIRED = "listenKeyExpired"
#: Margin ratio warning. Informational; nothing here acts on it.
MARGIN_CALL = "MARGIN_CALL"
#: Leverage or margin type changed, by us or by Binance.
ACCOUNT_CONFIG_UPDATE = "ACCOUNT_CONFIG_UPDATE"
#: A fill, repeated in a smaller envelope for latency-sensitive readers. Not
#: modelled: every field on it is on the ``ORDER_TRADE_UPDATE`` that
#: accompanies it, and reading both would double-count every fill.
TRADE_LITE = "TRADE_LITE"

#: The execution type that means this update carries a fill.
EXEC_TYPE_TRADE = "TRADE"


__all__ = [
    "ACCOUNT_BALANCE",
    "ACCOUNT_CONFIG_UPDATE",
    "ACCOUNT_POSITION",
    "ACCOUNT_STATUS",
    "ACCOUNT_UPDATE",
    "API_KEY_ONLY",
    "DEPTH",
    "EXEC_TYPE_TRADE",
    "LISTEN_KEY_EXPIRED",
    "MARGIN_CALL",
    "ORDER_CANCEL",
    "ORDER_MODIFY",
    "ORDER_PLACE",
    "ORDER_STATUS",
    "ORDER_TRADE_UPDATE",
    "PING",
    "SESSION_LOGON",
    "SESSION_LOGOUT",
    "SESSION_STATUS",
    "SIGNED",
    "TICKER_BOOK",
    "TICKER_PRICE",
    "TIME",
    "TRADE_LITE",
    "TRADING",
    "USER_DATA_STREAM_PING",
    "USER_DATA_STREAM_START",
    "USER_DATA_STREAM_STOP",
]

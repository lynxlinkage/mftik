"""Deribit channel names and JSON-RPC method paths.

Subscriptions are dotted strings::

    ticker.BTC_USDC-PERPETUAL.100ms
    user.orders.spot.any.raw

These names stay inside :mod:`mftik.exchange.deribit`.
"""

from __future__ import annotations

# --- public channels -------------------------------------------------------

TICKER = "ticker"
TRADES = "trades"
BOOK = "book"
QUOTE = "quote"
CHART = "chart.trades"
INTERVAL_100MS = "100ms"
INTERVAL_RAW = "raw"

#: ``book.{instrument}.{group}.{depth}.{interval}`` depths. Deribit
#: accepts only these three. ``none`` is a legal group for every currency.
BOOK_DEPTHS = (1, 10, 20)
DEFAULT_BOOK_DEPTH = 20
DEFAULT_BOOK_GROUP = "none"

# --- private channels ------------------------------------------------------

USER_ORDERS = "user.orders"
USER_TRADES = "user.trades"
USER_PORTFOLIO = "user.portfolio"


def ticker(instrument: str, *, interval: str = INTERVAL_100MS) -> str:
    return f"{TICKER}.{instrument}.{interval}"


def trades(instrument: str, *, interval: str = INTERVAL_100MS) -> str:
    return f"{TRADES}.{instrument}.{interval}"


def book(
    instrument: str,
    *,
    interval: str = INTERVAL_100MS,
    depth: int | None = DEFAULT_BOOK_DEPTH,
    group: str = DEFAULT_BOOK_GROUP,
) -> str:
    """``book.BTC_USDC.none.20.100ms`` — a snapshot of the top ``depth`` levels.

    Every notification on that channel is the whole truncated book
    (``[price, amount]``, no ``prev_change_id``). ``depth=None`` is the
    unbounded ``book.{instrument}.{interval}`` channel: one full snapshot,
    then increments. That snapshot is already past 1 MiB on BTC_USDC.
    """
    if depth is None:
        return f"{BOOK}.{instrument}.{interval}"
    if depth not in BOOK_DEPTHS:
        raise ValueError(
            f"Deribit book depth must be one of {BOOK_DEPTHS}, got {depth}"
        )
    return f"{BOOK}.{instrument}.{group}.{depth}.{interval}"


def unbounded_book(channel: str) -> bool:
    """True for ``book.{instrument}.{interval}``, the no-depth channel.

    ``book.{instrument}.{group}.{depth}.{interval}`` has five segments.
    Instrument names do not contain dots.
    """
    parts = (channel or "").split(".")
    return len(parts) == 3 and parts[0] == BOOK


def quote(instrument: str) -> str:
    return f"{QUOTE}.{instrument}"


def kline(instrument: str, resolution: str) -> str:
    return f"{CHART}.{instrument}.{resolution}"


def user_orders(
    kind: str, *, currency: str = "any", interval: str = INTERVAL_RAW
) -> str:
    return f"{USER_ORDERS}.{kind}.{currency}.{interval}"


def user_trades(
    kind: str, *, currency: str = "any", interval: str = INTERVAL_RAW
) -> str:
    return f"{USER_TRADES}.{kind}.{currency}.{interval}"


def user_portfolio(currency: str) -> str:
    return f"{USER_PORTFOLIO}.{currency}"


def instrument_of(channel: str) -> str:
    """The ``instrument_name`` embedded in a per-instrument channel, or ``""``."""
    parts = (channel or "").split(".")
    if len(parts) >= 2 and parts[0] in {TICKER, TRADES, BOOK, QUOTE}:
        return parts[1]
    if len(parts) >= 3 and channel.startswith(CHART + "."):
        return parts[2]
    return ""


# --- RPC methods -----------------------------------------------------------

PUBLIC_AUTH = "public/auth"
PUBLIC_SUBSCRIBE = "public/subscribe"
PUBLIC_UNSUBSCRIBE = "public/unsubscribe"
PUBLIC_SET_HEARTBEAT = "public/set_heartbeat"
PUBLIC_TEST = "public/test"
PUBLIC_GET_INSTRUMENTS = "public/get_instruments"
PUBLIC_GET_TIME = "public/get_time"
PUBLIC_TICKER = "public/ticker"
PUBLIC_GET_ORDER_BOOK = "public/get_order_book"
PUBLIC_GET_TRADINGVIEW = "public/get_tradingview_chart_data"
PUBLIC_GET_FUNDING_HISTORY = "public/get_funding_rate_history"

PRIVATE_SUBSCRIBE = "private/subscribe"
PRIVATE_UNSUBSCRIBE = "private/unsubscribe"
PRIVATE_BUY = "private/buy"
PRIVATE_SELL = "private/sell"
PRIVATE_CANCEL = "private/cancel"
PRIVATE_CANCEL_BY_LABEL = "private/cancel_by_label"
PRIVATE_GET_OPEN_ORDERS = "private/get_open_orders"
PRIVATE_GET_OPEN_ORDERS_BY_INSTRUMENT = "private/get_open_orders_by_instrument"
PRIVATE_GET_ORDER_STATE = "private/get_order_state"
PRIVATE_GET_ORDER_STATE_BY_LABEL = "private/get_order_state_by_label"
PRIVATE_GET_ACCOUNT_SUMMARIES = "private/get_account_summaries"
PRIVATE_GET_POSITIONS = "private/get_positions"


__all__ = [
    "BOOK",
    "BOOK_DEPTHS",
    "CHART",
    "DEFAULT_BOOK_DEPTH",
    "DEFAULT_BOOK_GROUP",
    "INTERVAL_100MS",
    "INTERVAL_RAW",
    "PRIVATE_BUY",
    "PRIVATE_CANCEL",
    "PRIVATE_CANCEL_BY_LABEL",
    "PRIVATE_GET_ACCOUNT_SUMMARIES",
    "PRIVATE_GET_OPEN_ORDERS",
    "PRIVATE_GET_OPEN_ORDERS_BY_INSTRUMENT",
    "PRIVATE_GET_ORDER_STATE",
    "PRIVATE_GET_ORDER_STATE_BY_LABEL",
    "PRIVATE_GET_POSITIONS",
    "PRIVATE_SELL",
    "PRIVATE_SUBSCRIBE",
    "PRIVATE_UNSUBSCRIBE",
    "PUBLIC_AUTH",
    "PUBLIC_GET_FUNDING_HISTORY",
    "PUBLIC_GET_INSTRUMENTS",
    "PUBLIC_GET_ORDER_BOOK",
    "PUBLIC_GET_TIME",
    "PUBLIC_GET_TRADINGVIEW",
    "PUBLIC_SET_HEARTBEAT",
    "PUBLIC_SUBSCRIBE",
    "PUBLIC_TEST",
    "PUBLIC_TICKER",
    "PUBLIC_UNSUBSCRIBE",
    "QUOTE",
    "TICKER",
    "TRADES",
    "USER_ORDERS",
    "USER_PORTFOLIO",
    "USER_TRADES",
    "book",
    "instrument_of",
    "kline",
    "quote",
    "ticker",
    "trades",
    "unbounded_book",
    "user_orders",
    "user_portfolio",
    "user_trades",
]

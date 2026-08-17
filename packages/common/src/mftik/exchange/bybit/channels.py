"""Bybit v5 topic names and op names.

A Bybit subscription is a **name**, not a parameterised request: the symbol,
the book depth and the candle window are all baked into one string, and
subscribing means listing those strings::

    {"op": "subscribe", "args": ["orderbook.50.BTCUSDT"], "req_id": "..."}

Symbols go in uppercase, exactly as the venue lists them. Unlike Binance's
market streams, Bybit's topic names are not lowercased — ``publicTrade.btcusdt``
is simply a topic that never pushes.

**The category is not in the topic.** ``publicTrade.BTCUSDT`` names the spot
tape or the linear-perp tape depending on which socket it was subscribed on
(:func:`~mftik.exchange.bybit.protocol.public_url`), which is why the public feed
class is constructed per category and the topic builders here take none.

The private topics are the other way round: one socket carries every category,
and each topic has a bare form covering all of them plus a suffixed form
scoping it to one — ``order`` against ``order.spot``. :func:`scoped` builds the
second, and the private stream uses it so a spot session is not woken by every
perp fill on the same account.
"""

from __future__ import annotations

from mftik.exchange.bybit.protocol import PRODUCTS

# --- public topics ---------------------------------------------------------

#: Book depths each product accepts. Bybit acknowledges a subscribe to any
#: other depth and then never pushes, so an unsupported one is a silent dead
#: feed rather than an error — which is why this table is checked locally.
BOOK_DEPTHS: dict[str, tuple[int, ...]] = {
    "spot": (1, 50, 200),
    "linear": (1, 50, 200, 500),
    "inverse": (1, 50, 200, 500),
    "option": (25, 100),
}

#: Candle windows, in Bybit's spelling: minutes as bare numbers, then ``D``,
#: ``W`` and ``M``. Translating our canonical spelling into these happens in
#: :mod:`.public`.
KLINE_INTERVALS = (
    "1",
    "3",
    "5",
    "15",
    "30",
    "60",
    "120",
    "240",
    "360",
    "720",
    "D",
    "W",
    "M",
)


def order_book(symbol: str, *, depth: int = 50) -> str:
    """``orderbook.50.BTCUSDT`` — a book, snapshot then deltas.

    Bybit sends one ``snapshot`` push and then ``delta`` pushes against it, so
    a consumer that wants whole books has to fold them —
    :class:`~mftik.exchange.bybit.feed.BybitBook` does. Depth ``1`` is the
    exception worth knowing: it pushes the top of book as a snapshot every
    time, which makes it Bybit's equivalent of a best-quote feed.
    """
    return f"orderbook.{depth}.{symbol.upper()}"


def public_trade(symbol: str) -> str:
    """``publicTrade.BTCUSDT`` — the tape, one message per aggressing order."""
    return f"publicTrade.{symbol.upper()}"


def tickers(symbol: str) -> str:
    """``tickers.BTCUSDT`` — 24h stats, and top of book on the derivative books.

    Spot and derivatives disagree about this topic in two ways that matter:
    spot pushes only snapshots while the perp books push deltas that carry just
    the changed fields, and spot's payload has no bid or ask at all. See
    :class:`~mftik.exchange.bybit.models.BybitTicker`.
    """
    return f"tickers.{symbol.upper()}"


def kline(symbol: str, interval: str) -> str:
    """``kline.1.BTCUSDT`` — ``interval`` in Bybit's spelling, not ours."""
    return f"kline.{interval}.{symbol.upper()}"


def all_liquidation(symbol: str) -> str:
    """``allLiquidation.BTCUSDT`` — every forced close on the contract books.

    Spot has no liquidations; this topic lives on the linear and inverse
    sockets only. ``S`` on the payload is the liquidated position's side —
    ``Buy`` means a long was closed out — not the aggressor on the tape.
    """
    return f"allLiquidation.{symbol.upper()}"


def symbol_of(topic: str) -> str:
    """The symbol a topic names, or ``""`` for one that names none.

    The symbol is the last dot-separated part of every public topic, which is
    what lets a push be routed without reading its payload — and the order book
    needs exactly that, because a ``delta`` says which instrument it is only in
    a field the topic already told us.
    """
    parts = topic.split(".")
    return parts[-1].upper() if len(parts) > 1 else ""


# --- private topics --------------------------------------------------------

#: Order lifecycle. Bybit's only order event, for every category.
ORDER = "order"
#: Fills. A separate topic here, unlike Binance where a fill is an order event.
EXECUTION = "execution"
#: Fills again, sooner and with fewer fields. Not used: it omits the fee, which
#: the shared :class:`~mftik.exchange.models.Fill` states.
EXECUTION_FAST = "execution.fast"
#: Balances, as whole account snapshots rather than deltas.
WALLET = "wallet"
#: Open positions. Nothing on the spot book has one.
POSITION = "position"
#: Options risk. Not modelled; we trade none.
GREEKS = "greeks"

#: The private topics that accept a category suffix. ``wallet`` does not — a
#: unified account has one balance sheet, not one per book.
SCOPED_TOPICS = frozenset({ORDER, EXECUTION, POSITION})


def scoped(topic: str, product: str | None = None) -> str:
    """``order`` → ``order.spot``, or ``order`` when no product is given.

    Scoping is worth doing wherever it is allowed: a unified account's private
    socket carries every category, so an unscoped ``order`` subscription on a
    spot session delivers every perp order update as well — messages the
    connector would then have to filter, having already paid to receive them.
    """
    if product is None or topic not in SCOPED_TOPICS:
        return topic
    if product not in PRODUCTS:
        raise ValueError(
            f"unknown Bybit product {product!r}; known: {', '.join(PRODUCTS)}"
        )
    return f"{topic}.{product}"


def base_topic(topic: str) -> str:
    """``order.spot`` → ``order``. What a push should be routed as.

    Bybit echoes the topic it pushed on, suffix and all, so a stream subscribed
    to ``order.spot`` receives frames whose topic is not the string anything
    else in this adapter says.
    """
    head = topic.split(".", 1)[0]
    if head == EXECUTION and topic.startswith(EXECUTION_FAST):
        return EXECUTION_FAST
    return head if head in (ORDER, EXECUTION, WALLET, POSITION, GREEKS) else topic


# --- trade ops -------------------------------------------------------------

#: Order entry, on the trade socket. The batch forms take several orders on one
#: frame; only the single forms are wrapped in :mod:`.trade`.
ORDER_CREATE = "order.create"
ORDER_AMEND = "order.amend"
ORDER_CANCEL = "order.cancel"
ORDER_CREATE_BATCH = "order.create-batch"
ORDER_AMEND_BATCH = "order.amend-batch"
ORDER_CANCEL_BATCH = "order.cancel-batch"

TRADE_OPS = frozenset(
    {
        ORDER_CREATE,
        ORDER_AMEND,
        ORDER_CANCEL,
        ORDER_CREATE_BATCH,
        ORDER_AMEND_BATCH,
        ORDER_CANCEL_BATCH,
    }
)

# --- REST paths ------------------------------------------------------------

#: The reads and writes REST serves. Order entry is here too, as the fallback
#: for a caller that would rather not hold a second socket open — the trade
#: socket is the fast path, not the only one.
MARKET_INSTRUMENTS = "/v5/market/instruments-info"
MARKET_TICKERS = "/v5/market/tickers"
MARKET_ORDER_BOOK = "/v5/market/orderbook"
MARKET_KLINE = "/v5/market/kline"
MARKET_TIME = "/v5/market/time"

ORDER_CREATE_PATH = "/v5/order/create"
ORDER_CANCEL_PATH = "/v5/order/cancel"
ORDER_AMEND_PATH = "/v5/order/amend"
ORDER_REALTIME_PATH = "/v5/order/realtime"
ORDER_HISTORY_PATH = "/v5/order/history"
EXECUTION_LIST_PATH = "/v5/execution/list"
WALLET_BALANCE_PATH = "/v5/account/wallet-balance"
POSITION_LIST_PATH = "/v5/position/list"


__all__ = [
    "BOOK_DEPTHS",
    "EXECUTION",
    "EXECUTION_FAST",
    "EXECUTION_LIST_PATH",
    "GREEKS",
    "KLINE_INTERVALS",
    "MARKET_INSTRUMENTS",
    "MARKET_KLINE",
    "MARKET_ORDER_BOOK",
    "MARKET_TICKERS",
    "MARKET_TIME",
    "ORDER",
    "ORDER_AMEND",
    "ORDER_AMEND_BATCH",
    "ORDER_AMEND_PATH",
    "ORDER_CANCEL",
    "ORDER_CANCEL_BATCH",
    "ORDER_CANCEL_PATH",
    "ORDER_CREATE",
    "ORDER_CREATE_BATCH",
    "ORDER_CREATE_PATH",
    "ORDER_HISTORY_PATH",
    "ORDER_REALTIME_PATH",
    "POSITION",
    "POSITION_LIST_PATH",
    "SCOPED_TOPICS",
    "TRADE_OPS",
    "WALLET",
    "WALLET_BALANCE_PATH",
    "all_liquidation",
    "base_topic",
    "kline",
    "order_book",
    "public_trade",
    "scoped",
    "symbol_of",
    "tickers",
]

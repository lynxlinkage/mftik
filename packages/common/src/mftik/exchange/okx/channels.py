"""OKX v5 channel names and REST paths.

An OKX subscription is a **parameterised object**, not a dotted string::

    {"op": "subscribe", "args": [{"channel": "tickers", "instId": "BTC-USDT"}]}

The channel is the feed; ``instId`` (or ``instType`` on the account topics)
says which instrument. Public and private sockets use the same shape, which
is why the builders here are shared.

Candle windows live on the business socket and bake the bar into the channel
name — ``candle1m``, ``candle1H`` — in OKX's own spelling. Translating our
canonical interval happens in :mod:`.public`.
"""

from __future__ import annotations

from typing import Any

# --- public channels -------------------------------------------------------

TICKERS = "tickers"
TRADES = "trades"
BOOKS = "books"
BOOKS5 = "books5"
BBO = "bbo-tbt"
LIQUIDATION = "liquidation-orders"
FUNDING_RATE = "funding-rate"
OPEN_INTEREST = "open-interest"

#: Book depths the REST ``/market/books`` endpoint accepts. The WS ``books``
#: channel is always 400 levels; a caller wanting fewer trims locally.
BOOK_DEPTHS = (1, 5, 10, 50, 100, 200, 400)

#: Candle windows, in OKX's spelling. Translating ours happens in :mod:`.public`.
KLINE_BARS = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1H",
    "2H",
    "4H",
    "6H",
    "12H",
    "1D",
    "1W",
    "1M",
    "3M",
)


def tickers(inst_id: str) -> dict[str, str]:
    return {"channel": TICKERS, "instId": inst_id}


def trades(inst_id: str) -> dict[str, str]:
    return {"channel": TRADES, "instId": inst_id}


def books(inst_id: str, *, channel: str = BOOKS) -> dict[str, str]:
    """``books`` — 400 levels, one snapshot then updates.

    ``books5`` is the always-snapshot form, used only as an escape hatch.
    """
    return {"channel": channel, "instId": inst_id}


def bbo(inst_id: str) -> dict[str, str]:
    """``bbo-tbt`` — top of book, a snapshot every time."""
    return {"channel": BBO, "instId": inst_id}


def candle(inst_id: str, bar: str) -> dict[str, str]:
    """``candle1m`` — ``bar`` in OKX's spelling, not ours."""
    return {"channel": f"candle{bar}", "instId": inst_id}


def liquidation(inst_type: str) -> dict[str, str]:
    """``liquidation-orders`` — forced closes, scoped by ``instType``.

    OKX will not take an ``instId`` here: one subscription covers the whole
    book, and the connector filters. Spot has none.
    """
    return {"channel": LIQUIDATION, "instType": inst_type}


def funding_rate(inst_id: str) -> dict[str, str]:
    """``funding-rate`` — the still-moving prediction for the next settlement."""
    return {"channel": FUNDING_RATE, "instId": inst_id}


def open_interest(inst_id: str) -> dict[str, str]:
    """``open-interest`` — current size, about every three seconds."""
    return {"channel": OPEN_INTEREST, "instId": inst_id}


# --- private channels ------------------------------------------------------

ORDERS = "orders"
FILLS = "fills"
ACCOUNT = "account"
POSITIONS = "positions"
ANY = "ANY"


def orders() -> dict[str, str]:
    """Every order on the account, whichever book it is on."""
    return {"channel": ORDERS, "instType": ANY}


def fills() -> dict[str, str]:
    """Every fill. Funding and ADL do not arrive here."""
    return {"channel": FILLS, "instType": ANY}


def account() -> dict[str, str]:
    """The unified wallet. Never scoped: there is one balance sheet."""
    return {"channel": ACCOUNT}


def positions() -> dict[str, str]:
    """Open contracts. Silent on a spot-only account."""
    return {"channel": POSITIONS, "instType": ANY}


def arg_key(arg: dict[str, Any]) -> tuple[str, str, str]:
    """Identity of a subscribe arg, for routing and de-duplication."""
    return (
        str(arg.get("channel") or ""),
        str(arg.get("instId") or ""),
        str(arg.get("instType") or ""),
    )


# --- REST paths ------------------------------------------------------------

MARKET_INSTRUMENTS = "/api/v5/public/instruments"
MARKET_TICKER = "/api/v5/market/ticker"
MARKET_BOOKS = "/api/v5/market/books"
MARKET_CANDLES = "/api/v5/market/candles"
MARKET_TIME = "/api/v5/public/time"
MARKET_FUNDING_HISTORY = "/api/v5/public/funding-rate-history"
MARKET_OPEN_INTEREST = "/api/v5/public/open-interest"

ORDER_PLACE = "/api/v5/trade/order"
ORDER_CANCEL = "/api/v5/trade/cancel-order"
ORDERS_PENDING = "/api/v5/trade/orders-pending"
ORDERS_HISTORY = "/api/v5/trade/orders-history"
FILLS_HISTORY = "/api/v5/trade/fills"

ACCOUNT_BALANCE = "/api/v5/account/balance"
ACCOUNT_POSITIONS = "/api/v5/account/positions"
ACCOUNT_LEVERAGE = "/api/v5/account/leverage-info"
ACCOUNT_CONFIG = "/api/v5/account/config"


__all__ = [
    "ACCOUNT",
    "ACCOUNT_BALANCE",
    "ACCOUNT_CONFIG",
    "ACCOUNT_LEVERAGE",
    "ACCOUNT_POSITIONS",
    "ANY",
    "BBO",
    "BOOKS",
    "BOOKS5",
    "BOOK_DEPTHS",
    "FILLS",
    "FILLS_HISTORY",
    "FUNDING_RATE",
    "KLINE_BARS",
    "LIQUIDATION",
    "MARKET_BOOKS",
    "MARKET_CANDLES",
    "MARKET_FUNDING_HISTORY",
    "MARKET_INSTRUMENTS",
    "MARKET_OPEN_INTEREST",
    "MARKET_TICKER",
    "MARKET_TIME",
    "OPEN_INTEREST",
    "ORDERS",
    "ORDERS_HISTORY",
    "ORDERS_PENDING",
    "ORDER_CANCEL",
    "ORDER_PLACE",
    "POSITIONS",
    "TICKERS",
    "TRADES",
    "account",
    "arg_key",
    "bbo",
    "books",
    "candle",
    "fills",
    "funding_rate",
    "liquidation",
    "open_interest",
    "orders",
    "positions",
    "tickers",
    "trades",
]

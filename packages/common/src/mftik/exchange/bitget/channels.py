"""Bitget UTA v3 channel names and REST paths.

A v3 subscription is a **parameterised object**::

    {"op": "subscribe", "args": [{"instType": "spot", "topic": "ticker",
                                  "symbol": "BTCUSDT"}]}

Public sockets key on Bitget's ``instType`` (``spot`` / ``usdt-futures`` /
``usdc-futures``), not on our :class:`~mftik.exchange.tickers.Category`.
Private sockets use ``instType: "UTA"`` and carry every book at once.

These names stay inside :mod:`mftik.exchange.bitget` (I10).
"""

from __future__ import annotations

from typing import Any

# --- public topics ---------------------------------------------------------

TICKER = "ticker"
PUBLIC_TRADE = "publicTrade"
BOOKS = "books"
BOOKS1 = "books1"
BOOKS5 = "books5"
BOOKS15 = "books15"
KLINE = "kline"
LIQUIDATION = "liquidation"

BOOK_DEPTHS = (1, 5, 15)

#: Candle windows in Bitget's spelling. Hours and longer are capitalised.
KLINE_BARS = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1H",
    "4H",
    "6H",
    "12H",
    "1D",
    "1W",
    "1M",
)


def ticker(inst_type: str, symbol: str) -> dict[str, str]:
    return {"instType": inst_type, "topic": TICKER, "symbol": symbol}


def public_trade(inst_type: str, symbol: str) -> dict[str, str]:
    return {"instType": inst_type, "topic": PUBLIC_TRADE, "symbol": symbol}


def books(inst_type: str, symbol: str, *, topic: str = BOOKS) -> dict[str, str]:
    return {"instType": inst_type, "topic": topic, "symbol": symbol}


def kline(inst_type: str, symbol: str, interval: str) -> dict[str, str]:
    return {
        "instType": inst_type,
        "topic": KLINE,
        "symbol": symbol,
        "interval": interval,
    }


def liquidation(inst_type: str) -> dict[str, str]:
    """Platform liquidations for one futures ``instType``. No ``symbol``."""
    return {"instType": inst_type, "topic": LIQUIDATION}


# --- private topics --------------------------------------------------------

UTA = "UTA"
ORDER = "order"
FILL = "fill"
ACCOUNT = "account"
POSITION = "position"


def orders() -> dict[str, str]:
    return {"instType": UTA, "topic": ORDER}


def fills() -> dict[str, str]:
    return {"instType": UTA, "topic": FILL}


def account() -> dict[str, str]:
    return {"instType": UTA, "topic": ACCOUNT}


def positions() -> dict[str, str]:
    return {"instType": UTA, "topic": POSITION}


def arg_key(arg: dict[str, Any]) -> tuple[str, str, str, str]:
    """Identity of a subscribe arg, for routing and de-duplication."""
    return (
        str(arg.get("topic") or ""),
        str(arg.get("symbol") or ""),
        str(arg.get("instType") or ""),
        str(arg.get("interval") or ""),
    )


# --- REST paths ------------------------------------------------------------

MARKET_INSTRUMENTS = "/api/v3/market/instruments"
MARKET_TICKERS = "/api/v3/market/tickers"
MARKET_ORDERBOOK = "/api/v3/market/orderbook"
MARKET_CANDLES = "/api/v3/market/candles"
MARKET_OPEN_INTEREST = "/api/v3/market/open-interest"
MARKET_FUNDING_HISTORY = "/api/v3/market/history-fund-rate"
MARKET_CURRENT_FUNDING = "/api/v3/market/current-fund-rate"
MARKET_TIME = "/api/v3/public/time"

ORDER_PLACE = "/api/v3/trade/place-order"
ORDER_CANCEL = "/api/v3/trade/cancel-order"
ORDER_AMEND = "/api/v3/trade/modify-order"
ORDER_INFO = "/api/v3/trade/order-info"
ORDERS_UNFILLED = "/api/v3/trade/unfilled-orders"
ORDERS_HISTORY = "/api/v3/trade/history-orders"
FILLS = "/api/v3/trade/fills"

ACCOUNT_SETTINGS = "/api/v3/account/settings"
ACCOUNT_ASSETS = "/api/v3/account/assets"
POSITION_CURRENT = "/api/v3/position/current-position"


__all__ = [
    "ACCOUNT",
    "ACCOUNT_ASSETS",
    "ACCOUNT_SETTINGS",
    "BOOKS",
    "BOOKS1",
    "BOOKS15",
    "BOOKS5",
    "BOOK_DEPTHS",
    "FILL",
    "FILLS",
    "KLINE",
    "KLINE_BARS",
    "LIQUIDATION",
    "MARKET_CANDLES",
    "MARKET_CURRENT_FUNDING",
    "MARKET_FUNDING_HISTORY",
    "MARKET_INSTRUMENTS",
    "MARKET_OPEN_INTEREST",
    "MARKET_ORDERBOOK",
    "MARKET_TICKERS",
    "MARKET_TIME",
    "ORDER",
    "ORDERS_HISTORY",
    "ORDERS_UNFILLED",
    "ORDER_AMEND",
    "ORDER_CANCEL",
    "ORDER_INFO",
    "ORDER_PLACE",
    "POSITION",
    "POSITION_CURRENT",
    "PUBLIC_TRADE",
    "TICKER",
    "UTA",
    "account",
    "arg_key",
    "books",
    "fills",
    "kline",
    "liquidation",
    "orders",
    "positions",
    "public_trade",
    "ticker",
]

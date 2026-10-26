__all__ = [
    "AGG_TRADE",
    "BEST_QUOTE",
    "FUNDING_RATE",
    "GREEKS",
    "LIQUIDATION",
    "MARK_PRICE",
    "OPEN_INTEREST",
    "ORDER_BOOK_DELTA",
    "TICKER",
    "TRADE",
    "kline",
]

BEST_QUOTE: str
TRADE: str
AGG_TRADE: str
ORDER_BOOK_DELTA: str
TICKER: str
LIQUIDATION: str
FUNDING_RATE: str
OPEN_INTEREST: str
GREEKS: str
MARK_PRICE: str

def kline(interval: str) -> str: ...

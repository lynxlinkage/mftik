"""Binance COIN-M wire models used by the public REST reads.

Depth is the only payload this slice maps. User-stream and order models wait
for the private client. Klines go through
:func:`~mftik.exchange.binance.models.kline_from_row` with
``quote_per_contract`` — dapi's ``[5]`` is a contract count, not base.
"""

from __future__ import annotations

from typing import Any

from pydantic import Field

from mftik.exchange.binance.models import BinanceMessage, levels, secs
from mftik.exchange.models import OrderBook
from mftik.exchange.tickers import UniversalTicker


class BinanceDeliveryDepth(BinanceMessage):
    """The ``depth`` reply — a whole book, no symbol on the body."""

    last_update_id: int = Field(default=0, alias="lastUpdateId")
    event_time: int = Field(default=0, alias="E")
    transact_time: int = Field(default=0, alias="T")
    bids: list[Any] = Field(default_factory=list)
    asks: list[Any] = Field(default_factory=list)

    def to_order_book(
        self, ticker: UniversalTicker, *, ts: float | None = None
    ) -> OrderBook:
        stamp = ts if ts is not None else secs(self.transact_time or self.event_time)
        fields: dict[str, Any] = {} if not stamp else {"ts": stamp}
        return OrderBook(
            universal_ticker=str(ticker),
            bids=levels(self.bids),
            asks=levels(self.asks),
            **fields,
        )


__all__ = ["BinanceDeliveryDepth"]

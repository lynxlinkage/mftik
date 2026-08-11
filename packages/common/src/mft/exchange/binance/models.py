"""Wire-model primitives every Binance payload is built from.

Binance writes the same things the same way on both markets: timestamps in
milliseconds, book levels as ``[price, qty]`` pairs, sides in uppercase, and a
taker side that has to be read off a maker flag. Those readings live here so
spot and futures cannot drift on them; what each market spells differently —
order statuses, order types, which events exist at all — stays in its own
``models`` module, where the difference is visible.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict

from mft.exchange.models import BookLevel, Kline, Side
from mft.exchange.tickers import UniversalTicker


class BinanceMessage(BaseModel):
    """Base for wire models: tolerant of new fields, immutable once parsed."""

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="ignore")


def side_of(is_buyer_maker: bool) -> Side:
    """The aggressor's side on a public trade.

    ``m`` says whether the buyer was the maker. If they were, the taker — whose
    side the tape reports — was the seller. Reading it as "buy" inverts every
    trade on the feed.
    """
    return Side.SELL if is_buyer_maker else Side.BUY


def _lower(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


#: Binance spells sides ``BUY``/``SELL``; :class:`~mft.exchange.models.Side` is
#: lowercase. Folded on the way in rather than at each use, so a model field
#: annotated with this parses the venue's casing and holds ours.
VenueSide = Annotated[Side, BeforeValidator(_lower)]


def secs(ms: Any) -> float:
    """Binance timestamps are milliseconds, as ints, everywhere."""
    if ms is None or ms == "":
        return 0.0
    return float(ms) / 1000.0


def levels(rows: list[Any] | None) -> list[BookLevel]:
    """``[[price, qty], ...]`` — how every Binance book side is written."""
    out: list[BookLevel] = []
    for row in rows or []:
        if len(row) < 2:
            continue
        out.append(BookLevel(price=Decimal(str(row[0])), qty=Decimal(str(row[1]))))
    return out


def avg(quote_total: Decimal, base_total: Decimal) -> Decimal | None:
    """``Z / z`` — None while nothing has filled, rather than a zero price.

    Spot publishes the two totals and leaves the division to us. Futures
    publishes ``ap`` directly, so this is only reached where a payload does
    not — but the guard is the same one either way: dividing by an unfilled
    quantity is what makes an average price a zero.
    """
    if base_total <= 0:
        return None
    return quote_total / base_total


def kline_from_row(
    row: list[Any], ticker: UniversalTicker, interval: str
) -> Kline:
    """One row of the ``klines`` reply — a positional array, not an object.

    Binance's column order *is* OHLC, unlike some venues, but the two volumes
    sit either side of a second timestamp, so they cannot be read positionally
    from the OHLC block::

        [0] open time, ms      [5] volume, base
        [1] open               [6] close time, ms
        [2] high               [7] quote volume
        [3] low                [8] trade count
        [4] close              ...

    A row from this endpoint is always a closed window except the last one,
    which is the bar in progress — and the reply says nothing about which is
    which. It is reported as closed here and the caller drops or keeps the tail
    knowing the interval, because guessing from a timestamp would be wrong
    exactly at the boundary that matters.
    """
    if len(row) < 8:
        raise ValueError(
            f"kline row for {ticker} {interval} has {len(row)} columns, "
            f"expected at least 8: {row!r}"
        )
    return Kline(
        universal_ticker=str(ticker),
        interval=interval,
        open_time=secs(row[0]),
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=Decimal(str(row[5])),
        quote_volume=Decimal(str(row[7])),
        closed=True,
    )


__all__ = [
    "BinanceMessage",
    "VenueSide",
    "avg",
    "kline_from_row",
    "levels",
    "secs",
    "side_of",
]

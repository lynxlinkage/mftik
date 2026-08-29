"""What an order commits, and therefore what has to be held for it.

TD is the authority here: it pre-locks against this figure before an order
leaves, and refuses the order outright if the account cannot cover it. But a
strategy that cannot predict the same figure only learns "not enough money" as
one rejection per tick, so it asks the question itself first — which means the
arithmetic has to be readable from both sides, and TD and STS are separate apps
that do not import each other. That is why it lives here rather than in either.

One copy rather than two that agree. A strategy computing its own would drift
from the one TD enforces, and the drift is invisible until an order is refused
on an account that could perfectly well afford it.
"""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.models import (
    Instrument,
    OrderType,
    PlaceOrderRequest,
    Side,
)
from mftik.exchange.tickers import Category

ZERO = Decimal("0")
ONE = Decimal("1")


def commitment_for(
    *,
    category: Category,
    side: Side,
    order_type: OrderType,
    base: str,
    quote: str,
    qty: Decimal | None = None,
    quote_qty: Decimal | None = None,
    price: Decimal | None = None,
    leverage: Decimal | None = None,
) -> tuple[str, Decimal] | None:
    """What an order of this shape commits: ``(asset, amount)``, or None.

    Spot: a buy commits quote currency, a sell commits base. A market buy
    sized in base has no price to size the commitment with, so it returns
    None — the caller decides whether to let it through unreserved rather
    than having a guess baked in here. A spot buy sized in ``quote_qty``
    commits that amount of quote; a spot sell sized that way delivers base
    we cannot size without a price, so it is unknowable too.

    Perp: both sides commit margin in the quote (settle) asset. The amount is
    ``notional / leverage``. Missing or non-positive ``leverage`` is treated
    as ``1`` — conservative until the leverage cache has been filled.

    ``None`` means unknowable, never nothing: a caller that then reserves
    nothing has *decided* to, which is not the same as reserving zero.

    Takes scalars because the earliest caller has no order yet — a strategy
    asks this to decide whether to build one at all. See
    :func:`reservation_for` for the reading of an order already built.
    """
    if quote_qty is not None:
        if category is Category.PERP:
            return quote, quote_qty / _leverage_or_one(leverage)
        if side is Side.SELL:
            # Quote-sized, but base is what leaves. No price, no amount.
            return None
        return quote, quote_qty
    if qty is None:
        return None
    if category is Category.PERP:
        if order_type is OrderType.MARKET or price is None:
            return None
        return quote, (qty * price) / _leverage_or_one(leverage)
    if side is Side.SELL:
        return base, qty
    if order_type is OrderType.MARKET or price is None:
        return None
    return quote, qty * price


def reservation_for(
    request: PlaceOrderRequest,
    instrument: Instrument,
    *,
    leverage: Decimal | None = None,
) -> tuple[str, Decimal] | None:
    """:func:`commitment_for` read off an order that has already been built.

    What TD calls at submit, where the request is the thing being reserved
    against and the instrument is where the two asset names come from.
    """
    return commitment_for(
        category=request.category,
        side=request.side,
        order_type=request.type,
        base=instrument.base,
        quote=instrument.quote,
        qty=request.qty,
        quote_qty=request.quote_qty,
        price=request.price,
        leverage=leverage,
    )


def _leverage_or_one(leverage: Decimal | None) -> Decimal:
    return leverage if leverage is not None and leverage > ZERO else ONE


__all__ = ["commitment_for", "reservation_for"]

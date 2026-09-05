"""Offline shape and capability checks for a :class:`PlaceOrderRequest`.

Does not talk to a venue. Lot size, min notional, balance, and whether the
order would fill are someone else's job. The kinds returned here are for TD
to map onto its own 1xx codes — this module does not import reject codes.
"""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange import venues
from mftik.exchange.errors import OrderError
from mftik.exchange.models import OrderType, PlaceOrderRequest, Side
from mftik.exchange.tickers import Category, InvalidTickerError

#: Constructed wrong — missing size, both sizes, a limit without a price.
SHAPE = "shape"
#: ``reduce_only`` on a spot instrument; no venue has anywhere to apply it.
REDUCE_ONLY = "reduce_only"
#: This venue cannot express that size unit on this order.
VENUE = "venue"

#: What an order may be sized in, when we know the venue.
_EITHER = "either"
_QTY = "qty"
_QUOTE = "quote"

_BOTH_QTY = {Side.BUY: _QTY, Side.SELL: _QTY}
_BOTH_EITHER = {Side.BUY: _EITHER, Side.SELL: _EITHER}

#: ``(venue, category)`` → what a market order there may be sized in, per side.
_MARKET_SIZE: dict[tuple[str, Category], dict[Side, str]] = {
    ("Paper", Category.SPOT): _BOTH_EITHER,
    ("Gate", Category.SPOT): {Side.BUY: _QUOTE, Side.SELL: _QTY},
    ("GateFutures", Category.PERP): _BOTH_QTY,
    ("Binance", Category.SPOT): _BOTH_EITHER,
    ("BinanceUM", Category.PERP): _BOTH_QTY,
    ("BinanceUM", Category.FUTURE): _BOTH_QTY,
    # Contract count as ``qty``. Inverse margin-in-coin is a reservation
    # question, not a ``quote_qty`` one.
    ("BinanceCM", Category.INVERSE): _BOTH_QTY,
    ("BinanceCM", Category.FUTURE): _BOTH_QTY,
    ("Bybit", Category.SPOT): _BOTH_EITHER,
    ("Bybit", Category.PERP): _BOTH_QTY,
    # Spot market buys default to quote unless ``tgtCcy=base_ccy``; we send
    # that flag, so both units are expressible. SWAP sizes in contracts.
    ("Okx", Category.SPOT): _BOTH_EITHER,
    ("Okx", Category.PERP): _BOTH_QTY,
    # Spot market buy ``qty`` is quote on the wire (V6 / I9). Limit and
    # market sell are base. Both linear perps size in base.
    ("Bitget", Category.SPOT): {Side.BUY: _QUOTE, Side.SELL: _QTY},
    ("Bitget", Category.PERP): _BOTH_QTY,
    # Linear and spot size in the base coin (V6). Inverse USD amounts are
    # out of scope; ``quote_qty`` is not expressible on either book.
    ("Deribit", Category.SPOT): _BOTH_QTY,
    ("Deribit", Category.PERP): _BOTH_QTY,
}

#: The same question for a limit order. Uniformly base today, which is why it
#: reads as one rule rather than a table — but it is a table because it is a
#: venue capability and not a property of the order type: an inverse
#: (coin-margined) book sizes *every* order in the quote asset, limits
#: included, so the book has to be able to say so. Stated per book rather than
#: assumed, so adding such a book is a row here and not an exception in
#: :func:`_shape_reason`.
_LIMIT_SIZE: dict[tuple[str, Category], dict[Side, str]] = {
    ("Paper", Category.SPOT): _BOTH_QTY,
    ("Gate", Category.SPOT): _BOTH_QTY,
    ("GateFutures", Category.PERP): _BOTH_QTY,
    ("Binance", Category.SPOT): _BOTH_QTY,
    ("BinanceUM", Category.PERP): _BOTH_QTY,
    ("BinanceUM", Category.FUTURE): _BOTH_QTY,
    ("BinanceCM", Category.INVERSE): _BOTH_QTY,
    ("BinanceCM", Category.FUTURE): _BOTH_QTY,
    ("Bybit", Category.SPOT): _BOTH_QTY,
    ("Bybit", Category.PERP): _BOTH_QTY,
    ("Okx", Category.SPOT): _BOTH_QTY,
    ("Okx", Category.PERP): _BOTH_QTY,
    ("Bitget", Category.SPOT): _BOTH_QTY,
    ("Bitget", Category.PERP): _BOTH_QTY,
    ("Deribit", Category.SPOT): _BOTH_QTY,
    ("Deribit", Category.PERP): _BOTH_QTY,
}

#: Every book in :mod:`~mftik.exchange.venues` needs a row in *both* tables. A
#: missing one reads as "no opinion", which :func:`_venue_reason` treats as
#: permission — so without the check below, adding a venue to the registry
#: would silently sign this module up to approve a size unit nobody had
#: confirmed its adapter can send, which is the one thing this module exists to
#: prevent.
_SIZE_TABLES: dict[OrderType, dict[tuple[str, Category], dict[Side, str]]] = {
    OrderType.MARKET: _MARKET_SIZE,
    OrderType.LIMIT: _LIMIT_SIZE,
}


def check_rules() -> None:
    """Assert every registered book says how each order type is sized."""
    missing = [
        f"{name}/{category.value}/{order_type.value}"
        for order_type, table in _SIZE_TABLES.items()
        for name, venue in venues.VENUES.items()
        for category in sorted(venue.categories)
        if (name, category) not in table
    ]
    if missing:
        raise ValueError(
            "no order size rule for " + ", ".join(sorted(missing))
            + "; add one to order_check._SIZE_TABLES"
        )


check_rules()


def classify(request: PlaceOrderRequest) -> tuple[str, str] | None:
    """Why ``request`` must not be sent, or ``None``.

    ``(kind, reason)`` — ``kind`` is :data:`SHAPE`, :data:`REDUCE_ONLY` or
    :data:`VENUE`. Shape is also enforced by the request's own validator; it
    is repeated here so a caller that skipped construction still gets an
    answer, and so one function covers every refusal TD maps.
    """
    shaped = _shape_reason(request)
    if shaped is not None:
        return SHAPE, shaped
    reduced = _reduce_only_reason(request)
    if reduced is not None:
        return REDUCE_ONLY, reduced
    venue = _venue_reason(request)
    if venue is not None:
        return VENUE, venue
    return None


def refusal_reason(request: PlaceOrderRequest) -> str | None:
    """Human-readable refusal, or ``None`` if the request may be sent."""
    found = classify(request)
    return None if found is None else found[1]


def require_legal(request: PlaceOrderRequest) -> None:
    """Raise :class:`OrderError` if ``request`` must not reach a venue."""
    reason = refusal_reason(request)
    if reason is not None:
        raise OrderError(reason)


def sized_amount(request: PlaceOrderRequest) -> Decimal:
    """The number that belongs on the venue's size field.

    ``quote_qty`` when the order is quote-sized, otherwise ``qty``. The
    request must already have passed shape checks — exactly one is set.
    """
    if request.quote_qty is not None:
        return request.quote_qty
    assert request.qty is not None
    return request.qty


def _shape_reason(request: PlaceOrderRequest) -> str | None:
    """What is wrong with the request on its own terms, whatever the venue.

    Which *unit* a size may be in is deliberately not here: that is a venue
    capability, which is what :data:`VENUE` and :func:`_venue_reason` are for.
    Shape only asks whether the request says one coherent thing — one size, and
    a price where a price is the difference between a limit and a market order.
    """
    if request.qty is not None and request.qty <= 0:
        return f"qty must be positive, got {request.qty}"
    if request.quote_qty is not None and request.quote_qty <= 0:
        return f"quote_qty must be positive, got {request.quote_qty}"
    if request.type is OrderType.LIMIT and request.price is None:
        return "limit order requires a price"
    if (request.qty is None) == (request.quote_qty is None):
        return f"{request.type} order requires exactly one of qty or quote_qty"
    return None


def _reduce_only_reason(request: PlaceOrderRequest) -> str | None:
    if not request.reduce_only:
        return None
    try:
        ticker = request.ticker
    except InvalidTickerError:
        # The instrument check answers a malformed ticker; this one does not.
        return None
    if ticker.category is Category.SPOT:
        return (
            f"reduce_only is not a spot concept; {request.universal_ticker} "
            "has no position to reduce"
        )
    return None


def _venue_reason(request: PlaceOrderRequest) -> str | None:
    try:
        ticker = request.ticker
    except InvalidTickerError:
        return None
    rule = _size_rule(
        ticker.venue, ticker.category, request.type, request.side
    )
    if rule is None:
        return None
    if rule is _EITHER:
        return None
    if rule is _QUOTE and request.quote_qty is None:
        return (
            f"{ticker.venue} {request.type} {request.side} sizes in quote "
            "currency; set quote_qty, not qty"
        )
    if rule is _QTY and request.quote_qty is not None:
        return (
            f"{ticker.venue} {request.type} orders size in base; "
            "quote_qty is not expressible"
        )
    return None


def _size_rule(
    venue: str, category: Category, order_type: OrderType, side: Side
) -> str | None:
    """How this book sizes that order, or ``None`` if we have no opinion.

    ``None`` only for a venue the registry does not have — nothing can route
    such a ticker anyway, so there is no table to invent. Every *registered*
    book has a row for every order type, which :func:`check_rules` enforces at
    import.
    """
    by_side = _SIZE_TABLES[order_type].get((venues.normalize(venue), category))
    return None if by_side is None else by_side[side]

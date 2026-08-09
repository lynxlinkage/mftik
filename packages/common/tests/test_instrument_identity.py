"""Payload identity — what a market update or an order says it is about.

The defect this closes: MD routes a strategy's updates to a hook by *message
type*, so every order book lands on ``on_order_book`` whichever feed it came
from, and the envelope carries no feed key. A payload identified by ``BTCUSDT``
alone left a strategy holding two feeds unable to tell them apart.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.models import (
    BestQuote,
    BookLevel,
    Fill,
    InstrumentScoped,
    Kline,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    Ticker,
    Trade,
    limit_order,
    market_order,
)
from mft.exchange.tickers import Category, InvalidTickerError, UniversalTicker


def _book(ticker: str) -> OrderBook:
    return OrderBook(
        universal_ticker=ticker,
        bids=[BookLevel(price=Decimal("1"), qty=Decimal("1"))],
        asks=[BookLevel(price=Decimal("2"), qty=Decimal("1"))],
    )


def test_two_venues_books_are_no_longer_the_same_message() -> None:
    """The case that motivated this: a cross-venue strategy holds both feeds,
    both books arrive on ``on_order_book``, and both are ``BTCUSDT``."""
    binance = _book("Binance_Spot_BTCUSDT")
    gate = _book("Gate_Spot_BTCUSDT")

    assert binance.symbol == gate.symbol == "BTCUSDT"
    assert binance.venue == "Binance"
    assert gate.venue == "Gate"
    assert binance.universal_ticker != gate.universal_ticker


def test_a_unified_venues_two_books_are_distinguishable() -> None:
    """Same venue, same symbol, different market — and different tick sizes."""
    spot = _book("Bybit_Spot_BTCUSDT")
    perp = _book("Bybit_Perp_BTCUSDT")

    assert spot.venue == perp.venue == "Bybit"
    assert spot.symbol == perp.symbol == "BTCUSDT"
    assert spot.category is Category.SPOT
    assert perp.category is Category.PERP


def test_the_parts_agree_with_the_parsed_ticker() -> None:
    """``symbol`` and ``venue`` are splits rather than parses, for the hot
    path — so they have to agree with the parse that is authoritative."""
    book = _book("Bybit_Perp_BTCUSDT")
    ticker = book.ticker

    assert isinstance(ticker, UniversalTicker)
    assert (book.venue, book.category, book.symbol) == (
        ticker.venue,
        ticker.category,
        ticker.symbol,
    )
    assert str(ticker) == book.universal_ticker


def test_identity_is_not_validated_on_construction_but_is_on_reading() -> None:
    """Deliberate: these are the per-message models, and the only writers are
    adapters building the ticker from the plane rather than from a string.

    The cost of being wrong is paid at the read that cares, not on every tick.
    """
    bad = _book("BTCUSDT")
    assert bad.symbol == "BTCUSDT"
    with pytest.raises(InvalidTickerError):
        bad.ticker
    with pytest.raises(InvalidTickerError):
        bad.category


@pytest.mark.parametrize(
    "event",
    [
        Ticker(
            universal_ticker="Gate_Spot_BTCUSDT",
            bid=Decimal("1"),
            ask=Decimal("2"),
            last=Decimal("1.5"),
        ),
        Trade(
            universal_ticker="Gate_Spot_BTCUSDT",
            price=Decimal("1"),
            qty=Decimal("1"),
            side=Side.BUY,
        ),
        Kline(
            universal_ticker="Gate_Spot_BTCUSDT",
            interval="1m",
            open_time=0.0,
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
        ),
        BestQuote(
            universal_ticker="Gate_Spot_BTCUSDT",
            bid=Decimal("1"),
            bid_qty=Decimal("1"),
            ask=Decimal("2"),
            ask_qty=Decimal("1"),
        ),
        _book("Gate_Spot_BTCUSDT"),
        Order(
            universal_ticker="Gate_Spot_BTCUSDT",
            side=Side.BUY,
            type=OrderType.LIMIT,
            status=OrderStatus.NEW,
            qty=Decimal("1"),
        ),
        Fill(
            universal_ticker="Gate_Spot_BTCUSDT",
            order_id="1",
            side=Side.BUY,
            price=Decimal("1"),
            qty=Decimal("1"),
        ),
    ],
    ids=["ticker", "trade", "kline", "bestquote", "orderbook", "order", "fill"],
)
def test_every_payload_names_its_instrument_the_same_way(
    event: InstrumentScoped,
) -> None:
    """One identity across market data and order events alike — including the
    two that travel to a strategy through TD rather than MD."""
    assert isinstance(event, InstrumentScoped)
    assert event.universal_ticker == "Gate_Spot_BTCUSDT"
    assert event.symbol == "BTCUSDT"
    assert event.venue == "Gate"


def test_identity_survives_the_wire() -> None:
    """These cross Redis as JSON; the ticker is one string on purpose."""
    book = _book("Bybit_Perp_BTCUSDT")
    payload = book.model_dump(mode="json")

    assert payload["universal_ticker"] == "Bybit_Perp_BTCUSDT"
    assert "symbol" not in payload
    assert OrderBook.model_validate(payload).ticker == book.ticker


# --- the request names one too -----------------------------------------------


def test_an_order_request_names_its_instrument_like_everything_else() -> None:
    """The last thing in the platform that spoke in bare symbols.

    A request is not an event, which is why the base is not called one: what
    they share is that everything on them is scoped to the instrument they
    name.
    """
    request = PlaceOrderRequest(
        universal_ticker="Bybit_Perp_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        qty=Decimal("1"),
        price=Decimal("60000"),
    )

    assert isinstance(request, InstrumentScoped)
    assert request.symbol == "BTCUSDT"
    assert request.venue == "Bybit"
    # What a unified connector reads to decide which book the order goes to,
    # per order rather than per session.
    assert request.category is Category.PERP


def test_the_builders_take_a_ticker_in_either_form() -> None:
    """A caller usually holds the parsed one; a test usually writes the string."""
    parsed = UniversalTicker.parse("Gate_Spot_BTCUSDT")
    from_object = limit_order(
        ticker=parsed, side=Side.BUY, qty=Decimal("1"), price=Decimal("2")
    )
    from_string = market_order(
        ticker="Gate_Spot_BTCUSDT", side=Side.SELL, qty=Decimal("1")
    )

    assert from_object.ticker == parsed
    assert from_string.ticker == parsed
    assert from_string.type is OrderType.MARKET

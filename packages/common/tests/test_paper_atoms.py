"""Paper atoms for the order book (B4-06).

The remote public client streams the book and nothing else. Capacity is
stated and not measured; B7-02g owns a measured value and the topics
whose remote streams are still unwired.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.atoms import (
    TOPIC_ORDERBOOK,
    TOPIC_TICKER,
    Atom,
    AtomOptions,
    InvalidAtomError,
    JoinPolicy,
    Projector,
    UnknownEndpointError,
    UnsupportedTopicError,
)
from mftik.exchange.models import OrderBook
from mftik.exchange.paper.atoms import (
    PUBLIC,
    VENUE,
    atoms_for,
    capacity,
    decode,
    join_policy,
    parse_channel,
)
from mftik.exchange.tickers import UniversalTicker

BTC = UniversalTicker.parse("Paper_Spot_BTCUSDT")
BOOK = Atom(VENUE, PUBLIC, "orderbook.BTCUSDT")


def test_an_order_book_is_one_public_atom() -> None:
    """One channel per (topic, symbol), passthrough, nothing to fold."""
    plan = atoms_for(TOPIC_ORDERBOOK, BTC, AtomOptions(depth=5))
    assert plan.topic == TOPIC_ORDERBOOK
    assert plan.ticker == BTC
    assert plan.projector is Projector.PASSTHROUGH
    assert plan.atoms == (BOOK,)
    assert plan.atom_ids == ("Paper:public:orderbook.BTCUSDT",)
    assert parse_channel(BOOK.channel) == (TOPIC_ORDERBOOK, "BTCUSDT")
    # Depth is not part of the channel: the push is the whole book.
    assert atoms_for(TOPIC_ORDERBOOK, BTC, AtomOptions()).atoms == plan.atoms
    assert join_policy(BOOK) is JoinPolicy.NEXT_PUSH


def test_only_the_streamed_topic_resolves() -> None:
    with pytest.raises(UnsupportedTopicError):
        atoms_for(TOPIC_TICKER, BTC, AtomOptions())
    other = UniversalTicker.parse("Binance_Spot_BTCUSDT")
    with pytest.raises(UnsupportedTopicError):
        atoms_for(TOPIC_ORDERBOOK, other, AtomOptions())


def test_decode_reads_one_complete_book() -> None:
    frame = {
        "universal_ticker": "Paper_Spot_BTCUSDT",
        "bids": [{"price": "50000", "qty": "1.5"}],
        "asks": [{"price": "50001", "qty": "2"}],
        "ts": 1.0,
    }
    events = decode(BOOK, frame)
    assert len(events) == 1
    book = events[0]
    assert isinstance(book, OrderBook)
    assert book.symbol == "BTCUSDT"
    assert book.venue == VENUE
    assert book.bids[0].price == Decimal("50000")
    assert book.asks[0].qty == Decimal("2")


def test_a_book_for_another_instrument_is_not_this_atom() -> None:
    frame = {
        "universal_ticker": "Paper_Spot_ETHUSDT",
        "bids": [{"price": "1", "qty": "1"}],
        "asks": [{"price": "2", "qty": "1"}],
    }
    assert decode(BOOK, frame) == []


def test_capacity_is_stated_for_public_only() -> None:
    """The numbers are placeholders. They are not a measured ceiling."""
    stated = capacity(PUBLIC)
    assert stated.max_atoms == 1024
    assert stated.max_messages_per_second == 1_000_000
    assert stated.subscribe_batch == 1024
    assert stated.subscribe_per_second == 1_000_000
    with pytest.raises(UnknownEndpointError):
        capacity("market")


def test_a_channel_needs_a_topic_and_a_symbol() -> None:
    with pytest.raises(InvalidAtomError):
        parse_channel("orderbook")
    with pytest.raises(InvalidAtomError):
        join_policy(Atom(VENUE, PUBLIC, "orderbook"))
    with pytest.raises(UnsupportedTopicError):
        join_policy(Atom(VENUE, PUBLIC, "ticker.BTCUSDT"))

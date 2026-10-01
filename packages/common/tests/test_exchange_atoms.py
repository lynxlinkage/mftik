"""The atom interface: identity, the per-venue shape, and two contract tests.

IF-08 defines this layer and returns null data. So the tests split in two:

* What is real now — an atom's identity, a plan's rendering, the topic
  vocabulary, and the fact that every venue has the four functions and that
  each of them refuses with the ticket number. These run and pass.
* What B7 will make true — ``xfail(strict=True)``, one per case the ticket's
  acceptance names. ``strict`` is the point: the implementation cannot land
  without deleting the marker, so the contract becomes a real test rather than
  a comment that rotted.
"""

from __future__ import annotations

from decimal import Decimal
from types import ModuleType
from typing import Any

import pytest
from mftik.exchange import atoms, venues
from mftik.exchange.atoms import (
    Atom,
    AtomOptions,
    AtomPlan,
    InvalidAtomError,
    JoinPolicy,
    Projector,
)
from mftik.exchange.binance.delivery import atoms as binance_cm_atoms
from mftik.exchange.binance.future import atoms as binance_um_atoms
from mftik.exchange.binance.spot import atoms as binance_spot_atoms
from mftik.exchange.bitget import atoms as bitget_atoms
from mftik.exchange.bybit import atoms as bybit_atoms
from mftik.exchange.deribit import atoms as deribit_atoms
from mftik.exchange.gate.future import atoms as gate_futures_atoms
from mftik.exchange.gate.spot import atoms as gate_spot_atoms
from mftik.exchange.models import Greeks, OpenInterest, Ticker, TickerStats
from mftik.exchange.okx import atoms as okx_atoms
from mftik.exchange.paper import atoms as paper_atoms
from mftik.exchange.tickers import UniversalTicker

#: Every venue in the registry has exactly one atoms module. Keyed by the venue
#: name so a venue added without one shows up as a missing key rather than as a
#: count that no longer matches.
VENUE_ATOMS: dict[str, ModuleType] = {
    "Binance": binance_spot_atoms,
    "BinanceCM": binance_cm_atoms,
    "BinanceUM": binance_um_atoms,
    "Bitget": bitget_atoms,
    "Bybit": bybit_atoms,
    "Deribit": deribit_atoms,
    "Gate": gate_spot_atoms,
    "GateFutures": gate_futures_atoms,
    "Okx": okx_atoms,
    "Paper": paper_atoms,
}

ADAPTER_FUNCTIONS = ("atoms_for", "decode", "capacity", "join_policy")

BINANCE_UM_PERP = UniversalTicker.parse("BinanceUM_Perp_BTCUSDT")
DERIBIT_OPTION = UniversalTicker.parse("Deribit_Option_BTCUSD-260913-70000-C")
#: Deribit's own spelling of that option, which is what an atom's channel
#: carries (A1).
DERIBIT_OPTION_WIRE = "BTC-13SEP26-70000-C"


# --- identity --------------------------------------------------------------


def test_an_atom_renders_and_parses_back() -> None:
    atom = Atom("BinanceUM", "public", "btcusdt@bookTicker")
    assert atom.atom_id == "BinanceUM:public:btcusdt@bookTicker"
    assert str(atom) == atom.atom_id
    assert Atom.parse(atom.atom_id) == atom


def test_a_channel_may_contain_the_separator() -> None:
    """OKX's normalized ``arg`` is ``tickers:BTC-USDT-SWAP``.

    Only the venue and the endpoint are separated off, so a channel that spells
    itself with ``:`` still round-trips. Nothing else about the channel is
    normalized: it is the venue's string, verbatim (A1).
    """
    atom = Atom("Okx", "public", "tickers:BTC-USDT-SWAP")
    assert atom.atom_id == "Okx:public:tickers:BTC-USDT-SWAP"
    assert Atom.parse(atom.atom_id) == atom


def test_atoms_sort_and_key_without_a_key_function() -> None:
    """A desired set is a set of atoms, and placement sorts them."""
    one = Atom("Deribit", "public", "book.BTC-PERPETUAL.none.20.100ms")
    two = Atom("Deribit", "public", "ticker.BTC-PERPETUAL.100ms")
    again = Atom("Deribit", "public", "ticker.BTC-PERPETUAL.100ms")
    assert sorted({two, one, again}) == [one, two]


@pytest.mark.parametrize(
    "atom_id",
    ["", "BinanceUM", "BinanceUM:public", ":public:btcusdt@ticker"],
)
def test_a_malformed_atom_id_is_refused(atom_id: str) -> None:
    with pytest.raises(InvalidAtomError):
        Atom.parse(atom_id)


def test_an_atom_needs_all_three_parts() -> None:
    with pytest.raises(InvalidAtomError):
        Atom("BinanceUM", "public", "")
    with pytest.raises(InvalidAtomError):
        Atom("Binance:UM", "public", "btcusdt@ticker")


def test_a_plan_renders_its_atom_ids_for_the_intent_record() -> None:
    """MD answers an intent with ``{feed: [atom_id]}`` (§6.1)."""
    plan = AtomPlan(
        topic=atoms.TOPIC_TICKER,
        ticker=BINANCE_UM_PERP,
        atoms=(
            Atom("BinanceUM", "market", "btcusdt@ticker"),
            Atom("BinanceUM", "public", "btcusdt@bookTicker"),
        ),
        projector=Projector.QUOTE_STATS,
    )
    assert plan.atom_ids == (
        "BinanceUM:market:btcusdt@ticker",
        "BinanceUM:public:btcusdt@bookTicker",
    )


def test_a_plan_is_passthrough_unless_it_says_otherwise() -> None:
    plan = AtomPlan(
        topic=atoms.TOPIC_TRADE,
        ticker=BINANCE_UM_PERP,
        atoms=(Atom("BinanceUM", "market", "btcusdt@aggTrade"),),
    )
    assert plan.projector is Projector.PASSTHROUGH


def test_a_kline_topic_carries_its_interval() -> None:
    assert atoms.kline_interval("kline_1m") == "1m"
    assert atoms.kline_interval(atoms.TOPIC_TICKER) == ""


# --- the per-venue shape ---------------------------------------------------


def test_every_registered_venue_has_an_atoms_module() -> None:
    assert set(VENUE_ATOMS) == set(venues.names())


@pytest.mark.parametrize("venue", sorted(VENUE_ATOMS))
def test_a_venue_module_has_the_four_functions(venue: str) -> None:
    module = VENUE_ATOMS[venue]
    for name in ADAPTER_FUNCTIONS:
        assert callable(getattr(module, name)), f"{venue} has no {name}"


@pytest.mark.parametrize("venue", sorted(VENUE_ATOMS))
def test_a_venue_module_refuses_with_the_ticket_number(venue: str) -> None:
    """Null data, and it says which ticket owns the gap (IF 共同驗收 2)."""
    module = VENUE_ATOMS[venue]
    atom = Atom(venue, "public", "whatever")
    ticker = UniversalTicker.parse(venues.require(venue).ticker_example)
    calls = {
        "atoms_for": lambda: module.atoms_for(
            atoms.TOPIC_TICKER, ticker, AtomOptions()
        ),
        "decode": lambda: module.decode(atom, {}),
        "capacity": lambda: module.capacity("public"),
        "join_policy": lambda: module.join_policy(atom),
    }
    for name, call in calls.items():
        with pytest.raises(NotImplementedError, match="IF-08") as caught:
            call()
        assert name in str(caught.value)


def test_ticker_stats_has_no_quote_in_it() -> None:
    """The half of a 24h ticker that is not a quote (F19).

    The point of the model is what it leaves out: a venue whose stats row has no
    bid and no ask cannot be turned into a
    :class:`~mftik.exchange.models.Ticker` without inventing one.
    """
    stats = TickerStats(universal_ticker=str(BINANCE_UM_PERP), last=Decimal("60000"))
    assert not {"bid", "ask"} & set(type(stats).model_fields)
    assert stats.ticker == BINANCE_UM_PERP
    assert stats.volume is None


# --- contracts B7 has to make true -----------------------------------------


@pytest.mark.xfail(strict=True, reason="B7-02b implements BinanceUM's atoms")
def test_binance_um_ticker_resolves_to_ticker_and_book_ticker_atoms() -> None:
    """One feed, two atoms, on the two endpoints that answer them (F19).

    ``@ticker`` carries the rolling stats and no quote; ``@bookTicker`` carries
    the quote and no stats. Which endpoint each one goes to is part of the
    contract rather than a detail: a subscribe on the wrong host is
    acknowledged and then never pushes.

    The ``quote_stats`` projector is what pairs them back into a ``Ticker``, on
    the STS ingress, because the two atoms may be on two connections and
    therefore in two processes.
    """
    plan = binance_um_atoms.atoms_for(
        atoms.TOPIC_TICKER, BINANCE_UM_PERP, AtomOptions()
    )

    assert plan.ticker == BINANCE_UM_PERP
    assert plan.projector is Projector.QUOTE_STATS
    by_channel = {atom.channel: atom for atom in plan.atoms}
    assert set(by_channel) == {"btcusdt@ticker", "btcusdt@bookTicker"}
    assert by_channel["btcusdt@ticker"].endpoint == binance_um_atoms.MARKET
    assert by_channel["btcusdt@bookTicker"].endpoint == binance_um_atoms.PUBLIC
    assert all(atom.venue == "BinanceUM" for atom in plan.atoms)

    # The two atoms decode to the two halves the join needs.
    stats = binance_um_atoms.decode(
        by_channel["btcusdt@ticker"],
        {"e": "24hrTicker", "E": 1700000000000, "s": "BTCUSDT", "c": "60000"},
    )
    assert [type(event) for event in stats] == [TickerStats]


@pytest.mark.xfail(strict=True, reason="B7-02a implements Deribit's atoms")
def test_a_deribit_ticker_frame_decodes_to_three_platform_events() -> None:
    """One channel, one frame, several events (F19) — the other direction.

    Deribit puts the quote, the open interest and (on an option) the greeks on
    the same ``ticker.*`` row, so five platform topics share one atom and
    ``decode`` is what fans the row out. This is why it returns a list.

    Only the three events the ticket names are required. A ``BestQuote`` off the
    same row is allowed; what is not allowed is MD publishing the venue's row
    (F21) or a consumer having to subscribe twice to see two of these.
    """
    atom = Atom("Deribit", "public", f"ticker.{DERIBIT_OPTION_WIRE}.100ms")
    frame: dict[str, Any] = {
        "instrument_name": DERIBIT_OPTION_WIRE,
        "last_price": "0.052",
        "best_bid_price": "0.051",
        "best_bid_amount": "2.5",
        "best_ask_price": "0.053",
        "best_ask_amount": "1.5",
        "open_interest": "123.4",
        "mark_price": "0.0525",
        "underlying_price": "65000",
        "mark_iv": "65.0",
        "greeks": {
            "delta": "0.55",
            "gamma": "0.01",
            "theta": "-12.5",
            "vega": "18.2",
            "rho": "3.1",
        },
        "timestamp": 1700000001000,
    }

    events = deribit_atoms.decode(atom, frame)

    by_type = {type(event): event for event in events}
    assert {Ticker, Greeks, OpenInterest} <= set(by_type)
    assert {event.universal_ticker for event in events} == {str(DERIBIT_OPTION)}
    assert by_type[Ticker].last == Decimal("0.052")
    assert by_type[Greeks].delta == Decimal("0.55")
    # A decimal fraction, not a percent — the row says 65.0.
    assert by_type[Greeks].mark_iv == Decimal("0.65")
    assert by_type[OpenInterest].qty == Decimal("123.4")


@pytest.mark.xfail(strict=True, reason="B7-02a states Deribit's capacity")
def test_deribit_states_a_capacity_and_a_late_joiner_policy() -> None:
    """Placement needs a number, and a late joiner needs an answer (I5).

    The ticker row is a delta — it names only the fields that moved — so a
    consumer joining a live one waits for the next row carrying the field it
    reads. Said out loud here rather than filled in from REST.
    """
    capacity = deribit_atoms.capacity(deribit_atoms.PUBLIC)
    assert capacity.max_atoms > 0
    assert capacity.subscribe_batch > 0

    ticker_atom = Atom("Deribit", "public", f"ticker.{DERIBIT_OPTION_WIRE}.100ms")
    assert deribit_atoms.join_policy(ticker_atom) is JoinPolicy.SILENT

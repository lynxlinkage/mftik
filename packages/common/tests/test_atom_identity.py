"""Atom identity: shared normalization, the venue registry, the hash.

B7-01. ``atom_id`` is the only spelling that is hashed, and the hash is
the full SHA-256 hex pinned below. A table built again from the same
ids names the same subjects. Venue adapters and desired membership are
not this ticket.
"""

from __future__ import annotations

import hashlib
import itertools

import pytest
from mftik.exchange import venues
from mftik.exchange.atoms import (
    Atom,
    AtomTable,
    InvalidAtomError,
    UnknownAtomVenueError,
    adapter_for,
    atom_venues,
    load_adapters,
)
from mftik.protocol.topics import Topics, atom_hash

#: §6.1 examples. The hex is SHA-256 of that exact ``atom_id``, computed
#: once. A later change of the hash or of the rendering fails this
#: literal; the test does not recompute the expected value.
_DERIBIT_ID = "Deribit:public:ticker.BTC-27DEC26-100000-C.100ms"
_DERIBIT_HASH = "c7817d629ff98e7859edf216645b5158190c47e69238a355bff3f8ec0fe74fac"
_OKX_ID = "Okx:public:tickers:BTC-USDT-SWAP"
_OKX_HASH = "720ab013329ca5458e0160a70c2ebd594729a8bdfaf8bb415d58cda42d952c90"
_BINANCE_ID = "BinanceUM:market:btcusdt@bookTicker"
_BINANCE_HASH = "25ad82c4a102912e0e86180518b0018bb7b9dff96c61641ba1a70a81d838e418"

# Channels that contain the atom separator, a dot, or both. Identity
# examples, not a claim about which string B7-02 will subscribe.
_CHANNELS = (
    "btcusdt@bookTicker",
    "tickers:BTC-USDT-SWAP",
    "ticker.BTC-27DEC26-100000-C.100ms",
    "book.BTC-PERPETUAL.none.20.100ms",
    "futures.tickers:BTC_USDT",
    "a:b:c.d",
    ":",
    ".",
)


@pytest.mark.parametrize("channel", _CHANNELS)
def test_atom_id_round_trips_when_the_channel_has_separators(channel: str) -> None:
    atom = Atom("BinanceUM", "public", channel)
    assert Atom.parse(atom.atom_id) == atom
    assert Atom.parse(atom.atom_id).atom_id == atom.atom_id
    assert Atom.parse(atom.atom_id).channel == channel


def test_accepted_ids_round_trip_across_a_table() -> None:
    """Every combination of these parts parses back to itself."""
    venues_ = ("Paper", "Okx", "GateFutures")
    endpoints = ("public", "market")
    channels = ("a", "a.b", "a:b", "a:b.c.d", "x" * 40, "@ticker")
    pairs = itertools.product(venues_, endpoints, channels)
    for venue, endpoint, channel in pairs:
        atom = Atom(venue, endpoint, channel)
        assert Atom.parse(atom.atom_id) == atom


@pytest.mark.parametrize(
    ("venue", "endpoint", "channel"),
    [
        ("okx", "public", "tickers:BTC"),
        ("Okx", "Public", "tickers:BTC"),
        ("Okx", "public", ""),
        ("Okx", "public", "tickers BTC"),
        ("Okx", "public", "tickers\nBTC"),
        ("Okx_Swap", "public", "x"),
        ("", "public", "x"),
    ],
)
def test_shared_normalization_is_refused(
    venue: str, endpoint: str, channel: str
) -> None:
    with pytest.raises(InvalidAtomError):
        Atom(venue, endpoint, channel)


@pytest.mark.parametrize(
    "atom_id",
    [
        "okx:public:tickers:BTC",
        "Okx:Public:tickers:BTC",
        "Okx:public:tickers BTC",
        "Okx:public:",
        "Okx::tickers",
        "",
        "Okx",
        "Okx:public",
    ],
)
def test_parse_applies_the_same_rules(atom_id: str) -> None:
    with pytest.raises(InvalidAtomError):
        Atom.parse(atom_id)


def test_the_channel_is_not_case_folded() -> None:
    """Symbol case belongs to the venue adapter, not to this layer."""
    lower = Atom("BinanceUM", "public", "btcusdt@bookTicker")
    upper = Atom("BinanceUM", "public", "BTCUSDT@bookTicker")
    assert lower.atom_id != upper.atom_id
    assert atom_hash(lower.atom_id) != atom_hash(upper.atom_id)


def test_the_hash_is_the_golden_sha256() -> None:
    atom = Atom.parse(_DERIBIT_ID)
    assert atom.atom_id == _DERIBIT_ID
    digest = hashlib.sha256(_DERIBIT_ID.encode("utf-8")).hexdigest()
    assert atom_hash(atom.atom_id) == digest == _DERIBIT_HASH
    assert len(_DERIBIT_HASH) == 64
    assert "." not in _DERIBIT_HASH
    assert atom_hash(_OKX_ID) == _OKX_HASH
    assert atom_hash(_BINANCE_ID) == _BINANCE_HASH


def test_atom_subject_is_md_atom_of_the_venue_and_the_hash() -> None:
    for atom_id in (_DERIBIT_ID, _OKX_ID, _BINANCE_ID):
        atom = Atom.parse(atom_id)
        assert Topics.atom_subject(atom_id) == Topics.md_atom(
            atom.venue, atom_hash(atom_id)
        )


def test_a_fresh_table_from_the_same_intents_names_the_same_subjects() -> None:
    intents = {
        "ticker.Deribit_Option_BTCUSD": [_DERIBIT_ID],
        "ticker.Okx_Perp_BTCUSDT": [_OKX_ID],
        "book.BinanceUM_Perp_BTCUSDT": [_BINANCE_ID],
    }
    flipped = {key: intents[key] for key in reversed(list(intents))}
    before = AtomTable.from_intents(intents)
    after = AtomTable.from_intents(flipped)
    assert before.subjects() == after.subjects()
    for atom_id, subject in before.subjects().items():
        atom = Atom.parse(atom_id)
        assert subject == Topics.atom_subject(atom_id)
        assert after.get(atom_hash(atom_id)) == atom


def test_two_feeds_naming_one_atom_are_one_row() -> None:
    table = AtomTable.from_intents(
        {
            "a": [_OKX_ID],
            "b": [_OKX_ID, _DERIBIT_ID],
        }
    )
    assert len(table) == 2
    assert set(table.subjects()) == {_OKX_ID, _DERIBIT_ID}


def test_the_registry_is_the_venue_list_and_does_not_fold_case() -> None:
    assert set(load_adapters()) == set(venues.names())
    paper = adapter_for("Paper")
    assert callable(paper.atoms_for)
    with pytest.raises(UnknownAtomVenueError):
        adapter_for("paper")
    # §6.1 writes OKX. The token shape allows it; the registry does not.
    assert Atom("OKX", "public", "x").atom_id != Atom("Okx", "public", "x").atom_id
    with pytest.raises(UnknownAtomVenueError):
        adapter_for("OKX")
    with pytest.raises(UnknownAtomVenueError):
        adapter_for("Nope")
    assert "Okx" in atom_venues()

"""The same atom ids name the same subjects after a controller restart.

B7-01. The hash table is memory. A new controller, and a new connection
worker, built from the same ids rebuild it. Desired membership stays
B8-01; this does not read a database.
"""

from __future__ import annotations

import pytest
from mftik.exchange.atoms import Atom, InvalidAtomError, UnknownAtomVenueError
from mftik.protocol.topics import Topics, atom_hash
from mftik_md.conn import ConnId, ConnWorker
from mftik_md.controller import MdOrchestrator

_DERIBIT = "Deribit:public:book.BTC-PERPETUAL.none.20.100ms"
_OKX = "Okx:public:tickers:BTC-USDT-SWAP"
_BOOK = "BinanceUM:public:btcusdt@bookTicker"
_TICKER = "BinanceUM:market:btcusdt@ticker"

_INTENTS = {
    "book.Deribit_Perp_BTCUSD": [_DERIBIT],
    "ticker.Okx_Perp_BTCUSDT": [_OKX],
    "ticker.BinanceUM_Perp_BTCUSDT": [_TICKER, _BOOK],
}


def test_a_restarted_controller_names_the_same_subjects() -> None:
    flipped = {key: _INTENTS[key] for key in reversed(list(_INTENTS))}
    before = MdOrchestrator.from_intents(1, _INTENTS)
    after = MdOrchestrator.from_intents(2, flipped)
    assert before.controller_epoch == 1
    assert after.controller_epoch == 2
    assert before.atom_index.subjects() == after.atom_index.subjects()
    for atom_id, subject in before.atom_index.subjects().items():
        atom = Atom.parse(atom_id)
        assert subject == Topics.md_atom(atom.venue, atom_hash(atom_id))
        assert subject == Topics.atom_subject(atom_id)
        assert after.atom_index.get(atom_hash(atom_id)) == atom


def test_a_rejected_intent_leaves_the_previous_table() -> None:
    orchestrator = MdOrchestrator(3)
    orchestrator.load_atoms({"book": ["Paper:public:orderbook.BTCUSDT"]})
    with pytest.raises(UnknownAtomVenueError):
        orchestrator.load_atoms({"book": ["Nope:public:orderbook.BTCUSDT"]})
    with pytest.raises(InvalidAtomError):
        orchestrator.load_atoms({"book": ["Paper:Public:orderbook.BTCUSDT"]})
    assert list(orchestrator.atom_index.subjects()) == [
        "Paper:public:orderbook.BTCUSDT",
    ]


def test_a_new_connection_worker_indexes_the_same_subjects() -> None:
    atoms = (
        Atom("Paper", "public", "orderbook.BTCUSDT"),
        Atom("Paper", "public", "orderbook.ETHUSDT"),
    )
    first = ConnWorker(
        ConnId("Paper", "public", 0),
        instance="md-jp",
        incarnation=1,
    )
    second = ConnWorker(
        ConnId("Paper", "public", 0),
        instance="md-jp",
        incarnation=2,
    )
    first.remember(atoms)
    second.remember(tuple(reversed(atoms)))
    assert first.atom_index.subjects() == second.atom_index.subjects()
    for atom in atoms:
        assert first.atom_index.subjects()[atom.atom_id] == Topics.atom_subject(
            atom.atom_id
        )

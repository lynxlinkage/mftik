"""Publication stamping (B4-06). No broker: the sink records the envelope."""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Mapping
from decimal import Decimal

import pytest
from mftik.clock import FakeClock
from mftik.exchange.atoms import Atom
from mftik.exchange.models import BookLevel, OrderBook
from mftik.exchange.paper.atoms import decode
from mftik.protocol import MD_ORDERBOOK, Topics, UntypedEnvelope
from mftik_md.conn import SEQ_ORIGIN, ConnError, ConnId, ConnWorker, SeqClock
from mftik_md.conn_worker import argv_for

CONN = ConnId("Paper", "public", 0)
BTC = Atom("Paper", "public", "orderbook.BTCUSDT")
ETH = Atom("Paper", "public", "orderbook.ETHUSDT")


def _book(symbol: str) -> OrderBook:
    return OrderBook(
        universal_ticker=f"Paper_Spot_{symbol}",
        bids=[BookLevel(price=Decimal("10"), qty=Decimal("1"))],
        asks=[BookLevel(price=Decimal("11"), qty=Decimal("1"))],
        ts=1.0,
    )


def _frame(symbol: str) -> dict[str, object]:
    return _book(symbol).model_dump(mode="json")


class _Sink:
    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    async def publish(self, topic: str, envelope: object) -> None:
        self.sent.append((topic, envelope))


async def _frames() -> AsyncIterator[tuple[Atom, Mapping[str, object]]]:
    yield BTC, _frame("BTCUSDT")
    yield BTC, _frame("BTCUSDT")
    yield ETH, _frame("ETHUSDT")


def test_an_empty_atom_id_is_not_a_sequence() -> None:
    with pytest.raises(ConnError):
        SeqClock(1).next("")


async def test_run_stamps_per_atom_seq_on_the_envelope() -> None:
    """Three publishes of one atom are 1, 2 and the sibling starts at 1.

    No NATS. Time comes from a FakeClock, and it is the envelope ``ts``.
    ``source`` is the worker id. A second incarnation is a new clock.
    """
    clock = FakeClock(start=50.0)
    worker = ConnWorker(CONN, instance="md-paper", incarnation=4, clock=clock)
    sink = _Sink()
    await worker.run(_frames(), decode=decode, publisher=sink)

    assert [item[0] for item in sink.sent] == [
        Topics.atom_subject(BTC.atom_id),
        Topics.atom_subject(BTC.atom_id),
        Topics.atom_subject(ETH.atom_id),
    ]
    seqs = []
    for _topic, envelope in sink.sent:
        assert envelope.source == worker.worker_id  # type: ignore[attr-defined]
        assert envelope.type == MD_ORDERBOOK  # type: ignore[attr-defined]
        assert envelope.ts == 50.0  # type: ignore[attr-defined]
        assert envelope.pv == 2  # type: ignore[attr-defined]
        seqs.append(envelope.seq)  # type: ignore[attr-defined]
        raw = envelope.to_json()  # type: ignore[attr-defined]
        restored = UntypedEnvelope.from_json(raw)
        book = OrderBook.model_validate(restored.payload)
        assert book.venue == "Paper"
        assert restored.seq == envelope.seq  # type: ignore[attr-defined]
    assert seqs == [SEQ_ORIGIN, 2, SEQ_ORIGIN]

    restarted = ConnWorker(CONN, instance="md-paper", incarnation=5, clock=clock)
    again = restarted.publish(BTC, _book("BTCUSDT"), recv_ts=clock.now())
    assert again.seq == SEQ_ORIGIN
    assert again.owner == restarted.owner
    assert restarted.envelope_for(again).seq == SEQ_ORIGIN


def test_argv_is_the_fixed_atom_list() -> None:
    argv = argv_for(
        python=sys.executable,
        instance="md-paper",
        incarnation=2,
        conn=CONN,
        atoms=(BTC, ETH),
    )
    assert argv[:3] == (sys.executable, "-m", "mftik_md.conn_worker")
    assert "--incarnation" in argv
    assert argv[argv.index("--incarnation") + 1] == "2"
    assert argv[argv.index("--atom") + 1] == BTC.atom_id
    assert ETH.atom_id in argv

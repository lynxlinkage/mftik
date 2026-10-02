"""The connection-worker interface: identity, null data, and the contracts.

IF-10 defines ``mftik_md.conn`` and returns null data. The tests split:

* What is real now — a connection's identity, the shape of a desired set
  and an observed set, the broadcast payloads, and the fact that every
  entry that would touch a socket or a process refuses with the ticket
  number. These run and pass.
* What B4-06 and B8-03 will make true — ``xfail(strict=True)``, one per
  case the ticket's acceptance names, plus the generation rule F18 states
  and B8-03 has to satisfy. ``strict`` is the point: the implementation
  cannot land without deleting the marker.
"""

from __future__ import annotations

import pytest
from mftik.exchange.atoms import (
    TOPIC_AGG_TRADE,
    TOPIC_BEST_QUOTE,
    TOPIC_LIQUIDATION,
    TOPIC_ORDERBOOK,
    TOPIC_TICKER,
    TOPIC_TRADE,
    Atom,
)
from mftik.protocol import MdAtomState, MdWorkerState
from mftik_md.conn import (
    BROADCAST_INTERVAL_S,
    SEQ_ORIGIN,
    TAPED_TOPICS,
    Ack,
    Action,
    ActionKind,
    AtomPhase,
    AtomView,
    BookFold,
    ConnError,
    ConnId,
    ConnWorker,
    Desired,
    Generation,
    Observed,
    Owner,
    Reconciler,
    SeqClock,
    StateBroadcast,
    TapeAppend,
    accept_generation,
    fold_ack,
    reconcile,
    reset_observed,
    restart_in_place,
    taped,
)

BOOK_BTC = Atom("Deribit", "public", "book.BTC-PERPETUAL.none.20.100ms")
BOOK_ETH = Atom("Deribit", "public", "book.ETH-PERPETUAL.none.20.100ms")
TRADE_BTC = Atom("Deribit", "public", "trades.BTC-PERPETUAL.100ms")
CONN = ConnId("Deribit", "public", 0)
INSTANCE = "md-jp"


def _view(atom: Atom, *, gap: bool = False) -> AtomView:
    return AtomView(
        atom,
        AtomPhase.SUBSCRIBED,
        first_msg_at=1.0,
        last_msg_at=2.0,
        gap=gap,
    )


# --- identity and the null surface -----------------------------------------


def test_a_connection_id_is_one_subject_token() -> None:
    """``md/conn/Deribit/public/0`` is the spelling §3.1 and IF-01 use."""
    assert CONN.worker_id == "md/conn/Deribit/public/0"
    worker = ConnWorker(CONN, instance=INSTANCE, incarnation=3)
    assert worker.worker_id == CONN.worker_id
    assert worker.subject == "md.w.md-jp.md/conn/Deribit/public/0"
    assert worker.owner == Owner("md/conn/Deribit/public/0", 3)
    assert worker.desired() is None


def test_a_connection_id_rejects_a_dot_or_a_negative_index() -> None:
    """A dot would split ``md.w.*``. ``:`` would split an atom id.

    A negative index is not a socket.
    """
    with pytest.raises(ConnError):
        ConnId("Deribit", "public.main", 0)
    with pytest.raises(ConnError):
        ConnId("Binance.UM", "public", 0)
    with pytest.raises(ConnError):
        ConnId("Deribit", "public:main", 0)
    with pytest.raises(ConnError):
        ConnId("Deribit", "public", -1)


def test_a_generation_orders_lexicographically() -> None:
    """``(controller_epoch, seq)``, epoch first (F18). Not an env generation."""
    assert Generation(1, 9) < Generation(2, 0)
    assert Generation(1, 0) < Generation(1, 1)
    assert Generation(2, 0) == Generation(2, 0)
    assert not Generation(2, 0) < Generation(1, 9)


def test_a_generation_rejects_a_negative_part() -> None:
    with pytest.raises(ConnError):
        Generation(-1, 0)
    with pytest.raises(ConnError):
        Generation(0, -1)


def test_desired_is_a_set_not_a_delta() -> None:
    desired = Desired(Generation(1, 0), frozenset({BOOK_ETH, BOOK_BTC}))
    assert desired.atoms == frozenset({BOOK_BTC, BOOK_ETH})
    with pytest.raises(ConnError):
        Desired(Generation(1, 0), {BOOK_BTC})  # type: ignore[arg-type]


def test_observed_rejects_a_duplicate_and_a_gap_on_a_pending_atom() -> None:
    observed = Observed(epoch=1, atoms=(_view(BOOK_BTC),))
    assert observed.get(BOOK_BTC) is not None
    assert observed.get(BOOK_ETH) is None
    with pytest.raises(ConnError):
        Observed(epoch=1, atoms=(_view(BOOK_BTC), _view(BOOK_BTC)))
    with pytest.raises(ConnError):
        AtomView(BOOK_BTC, AtomPhase.PENDING, gap=True)


def test_a_resync_action_names_exactly_one_atom() -> None:
    assert Action(ActionKind.RESYNC, (BOOK_BTC,)).atoms == (BOOK_BTC,)
    with pytest.raises(ConnError):
        Action(ActionKind.RESYNC, (BOOK_BTC, BOOK_ETH))
    with pytest.raises(ConnError):
        Action(ActionKind.SUBSCRIBE, ())


def test_only_the_unrecoverable_trade_topics_are_taped() -> None:
    """Book and quotes are the next push; they are not a tape (F20)."""
    assert taped(TOPIC_TRADE)
    assert taped(TOPIC_AGG_TRADE)
    assert taped(TOPIC_LIQUIDATION)
    assert TAPED_TOPICS == frozenset({TOPIC_TRADE, TOPIC_AGG_TRADE, TOPIC_LIQUIDATION})
    assert not taped(TOPIC_ORDERBOOK)
    assert not taped(TOPIC_TICKER)
    assert not taped(TOPIC_BEST_QUOTE)


def test_the_broadcast_interval_is_two_seconds() -> None:
    assert BROADCAST_INTERVAL_S == 2.0


def test_the_broadcast_payloads_use_the_v2_models() -> None:
    """Building the payload is the interface. Sending it is not."""
    broadcast = StateBroadcast(instance=INSTANCE, conn=CONN, incarnation=3)
    assert broadcast.subject == "md.w.md-jp.md/conn/Deribit/public/0"
    assert broadcast.WORKER_TYPE == "md.worker.state"
    assert broadcast.ATOM_TYPE == "md.atom.state"

    worker_state = broadcast.worker_state(state="live", version=4)
    assert isinstance(worker_state, MdWorkerState)
    assert worker_state.instance == INSTANCE
    assert worker_state.worker_id == CONN.worker_id
    assert worker_state.incarnation == 3
    assert worker_state.state == "live"
    assert worker_state.version == 4

    view = _view(BOOK_BTC)
    atom_state = broadcast.atom_state(view, version=5)
    assert isinstance(atom_state, MdAtomState)
    assert atom_state.atom_id == BOOK_BTC.atom_id
    assert atom_state.state == "subscribed"
    assert atom_state.version == 5
    assert atom_state.incarnation == 3
    assert atom_state.first_msg_at == 1.0
    assert atom_state.last_msg_at == 2.0
    assert atom_state.error is None
    assert "gap" not in MdAtomState.model_fields


def test_reconcile_returns_no_actions() -> None:
    """Null data (IF 共同驗收 2). B8-03 replaces the body and deletes this."""
    desired = Desired(Generation(1, 0), frozenset({BOOK_BTC}))
    observed = Observed(epoch=1, atoms=(_view(BOOK_ETH, gap=True),))
    assert reconcile(desired, observed) == ()
    assert Reconciler.reconcile(desired, observed) == ()


def test_the_unimplemented_entries_refuse_with_the_ticket_number() -> None:
    """Every path that would do the work names IF-10 (共同驗收 2)."""
    worker = ConnWorker(CONN, instance=INSTANCE, incarnation=1)
    broadcast = StateBroadcast(instance=INSTANCE, conn=CONN, incarnation=1)
    tape = TapeAppend()
    observed = Observed(epoch=1, atoms=(_view(BOOK_BTC),))
    ack = Ack(epoch=1, atom=BOOK_ETH, phase=AtomPhase.SUBSCRIBED)
    calls = [
        lambda: fold_ack(observed, ack),
        lambda: reset_observed(observed),
        lambda: accept_generation(None, Generation(1, 0)),
        lambda: Reconciler.fold_ack(observed, ack),
        lambda: Reconciler.reset(observed),
        lambda: Reconciler.accept_generation(Generation(1, 0), Generation(1, 1)),
        lambda: SeqClock(1).next(BOOK_BTC.atom_id),
        lambda: BookFold(BOOK_BTC).apply(object()),
        lambda: BookFold(BOOK_BTC).snapshot(),
        lambda: worker.book(BOOK_BTC).apply(object()),
        lambda: tape.append(TRADE_BTC, object(), recv_ts=1.0),
        lambda: tape.note_gap(TRADE_BTC, reason="reconnect"),
        lambda: broadcast.publish(broadcast.worker_state(state="live", version=1)),
        lambda: worker.accept_desired(Desired(Generation(1, 0), frozenset({BOOK_BTC}))),
        lambda: worker.run(),
        lambda: worker.publish(BOOK_BTC, object(), recv_ts=1.0),
        lambda: restart_in_place(CONN),
    ]
    assert calls
    for call in calls:
        with pytest.raises(NotImplementedError, match="IF-10"):
            call()


def test_seq_origin_is_one() -> None:
    assert SEQ_ORIGIN == 1


# --- contracts the B tickets have to make true -----------------------------


@pytest.mark.xfail(
    strict=True, reason="B8-03 folds venue acks by connection epoch"
)
def test_an_ack_from_an_older_connection_epoch_is_discarded() -> None:
    """A late ack from the socket that died does not land (C3, F18).

    The epoch is the key, not a watermark: an ack from a newer epoch than
    the one ``observed`` is on is discarded too. Reconnect advances the
    epoch on purpose, so the old socket's replies stop matching. Atoms
    the current epoch has already acked stay acked.
    """
    observed = Observed(epoch=4, atoms=(_view(BOOK_BTC),))

    for stale_epoch in (3, 5):
        folded = fold_ack(
            observed,
            Ack(epoch=stale_epoch, atom=BOOK_ETH, phase=AtomPhase.SUBSCRIBED),
        )
        assert folded.epoch == 4
        assert folded.get(BOOK_ETH) is None
        still = folded.get(BOOK_BTC)
        assert still is not None
        assert still.phase is AtomPhase.SUBSCRIBED
        assert still.first_msg_at == 1.0
        assert still.last_msg_at == 2.0

    applied = fold_ack(
        observed,
        Ack(epoch=4, atom=BOOK_ETH, phase=AtomPhase.SUBSCRIBED),
    )
    added = applied.get(BOOK_ETH)
    assert added is not None
    assert added.phase is AtomPhase.SUBSCRIBED
    assert applied.get(BOOK_BTC) is not None
    assert applied.epoch == 4


@pytest.mark.xfail(
    strict=True, reason="B8-03 zeroes observed on reconnect and backfills"
)
def test_a_reconnect_zeroes_observed_and_the_next_diff_backfills() -> None:
    """The new socket has acked nothing, so the diff subscribes it all (C4).

    The epoch moves by one, which is what makes the dead socket's acks
    miss (C3). Nothing is unsubscribed and nothing is resynced: there is
    nothing on the new socket to take down, and a gap belonged to the
    book that was just dropped.
    """
    observed = Observed(epoch=2, atoms=(_view(BOOK_BTC), _view(BOOK_ETH, gap=True)))
    desired = Desired(Generation(1, 5), frozenset({BOOK_BTC, BOOK_ETH, TRADE_BTC}))

    reset = reset_observed(observed)

    assert reset.epoch == 3
    assert reset.atoms == ()
    actions = reconcile(desired, reset)
    subscribed = {
        atom
        for action in actions
        if action.kind is ActionKind.SUBSCRIBE
        for atom in action.atoms
    }
    assert subscribed == {BOOK_BTC, BOOK_ETH, TRADE_BTC}
    assert all(action.kind is ActionKind.SUBSCRIBE for action in actions)


@pytest.mark.xfail(strict=True, reason="B8-03 resyncs a book gap as one atom")
def test_a_book_gap_resyncs_only_that_atom() -> None:
    """A hole in one book does not touch the socket or any sibling (C5).

    The action is one resync — unsubscribe then subscribe that atom — and
    it names nothing else. An atom that left the desired set is an
    unsubscribe, not a resync, even when its book also had a hole.
    """
    observed = Observed(
        epoch=3,
        atoms=(_view(BOOK_BTC, gap=True), _view(BOOK_ETH), _view(TRADE_BTC)),
    )
    desired = Desired(
        Generation(2, 1),
        frozenset({BOOK_BTC, BOOK_ETH, TRADE_BTC}),
    )

    assert reconcile(desired, observed) == (Action(ActionKind.RESYNC, (BOOK_BTC,)),)

    # The gapped atom is no longer wanted. Take it down; leave the other.
    dropped = Desired(Generation(2, 2), frozenset({BOOK_ETH, TRADE_BTC}))
    actions = reconcile(dropped, observed)
    assert actions == (Action(ActionKind.UNSUBSCRIBE, (BOOK_BTC,)),)
    assert all(action.kind is not ActionKind.RESYNC for action in actions)


@pytest.mark.xfail(
    strict=True, reason="B4-06 stamps a per-atom seq inside one incarnation"
)
def test_seq_is_contiguous_within_one_incarnation() -> None:
    """Three publishes of one atom are ``1, 2, 3``; a sibling starts at 1 (C6).

    A new incarnation starts over at the same origin. This says nothing
    about a reconnect inside the incarnation: that is the open reading
    of F25's ``live`` clause, and the clock here is simply not replaced.
    """
    clock = SeqClock(incarnation=7)
    assert [clock.next(BOOK_BTC.atom_id) for _ in range(3)] == [1, 2, 3]
    assert clock.next(TRADE_BTC.atom_id) == SEQ_ORIGIN
    assert clock.next(BOOK_BTC.atom_id) == 4

    restarted = SeqClock(incarnation=8)
    assert restarted.next(BOOK_BTC.atom_id) == SEQ_ORIGIN
    assert restarted.incarnation == 8
    # The old incarnation's clock was not disturbed by the new one.
    assert clock.next(BOOK_BTC.atom_id) == 5


@pytest.mark.xfail(
    strict=True, reason="B8-03 accepts only a strictly newer generation"
)
def test_only_a_strictly_newer_generation_is_accepted() -> None:
    """Equal is a repeat, not a new desired (C7, F18).

    The first push has nothing to beat. After that the worker compares
    the counter and not the atom set: an older generation carrying a
    different set is still ignored, which is what keeps a stale
    controller from winning an overlap.
    """
    first = Generation(1, 0)
    assert accept_generation(None, first) is True
    assert accept_generation(first, Generation(1, 1)) is True
    assert accept_generation(first, Generation(2, 0)) is True
    assert accept_generation(Generation(1, 1), first) is False
    assert accept_generation(first, first) is False

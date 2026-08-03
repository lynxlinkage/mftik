"""The order lifecycle — which moves are legal and which sets an order is in.

The transition table is the thing every downstream consumer trusts when it
decides whether to release a reservation or refuse a cancel, so the shape of
it is pinned here rather than left implicit in the code that reads it.
"""

from __future__ import annotations

import pytest
from mft.exchange.models import (
    OPEN_STATUSES,
    PENDING_STATUSES,
    TERMINAL_STATUSES,
    WORKING_STATUSES,
    OrderStatus,
    can_transition,
    is_open,
    is_pending,
    is_terminal,
    is_working,
    next_statuses,
)

S = OrderStatus


def test_every_status_has_a_transition_entry() -> None:
    """A status missing from the table would KeyError at the worst moment."""
    for status in S:
        assert isinstance(next_statuses(status), frozenset)


@pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES))
def test_terminal_states_have_no_way_out(status: OrderStatus) -> None:
    assert next_statuses(status) == frozenset()
    assert is_terminal(status)
    assert not is_open(status)
    for other in S:
        assert not can_transition(status, other)


def test_the_sets_partition_the_enum() -> None:
    assert PENDING_STATUSES == {S.PENDING_NEW, S.PENDING_CANCEL}
    assert WORKING_STATUSES == {S.NEW, S.PARTIALLY_FILLED}
    assert TERMINAL_STATUSES == {S.FILLED, S.CANCELED, S.REJECTED}
    # Every status is either finished or holding exposure, never neither.
    assert OPEN_STATUSES | TERMINAL_STATUSES == set(S)
    assert not (OPEN_STATUSES & TERMINAL_STATUSES)
    # UNKNOWN counts as open: we cannot prove the order is gone.
    assert is_open(S.UNKNOWN)
    assert not is_working(S.UNKNOWN)
    assert not is_pending(S.UNKNOWN)


@pytest.mark.parametrize(
    ("current", "nxt"),
    [
        (S.PENDING_NEW, S.NEW),
        (S.PENDING_NEW, S.REJECTED),
        (S.PENDING_NEW, S.UNKNOWN),
        (S.NEW, S.PARTIALLY_FILLED),
        (S.NEW, S.PENDING_CANCEL),
        (S.NEW, S.REJECTED),
        (S.PARTIALLY_FILLED, S.PARTIALLY_FILLED),
        (S.PARTIALLY_FILLED, S.FILLED),
        (S.PARTIALLY_FILLED, S.PENDING_CANCEL),
        (S.PENDING_CANCEL, S.CANCELED),
        (S.PENDING_CANCEL, S.FILLED),
        (S.PENDING_CANCEL, S.NEW),
        (S.UNKNOWN, S.FILLED),
        (S.UNKNOWN, S.CANCELED),
        (S.UNKNOWN, S.REJECTED),
    ],
)
def test_documented_transitions_are_allowed(
    current: OrderStatus, nxt: OrderStatus
) -> None:
    assert can_transition(current, nxt)


@pytest.mark.parametrize(
    ("current", "nxt"),
    [
        # An order with no venue id cannot be cancelled.
        (S.PENDING_NEW, S.PENDING_CANCEL),
        (S.PENDING_NEW, S.CANCELED),
        # A venue does not reject an order it already accepted and filled.
        (S.PARTIALLY_FILLED, S.REJECTED),
        (S.PARTIALLY_FILLED, S.NEW),
        # Recovery resolves UNKNOWN; it never leaves it in flight.
        (S.UNKNOWN, S.PENDING_NEW),
        (S.UNKNOWN, S.PENDING_CANCEL),
        (S.NEW, S.PENDING_NEW),
    ],
)
def test_illegal_transitions_are_refused(
    current: OrderStatus, nxt: OrderStatus
) -> None:
    assert not can_transition(current, nxt)


def test_an_order_can_fill_without_resting_first() -> None:
    """A marketable order skips the book: PENDING_NEW straight to FILLED.

    Not in the hand-drawn lifecycle, but it is what a market order does and
    what the paper engine emits, so the table has to permit it.
    """
    assert can_transition(S.PENDING_NEW, S.FILLED)
    assert can_transition(S.PENDING_NEW, S.PARTIALLY_FILLED)
    assert can_transition(S.NEW, S.FILLED)


def test_recovery_can_find_an_unknown_order_still_working() -> None:
    """Reconnecting may show the order alive, not just finished."""
    assert can_transition(S.UNKNOWN, S.NEW)
    assert can_transition(S.UNKNOWN, S.PARTIALLY_FILLED)


def test_every_reachable_path_ends_terminal() -> None:
    """No status can strand an order: each one can reach a terminal state."""
    for start in S:
        seen: set[OrderStatus] = set()
        frontier = [start]
        while frontier:
            node = frontier.pop()
            if node in seen:
                continue
            seen.add(node)
            frontier.extend(next_statuses(node))
        assert seen & TERMINAL_STATUSES, f"{start} cannot finish"

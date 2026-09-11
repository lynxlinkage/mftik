from __future__ import annotations

from uuid import uuid4

from mftik.strategy.client_order_id import (
    EPOCH_MS,
    SLOT_MASK,
    ClientOrderIdFactory,
    format_client_order_id,
    ms_since_epoch,
    pack,
    slot_for_session,
    slot_of,
    unpack,
)


def test_pack_unpack_roundtrip() -> None:
    value = pack(42, 1_000_000, 7)
    assert unpack(value) == (42, 1_000_000, 7)
    assert unpack(str(value)) == (42, 1_000_000, 7)


def test_factory_seq_increments() -> None:
    # Fixed "now" so ts is stable across calls.
    now = (EPOCH_MS + 12_345) / 1000.0
    factory = ClientOrderIdFactory(7)
    a = factory.next(now=now)
    b = factory.next(now=now)
    assert factory.seq == 2
    assert unpack(a) == (7, 12_345, 1)
    assert unpack(b) == (7, 12_345, 2)
    assert int(a) < int(b) or unpack(a)[2] != unpack(b)[2]


def test_distinct_slots_never_collide() -> None:
    """Two sessions in lockstep must not mint the same id.

    Same strategy class, same millisecond, same seq — the slot is the only
    thing keeping them apart, since each session's counter starts at 0.
    """
    now = (EPOCH_MS + 999) / 1000.0
    a = ClientOrderIdFactory(1)
    b = ClientOrderIdFactory(2)
    ids_a = [a.next(now=now) for _ in range(5)]
    ids_b = [b.next(now=now) for _ in range(5)]

    assert not set(ids_a) & set(ids_b)
    assert all(slot_of(cid) == 1 for cid in ids_a)
    assert all(slot_of(cid) == 2 for cid in ids_b)


def test_same_slot_in_lockstep_does_collide() -> None:
    """Documents why the slot must be per-session, not per-strategy-class."""
    now = (EPOCH_MS + 999) / 1000.0
    a = ClientOrderIdFactory(1)
    b = ClientOrderIdFactory(1)
    assert a.next(now=now) == b.next(now=now)


def test_format_matches_pack() -> None:
    assert format_client_order_id(1, 99, 3) == str(pack(1, 99, 3))


def test_ms_since_epoch_positive() -> None:
    assert ms_since_epoch() >= 0


def test_the_slot_is_a_pure_function_of_the_session_id() -> None:
    """Pinned: a rebuild must land on the slot its resting orders carry, and
    it derives that from the id alone — in another process, another release."""
    assert slot_for_session("112a28a60a0240d288641807d77a2da0") == 21186
    assert slot_for_session("112a28a60a0240d288641807d77a2da0") == 21186


def test_the_slot_is_defined_for_any_session_id() -> None:
    """Not every id the platform mints is hex — the derivation is not a slice."""
    for session_id in ("", "r-2", "churn-a", uuid4().hex, "\u4e2d\u6587"):
        assert 0 <= slot_for_session(session_id) <= SLOT_MASK


def test_seq_wrap_bumps_timestamp() -> None:
    now = (EPOCH_MS + 50) / 1000.0
    factory = ClientOrderIdFactory(1)
    for _ in range(255):
        factory.next(now=now)
    # seq=256 → low bits 0 → must advance ts bucket
    cid = factory.next(now=now)
    slot, ts, seq = unpack(cid)
    assert slot == 1
    assert seq == 0  # low 8 bits of 256
    assert ts == 51

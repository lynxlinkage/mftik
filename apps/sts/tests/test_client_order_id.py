from __future__ import annotations

from mft_sts.client_order_id import (
    EPOCH_MS,
    ClientOrderIdFactory,
    format_client_order_id,
    ms_since_epoch,
    pack,
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


def test_format_matches_pack() -> None:
    assert format_client_order_id(1, 99, 3) == str(pack(1, 99, 3))


def test_ms_since_epoch_positive() -> None:
    assert ms_since_epoch() >= 0


def test_seq_wrap_bumps_timestamp() -> None:
    now = (EPOCH_MS + 50) / 1000.0
    factory = ClientOrderIdFactory(1)
    for _ in range(255):
        factory.next(now=now)
    # seq=256 → low bits 0 → must advance ts bucket
    cid = factory.next(now=now)
    sid, ts, seq = unpack(cid)
    assert sid == 1
    assert seq == 0  # low 8 bits of 256
    assert ts == 51

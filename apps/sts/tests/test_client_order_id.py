from __future__ import annotations

import pytest
from mftik.strategy.client_order_id import (
    EPOCH_S,
    SESSION_MASK,
    TS_MASK,
    VERSION,
    ClientOrderIdFactory,
    format_client_order_id,
    is_v1_session_id,
    pack,
    seconds_since_epoch,
    session_id_of,
    unpack,
    version_of,
)


def test_pack_unpack_roundtrip() -> None:
    value = pack(0xAABBCC, 1_000_000, 7)
    assert unpack(value) == (VERSION, "aabbcc", 1_000_000, 7)
    assert unpack(str(value)) == (VERSION, "aabbcc", 1_000_000, 7)
    assert session_id_of(value) == "aabbcc"
    assert version_of(value) == VERSION


def test_factory_seq_increments() -> None:
    now = float(EPOCH_S + 12_345)
    factory = ClientOrderIdFactory("000007")
    a = factory.next(now=now)
    b = factory.next(now=now)
    assert factory.seq == 2
    assert unpack(a) == (VERSION, "000007", 12_345, 1)
    assert unpack(b) == (VERSION, "000007", 12_345, 2)


def test_distinct_sessions_never_collide() -> None:
    """Two sessions in lockstep must not mint the same id.

    Same strategy class, same second, same seq — the session field is the
    only thing keeping them apart, since each session's counter starts at 0.
    """
    now = float(EPOCH_S + 999)
    a = ClientOrderIdFactory("000001")
    b = ClientOrderIdFactory("000002")
    ids_a = [a.next(now=now) for _ in range(5)]
    ids_b = [b.next(now=now) for _ in range(5)]

    assert not set(ids_a) & set(ids_b)
    assert all(session_id_of(cid) == "000001" for cid in ids_a)
    assert all(session_id_of(cid) == "000002" for cid in ids_b)


def test_same_session_in_lockstep_does_collide() -> None:
    """Documents why the packed field must be the session, not the class."""
    now = float(EPOCH_S + 999)
    a = ClientOrderIdFactory("000001")
    b = ClientOrderIdFactory("000001")
    assert a.next(now=now) == b.next(now=now)


def test_format_matches_pack() -> None:
    assert format_client_order_id("000001", 99, 3) == str(pack(1, 99, 3))


def test_seconds_since_epoch_positive() -> None:
    assert seconds_since_epoch() >= 0


def test_v1_session_id_is_six_lowercase_hex() -> None:
    assert is_v1_session_id("aabbcc")
    assert is_v1_session_id("000000")
    assert not is_v1_session_id("AABBCC")
    assert not is_v1_session_id("aabbcc0")
    assert not is_v1_session_id("+1a")
    assert not is_v1_session_id(" 1a")
    assert not is_v1_session_id("0x1a")
    assert not is_v1_session_id("cafe")  # 4 chars
    assert not is_v1_session_id("112a28a60a0240d288641807d77a2da0")


def test_factory_rejects_a_non_v1_session_id() -> None:
    with pytest.raises(ValueError, match="6 lowercase hex"):
        ClientOrderIdFactory("not-hex")


def test_unpack_rejects_ver_zero_and_eight() -> None:
    good = pack(1, 1, 1, ver=VERSION)
    assert unpack(good)[0] == VERSION
    zero = good & ((1 << 60) - 1)
    with pytest.raises(ValueError, match="ver=0"):
        unpack(zero)
    signed = (8 << 60) | (1 << 36) | (1 << 8) | 1
    with pytest.raises(ValueError, match="ver=8"):
        unpack(signed)


def test_pack_rejects_ver_outside_1_to_7() -> None:
    with pytest.raises(ValueError, match="ver=0"):
        pack(1, 1, 1, ver=0)
    with pytest.raises(ValueError, match="ver=8"):
        pack(1, 1, 1, ver=8)


def test_seq_wrap_bumps_timestamp() -> None:
    now = float(EPOCH_S + 50)
    factory = ClientOrderIdFactory("000001")
    for _ in range(255):
        factory.next(now=now)
    cid = factory.next(now=now)
    ver, session_id, ts, seq = unpack(cid)
    assert ver == VERSION
    assert session_id == "000001"
    assert seq == 0  # low 8 bits of 256
    assert ts == 51


def test_session_field_fits_24_bits() -> None:
    assert SESSION_MASK == 0xFFFFFF
    assert TS_MASK == (1 << 28) - 1

"""uint64 client_order_id packing for strategy order entry.

Layout v1 (MSB → LSB)::

    [63:60] ver      (4 bits)   — ``1``; legal range ``1..7``
    [59:36] session  (24 bits)  — ``session_id`` as ``int(..., 16)``
    [35:8]  ts_sec   (28 bits)  — seconds since 2026-01-01T00:00:00Z
    [7:0]   seq      (8 bits)   — low 8 bits of per-session counter

``ver >= 8`` would set the uint64 sign bit. Any int64 parse (a venue SDK,
a future BIGINT column) then breaks, so versions stop at 7. Version bits
are for layout changes, not an epoch counter: 28-bit seconds last until
~2034-07; v2 changes the epoch or the layout once.

The packed session field **is** the session id, not the strategy class:
the seq counter is per-session and starts at 0 in every session, so two
sessions of the same strategy class would otherwise mint identical ids
whenever they submit their n-th order in the same second (common — they
are woken by the same MD tick). The strategy class is recoverable from
the ``sts_sessions`` row. ``Strategy.owns`` decodes the field and compares
it to ``session.session_id``.

``session_id`` is six lowercase hex digits (``token_hex(3)``). See
:data:`V1_SESSION_ID`.

Wire form is the decimal string of the packed ``uint64``.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime

VERSION = 1
VER_MIN = 1
VER_MAX = 7

VER_BITS = 4
SESSION_BITS = 24
TS_BITS = 28
SEQ_BITS = 8

VER_SHIFT = SESSION_BITS + TS_BITS + SEQ_BITS  # 60
SESSION_SHIFT = TS_BITS + SEQ_BITS  # 36
TS_SHIFT = SEQ_BITS  # 8

VER_MASK = (1 << VER_BITS) - 1
SESSION_MASK = (1 << SESSION_BITS) - 1
TS_MASK = (1 << TS_BITS) - 1
SEQ_MASK = (1 << SEQ_BITS) - 1

SESSION_HEX_WIDTH = SESSION_BITS // 4  # 6

#: Six lowercase hex digits. Not :func:`int` — ``int("+1a", 16)`` and
#: ``int("0x1a", 16)`` both succeed and are not a session id.
V1_SESSION_ID = re.compile(r"^[0-9a-f]{6}$")

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
EPOCH_S = int(EPOCH.timestamp())


def is_v1_session_id(session_id: str) -> bool:
    """Whether ``session_id`` can be packed into a v1 client_order_id."""
    return V1_SESSION_ID.fullmatch(session_id) is not None


def parse_session_id(session_id: str) -> int:
    """``session_id`` as the 24-bit field, or raise."""
    if not is_v1_session_id(session_id):
        raise ValueError(f"session_id={session_id!r} is not 6 lowercase hex")
    return int(session_id, 16)


def format_session_id(session: int) -> str:
    """Zero-padded 6 hex of the packed session field."""
    return format(session & SESSION_MASK, "06x")


def seconds_since_epoch(now: float | None = None) -> int:
    """Seconds since 2026-01-01 UTC."""
    now_s = int(time.time() if now is None else now)
    delta = now_s - EPOCH_S
    if delta < 0:
        raise ValueError("client_order_id timestamp before 2026-01-01 UTC")
    if delta > TS_MASK:
        raise ValueError("client_order_id timestamp overflow (28-bit s)")
    return delta


def pack(session: int, ts_sec: int, seq: int, *, ver: int = VERSION) -> int:
    """Pack fields into a uint64 client_order_id."""
    if not VER_MIN <= ver <= VER_MAX:
        raise ValueError(f"ver={ver} out of range {VER_MIN}..{VER_MAX}")
    if not 0 <= session <= SESSION_MASK:
        raise ValueError(f"session={session} out of range 0..{SESSION_MASK}")
    if not 0 <= ts_sec <= TS_MASK:
        raise ValueError(f"ts_sec={ts_sec} out of range 0..{TS_MASK}")
    return (
        ((ver & VER_MASK) << VER_SHIFT)
        | ((session & SESSION_MASK) << SESSION_SHIFT)
        | ((ts_sec & TS_MASK) << TS_SHIFT)
        | (seq & SEQ_MASK)
    )


def unpack(client_order_id: int | str) -> tuple[int, str, int, int]:
    """Return ``(ver, session_id, ts_sec, seq)`` from a packed id."""
    value = int(client_order_id)
    if value < 0 or value > (1 << 64) - 1:
        raise ValueError(f"client_order_id={value} is not a uint64")
    ver = (value >> VER_SHIFT) & VER_MASK
    if not VER_MIN <= ver <= VER_MAX:
        raise ValueError(f"client_order_id ver={ver} is not in {VER_MIN}..{VER_MAX}")
    session = (value >> SESSION_SHIFT) & SESSION_MASK
    ts_sec = (value >> TS_SHIFT) & TS_MASK
    seq = value & SEQ_MASK
    return ver, format_session_id(session), ts_sec, seq


def version_of(client_order_id: int | str) -> int:
    """Return just the version nibble packed into ``client_order_id``."""
    return (int(client_order_id) >> VER_SHIFT) & VER_MASK


def session_id_of(client_order_id: int | str) -> str:
    """Return just the session id packed into ``client_order_id``.

    Shift and mask only — the hot path. Version is not checked here;
    :meth:`mftik.strategy.base.Strategy.owns` compares this to
    ``session.session_id`` and treats a bad version as not ours.
    """
    return format_session_id((int(client_order_id) >> SESSION_SHIFT) & SESSION_MASK)


def format_client_order_id(session_id: str, ts_sec: int, seq: int) -> str:
    """Pack and return the decimal wire form."""
    return str(pack(parse_session_id(session_id), ts_sec, seq))


class ClientOrderIdFactory:
    """Monotonic seq generator for one session.

    Ids are unique within a session; uniqueness *across* sessions comes from
    the session field itself, never from the seq (every session's counter
    starts at 0).
    """

    def __init__(self, session_id: str) -> None:
        self.session_id = format_session_id(parse_session_id(session_id))
        self._session = int(self.session_id, 16)
        self._seq = 0
        self._last_ts_sec = -1

    @property
    def seq(self) -> int:
        return self._seq

    def next(self, *, now: float | None = None) -> str:
        """Allocate the next client_order_id (seq += 1)."""
        self._seq += 1
        ts_sec = seconds_since_epoch(now)
        # Low 8 bits wrap every 256 orders — advance the second bucket so the
        # packed uint64 stays unique within a burst.
        if (self._seq & SEQ_MASK) == 0:
            ts_sec = max(ts_sec, self._last_ts_sec + 1)
        elif ts_sec < self._last_ts_sec:
            ts_sec = self._last_ts_sec
        if ts_sec > TS_MASK:
            raise ValueError("client_order_id timestamp overflow (28-bit s)")
        self._last_ts_sec = ts_sec
        return str(pack(self._session, ts_sec, self._seq))

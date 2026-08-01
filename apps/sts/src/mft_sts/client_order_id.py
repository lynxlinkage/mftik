"""uint64 client_order_id packing for strategy order entry.

Layout (MSB → LSB)::

    [63:48] strategy_id  (16 bits)  — ``Strategy.id``
    [47:8]  ts_ms        (40 bits)  — ms since 2026-01-01T00:00:00Z
    [7:0]   seq          (8 bits)   — low 8 bits of per-strategy counter

Wire form is the decimal string of the packed ``uint64``.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

STRATEGY_BITS = 16
TS_BITS = 40
SEQ_BITS = 8

STRATEGY_SHIFT = TS_BITS + SEQ_BITS  # 48
TS_SHIFT = SEQ_BITS  # 8

STRATEGY_MASK = (1 << STRATEGY_BITS) - 1
TS_MASK = (1 << TS_BITS) - 1
SEQ_MASK = (1 << SEQ_BITS) - 1

EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)
EPOCH_MS = int(EPOCH.timestamp() * 1000)


def ms_since_epoch(now: float | None = None) -> int:
    """Milliseconds since 2026-01-01 UTC."""
    if now is None:
        now_ms = int(time.time() * 1000)
    else:
        now_ms = int(now * 1000)
    delta = now_ms - EPOCH_MS
    if delta < 0:
        raise ValueError("client_order_id timestamp before 2026-01-01 UTC")
    if delta > TS_MASK:
        raise ValueError("client_order_id timestamp overflow (40-bit ms)")
    return delta


def pack(strategy_id: int, ts_ms: int, seq: int) -> int:
    """Pack fields into a uint64 client_order_id."""
    if not 0 <= strategy_id <= STRATEGY_MASK:
        raise ValueError(
            f"strategy_id={strategy_id} out of range 0..{STRATEGY_MASK}"
        )
    if not 0 <= ts_ms <= TS_MASK:
        raise ValueError(f"ts_ms={ts_ms} out of range 0..{TS_MASK}")
    return (
        ((strategy_id & STRATEGY_MASK) << STRATEGY_SHIFT)
        | ((ts_ms & TS_MASK) << TS_SHIFT)
        | (seq & SEQ_MASK)
    )


def unpack(client_order_id: int | str) -> tuple[int, int, int]:
    """Return ``(strategy_id, ts_ms, seq)`` from a packed id."""
    value = int(client_order_id)
    if value < 0 or value > (1 << 64) - 1:
        raise ValueError(f"client_order_id={value} is not a uint64")
    strategy_id = (value >> STRATEGY_SHIFT) & STRATEGY_MASK
    ts_ms = (value >> TS_SHIFT) & TS_MASK
    seq = value & SEQ_MASK
    return strategy_id, ts_ms, seq


def format_client_order_id(strategy_id: int, ts_ms: int, seq: int) -> str:
    """Pack and return the decimal wire form."""
    return str(pack(strategy_id, ts_ms, seq))


class ClientOrderIdFactory:
    """Monotonic seq generator for one strategy instance."""

    def __init__(self, strategy_id: int) -> None:
        if not 0 <= strategy_id <= STRATEGY_MASK:
            raise ValueError(
                f"strategy_id={strategy_id} out of range 0..{STRATEGY_MASK}"
            )
        self.strategy_id = strategy_id
        self._seq = 0
        self._last_ts_ms = -1

    @property
    def seq(self) -> int:
        return self._seq

    def next(self, *, now: float | None = None) -> str:
        """Allocate the next client_order_id (seq += 1)."""
        self._seq += 1
        ts_ms = ms_since_epoch(now)
        # Low 8 bits wrap every 256 orders — advance the ms bucket so the
        # packed uint64 stays unique within a burst.
        if (self._seq & SEQ_MASK) == 0:
            ts_ms = max(ts_ms, self._last_ts_ms + 1)
        elif ts_ms < self._last_ts_ms:
            ts_ms = self._last_ts_ms
        if ts_ms > TS_MASK:
            raise ValueError("client_order_id timestamp overflow (40-bit ms)")
        self._last_ts_ms = ts_ms
        return format_client_order_id(self.strategy_id, ts_ms, self._seq)

"""``seq``, ``recv_ts`` and ``age`` beside a payload, not on the wire.

The hook already receives the platform model (a :class:`~mftik.exchange.models.Ticker`,
an :class:`~mftik.exchange.models.Order`, and the rest). Those models are
what MD publishes and what TD fans out, so a field added to them would
leave the process on the next publish and would change equality.

``seq`` is the MD connection worker's per-atom sequence, copied off the
envelope (F25). ``recv_ts`` is when the STS ingress received the frame.
``age`` is ``clock() - recv_ts``, read when the strategy asks, which is
on the strategy thread. Nothing is bound until the session worker stamps
the instance it is about to hand to the hook. Until then each one is
``None`` — unknown, not zero.

The stamp is keyed by :func:`id`, not by the model's equality. Two
payloads that compare equal are still two frames, and a hole in ``seq``
is per frame (F25). A :func:`weakref.finalize` drops the entry once
nothing else holds the model. The callback removes the entry only when
it is still this stamp, so a recycled ``id`` cannot erase a newer one.
"""

from __future__ import annotations

import weakref
from collections.abc import Callable
from typing import Any


class _Stamp:
    __slots__ = ("clock", "key", "recv_ts", "seq")

    def __init__(
        self,
        seq: int | None,
        recv_ts: float | None,
        clock: Callable[[], float] | None,
    ) -> None:
        self.seq = seq
        self.recv_ts = recv_ts
        self.clock = clock
        self.key = 0

    def discard(self, _ref: object) -> None:
        if _STAMPS.get(self.key) is self:
            _STAMPS.pop(self.key, None)


_STAMPS: dict[int, _Stamp] = {}


def bind_delivery(
    event: Any,
    *,
    seq: int | None,
    recv_ts: float | None,
    clock: Callable[[], float] | None,
) -> None:
    """Remember ``seq``, ``recv_ts`` and the clock for this instance.

    ``event`` has to be weak-referenceable, which a pydantic model is.
    A second call on the same instance replaces the stamp. Another
    instance that compares equal does not share it. The model itself
    is unchanged.
    """
    stamp = _Stamp(seq, recv_ts, clock)
    stamp.key = id(event)
    _STAMPS[stamp.key] = stamp
    weakref.finalize(event, stamp.discard, None)


def _lookup(event: object) -> _Stamp | None:
    return _STAMPS.get(id(event))


class HasDelivery:
    """``seq``, ``recv_ts`` and ``age`` for one hooked payload.

    Mixed into the models a strategy hook receives. Absent a stamp, every
    answer is ``None``.
    """

    @property
    def seq(self) -> int | None:
        """MD per-atom sequence, or ``None`` when the frame had none (F25)."""
        stamp = _lookup(self)
        if stamp is None:
            return None
        return stamp.seq

    @property
    def recv_ts(self) -> float | None:
        """When the ingress received this frame, or ``None`` if it was not stamped."""
        stamp = _lookup(self)
        if stamp is None:
            return None
        return stamp.recv_ts

    @property
    def age(self) -> float | None:
        """Seconds since :attr:`recv_ts`, or ``None`` if either is unknown.

        Read on the strategy thread: the clock is the ingress clock, and
        the subtraction happens when the strategy asks.
        """
        stamp = _lookup(self)
        if stamp is None or stamp.clock is None or stamp.recv_ts is None:
            return None
        return stamp.clock() - stamp.recv_ts

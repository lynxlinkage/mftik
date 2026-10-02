"""Outstanding order requests, shared by the two threads (§5.3).

The strategy thread registers a future and publishes. The ingress thread
completes it, or expires it, from the clock it was given. The timeout is
that comparison. It is not an ``asyncio`` timer on the strategy loop:
uvloop runs an expired timer before it reads the socket, so a hook that
held the loop would time the ack out after the reply had already arrived.
"""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass

from mftik.broker.errors import RequestTimeoutError


@dataclass
class _Slot:
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[str]
    deadline: float
    subject: str
    request_id: str
    timeout: float


class PendingTable:
    """Request id → the strategy-loop future waiting for that reply.

    The id is the outbound envelope's id, and the inbox subject carries
    it. The reply envelope has its own id; nothing here matches on that.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._slots: dict[str, _Slot] = {}

    def register(
        self,
        request_id: str,
        *,
        loop: asyncio.AbstractEventLoop,
        future: asyncio.Future[str],
        deadline: float,
        subject: str,
        timeout: float,
    ) -> None:
        """Record ``future`` before the publish that names ``request_id``."""
        slot = _Slot(
            loop=loop,
            future=future,
            deadline=deadline,
            subject=subject,
            request_id=request_id,
            timeout=timeout,
        )
        with self._lock:
            self._slots[request_id] = slot

    def complete(self, request_id: str, raw: str, *, now: float) -> bool:
        """Hand ``raw`` back, or time it out if ``now`` is past the deadline.

        ``False`` means the id was not waiting (already expired, or never
        registered). A reply that arrives after the deadline is a timeout,
        not a late success: the strategy has already been told there was
        no ack.
        """
        with self._lock:
            slot = self._slots.pop(request_id, None)
        if slot is None:
            return False
        if now > slot.deadline:
            _resolve(
                slot,
                RequestTimeoutError(slot.subject, slot.request_id, slot.timeout),
            )
        else:
            _resolve(slot, result=raw)
        return True

    def expire(self, now: float) -> None:
        """Fail every request whose deadline ``now`` has passed."""
        late: list[_Slot] = []
        with self._lock:
            for key, slot in list(self._slots.items()):
                if now > slot.deadline:
                    late.append(slot)
                    del self._slots[key]
        for slot in late:
            _resolve(
                slot,
                RequestTimeoutError(slot.subject, slot.request_id, slot.timeout),
            )

    def cancel(self, request_id: str, exc: BaseException) -> None:
        """Drop one id. The publish that was about to use it did not happen."""
        with self._lock:
            slot = self._slots.pop(request_id, None)
        if slot is not None:
            _resolve(slot, exc)

    def cancel_all(self, exc: BaseException) -> None:
        """Fail everything still waiting. The ingress is going away."""
        with self._lock:
            slots = list(self._slots.values())
            self._slots.clear()
        for slot in slots:
            _resolve(slot, exc)


def _resolve(
    slot: _Slot,
    exc: BaseException | None = None,
    result: str | None = None,
) -> None:
    def _set() -> None:
        if slot.future.done():
            return
        if exc is not None:
            slot.future.set_exception(exc)
        else:
            slot.future.set_result(result if result is not None else "")

    try:
        slot.loop.call_soon_threadsafe(_set)
    except RuntimeError:
        # The strategy loop has already closed. Nothing is awaiting this.
        return

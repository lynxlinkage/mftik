"""Per-socket ledger of reserved and acked wire identities.

MD refcounts product feeds. This is the other ledger: whether this socket
has already sent ``SUBSCRIBE`` for a given opaque key. The key is whatever
the socket uses — a Binance stream name, a Bybit topic, an OKX
``arg_key`` tuple, a Gate ``(channel, payload)``.

Reservation happens *before* the venue ack, so two concurrent
:meth:`WireLedger.acquire` calls for the same key send one frame. A
failed ack rolls the reservation back so a later caller retries.
Consumer liveness is not counted here; callers derive that by scanning
their ``_subs``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Hashable, Iterable, Sequence
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)


def first_seen[T: Hashable](keys: Iterable[T]) -> list[T]:
    """Each key once, in the order it first appeared.

    Reconnect restore sends this list, not a flattened ``_Sub`` walk —
    two consumers of one identity must not produce two ``SUBSCRIBE`` args.
    """
    seen: dict[K, None] = {}
    for key in keys:
        seen.setdefault(key, None)
    return list(seen)


class WireLedger(Generic[K]):
    """Reserved and acked identities for one socket."""

    def __init__(self) -> None:
        self._held: set[K] = set()
        self._inflight: dict[K, asyncio.Future[None]] = {}
        self._lock = asyncio.Lock()

    def held(self) -> frozenset[K]:
        return frozenset(self._held)

    def clear(self) -> None:
        """Forget every reservation. Call before a restore request.

        A fresh socket is subscribed to nothing. Clearing first means a
        failed restore leaves the set empty, so the next ``subscribe_*``
        re-sends instead of treating the name as live.
        """
        self._held.clear()

    def discard(self, keys: Iterable[K]) -> None:
        """Drop keys after an explicit venue unsubscribe.

        Not used by book-gap resync: that round trip is not a ledger
        open or close, and must not mark the identity free for a
        co-reader.
        """
        for key in keys:
            self._held.discard(key)

    async def acquire(
        self,
        keys: Sequence[K],
        send: Callable[[Sequence[K]], Awaitable[None]],
    ) -> None:
        """Ensure ``keys`` are subscribed.

        ``send`` is called with the identities this caller is responsible
        for — not already held, not already being sent by someone else.
        Concurrent callers of a key already in flight wait for that
        send. A failed send fails those waiters and forgets the
        reservation so a later ``acquire`` retries.
        """
        keys = first_seen(keys)
        if not keys:
            return

        waiters: list[asyncio.Future[None]] = []
        to_send: list[K] = []
        async with self._lock:
            for key in keys:
                if key in self._held:
                    continue
                existing = self._inflight.get(key)
                if existing is not None:
                    waiters.append(existing)
                    continue
                fut = asyncio.get_running_loop().create_future()
                self._inflight[key] = fut
                to_send.append(key)

        if to_send:
            try:
                await send(to_send)
            except BaseException as exc:
                async with self._lock:
                    for key in to_send:
                        fut = self._inflight.pop(key, None)
                        if fut is not None and not fut.done():
                            fut.set_exception(exc)
                raise
            async with self._lock:
                self._held.update(to_send)
                for key in to_send:
                    fut = self._inflight.pop(key, None)
                    if fut is not None and not fut.done():
                        fut.set_result(None)

        if waiters:
            await asyncio.gather(*waiters)


__all__ = ["WireLedger", "first_seen"]

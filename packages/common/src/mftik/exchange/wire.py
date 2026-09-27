"""Per-socket ledger of reserved and acked wire identities.

MD refcounts product feeds. This is the other ledger: whether this socket
has already sent ``SUBSCRIBE`` for a given opaque key. The key is whatever
the socket uses — a Binance stream name, a Bybit topic, an OKX
``arg_key`` tuple, a Gate ``(channel, payload)``.

Reservation happens *before* the venue ack, so two concurrent
:meth:`WireLedger.acquire` calls for the same key send one frame. A
failed ack rolls the reservation back so a later caller retries.
Consumer liveness is not counted here; callers derive that by scanning
their ``_subs``. The last reader unsubscribes through :meth:`WireLedger.release`,
which shares this lock with ``acquire``.
"""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Awaitable, Callable, Hashable, Iterable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Generic, TypeVar

logger = logging.getLogger(__name__)

K = TypeVar("K", bound=Hashable)

#: How long a public socket waits after the last reader leaves before it
#: unsubscribes. A reattach inside the window — a lease renewing, a strategy
#: restarting — finds the key still held and sends nothing. Tests inject a
#: shorter value.
RELEASE_LINGER = 2.0

#: How many times a book resync retries ``SUBSCRIBE`` after the unsubscribe
#: has landed, before it gives up and reconnects the socket.
RESYNC_SUBSCRIBE_ATTEMPTS = 3


class ReleaseOutcome(enum.Enum):
    """What one identity's ``UNSUBSCRIBE`` did to the venue.

    ``ACKED`` drops the key. ``REJECTED`` is an explicit venue error, so the
    socket is still carrying the identity and the key goes back to held.
    ``UNKNOWN`` is a timeout or a lost connection: the venue may already
    have dropped the channel, so the key stays free and the next
    ``acquire`` subscribes again.
    """

    ACKED = "acked"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class ResyncResult(enum.Enum):
    """How a forced book resubscribe ended."""

    #: The subscribe landed, or nobody wanted the key so nothing was sent.
    DONE = "done"
    #: The unsubscribe was explicitly rejected. The venue still has the channel.
    STILL_HELD = "still_held"
    #: The re-subscribe did not land. The key was discarded and the connection dropped.
    DROPPED = "dropped"


def classify_release(exc: BaseException) -> ReleaseOutcome:
    """Sort a failed unsubscribe into rejected or unknown.

    A venue error reply means the socket is still carrying the identity.
    A timeout or a dropped connection does not: the channel may already
    be gone, and treating it as held would leave the next reader silent.
    ``CancelledError`` is not a venue answer; callers re-raise it themselves.
    """
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return ReleaseOutcome.UNKNOWN
    if type(exc).__module__.startswith("websockets") and type(exc).__name__.startswith(
        "Connection"
    ):
        return ReleaseOutcome.UNKNOWN
    text = str(exc).lower()
    if "no reply" in text or "no ack" in text or "timed out" in text:
        return ReleaseOutcome.UNKNOWN
    return ReleaseOutcome.REJECTED


def map_release[T: Hashable](
    keys: Sequence[T], exc: BaseException | None
) -> dict[T, ReleaseOutcome]:
    """One outcome for every key in a frame that succeeded or failed together."""
    if exc is None:
        return {key: ReleaseOutcome.ACKED for key in keys}
    outcome = classify_release(exc)
    return {key: outcome for key in keys}


def raise_for_release(
    outcomes: Mapping[Hashable, ReleaseOutcome],
    errors: Sequence[BaseException] = (),
) -> None:
    """Raise when an explicit ``unsubscribe()`` did not fully land.

    The flusher does not call this. It logs and leaves the ledger as
    ``release`` already wrote it. A caller that asked to unsubscribe
    still sees the venue's error.
    """
    if not outcomes or all(
        outcome is ReleaseOutcome.ACKED for outcome in outcomes.values()
    ):
        return
    if errors:
        raise errors[0]
    failed = [
        key for key, outcome in outcomes.items() if outcome is not ReleaseOutcome.ACKED
    ]
    raise ConnectionError(f"unsubscribe failed for {failed}")


def assert_last_reader[T: Hashable](
    held_by: dict[T, Sequence[Iterable[T]]],
) -> None:
    """Raise unless every key is held by at most one fully-covered reader.

    ``held_by`` maps each identity being unsubscribed to the claim-sets of
    the ``_Sub`` s that hold it. A claim-set that is not a subset of the
    keys is a wider subscription — half-unsubscribing it would leave a
    stream silently missing a contract. More than one holder is a
    co-reader. Either way this is not a last-reader close, so raise
    rather than no-op: a silent success is the failure mode this ledger
    exists to argue against.
    """
    wanted = frozenset(held_by)
    for key, claims in held_by.items():
        if any(not frozenset(claim) <= wanted for claim in claims):
            raise ValueError(f"unsubscribe {key!r} is claimed by a wider subscription")
        if len(claims) > 1:
            raise ValueError(f"unsubscribe {key!r} still has {len(claims)} readers")


def first_seen[T: Hashable](keys: Iterable[T]) -> list[T]:
    """Each key once, in the order it first appeared.

    Reconnect restore sends this list, not a flattened ``_Sub`` walk —
    two consumers of one identity must not produce two ``SUBSCRIBE`` args.
    """
    seen: dict[K, None] = {}
    for key in keys:
        seen.setdefault(key, None)
    return list(seen)


def orphaned_keys[T: Hashable](
    closed: Iterable[T], live: Iterable[Iterable[T]]
) -> list[T]:
    """Keys from ``closed`` that no remaining subscription still holds."""
    held: set[T] = set()
    for group in live:
        held.update(group)
    return [key for key in first_seen(closed) if key not in held]


class WireLedger(Generic[K]):
    """Reserved and acked identities for one socket."""

    def __init__(self) -> None:
        self._held: set[K] = set()
        self._inflight: dict[K, asyncio.Future[None]] = {}
        self._releasing: dict[K, asyncio.Future[ReleaseOutcome]] = {}
        self._cycling: dict[K, asyncio.Future[None]] = {}
        self._lock = asyncio.Lock()
        #: Bumped by :meth:`clear`. A leader that acks after a clear
        #: must not write its keys back onto ``_held``.
        self._generation = 0

    def held(self) -> frozenset[K]:
        return frozenset(self._held)

    def clear(self) -> None:
        """Forget every reservation. Call before a restore request.

        A fresh socket is subscribed to nothing. Clearing first means a
        failed restore leaves the set empty, so the next ``subscribe_*``
        re-sends instead of treating the name as live.

        In-flight subscribes, in-flight unsubscribes and resync cycles
        all go. Waiters of a dead leader are failed immediately rather
        than at ``ack_timeout``. Synchronous because ``_teardown`` is a
        plain ``def``; ``_restore`` is async and could await, but the
        sync caller decides the signature.
        """
        self._held.clear()
        self._generation += 1
        self._fail(self._inflight)
        self._inflight = {}
        self._fail(self._releasing)
        self._releasing = {}
        cycling = self._cycling
        self._cycling = {}
        for fut in cycling.values():
            if not fut.done():
                fut.set_exception(ConnectionError("socket cleared"))
                fut.exception()

    @staticmethod
    def _fail(inflight: Mapping[K, asyncio.Future[object]]) -> None:
        for fut in inflight.values():
            if not fut.done():
                fut.set_exception(ConnectionError("socket cleared"))
                fut.exception()

    def discard(self, keys: Iterable[K]) -> None:
        """Forget keys the venue is no longer sending.

        A failed book resync uses this when the re-subscribe did not
        land, so the next ``acquire`` sends again. A successful resync
        does not discard: the identity stays held across the direct
        unsubscribe and subscribe. Explicit ``unsubscribe()`` goes
        through :meth:`release`, not here.
        """
        for key in keys:
            self._held.discard(key)

    def _reserve_locked(
        self, keys: Sequence[K]
    ) -> tuple[list[asyncio.Future[None]], list[K]]:
        """Claim keys to subscribe. Caller holds the lock."""
        waiters: list[asyncio.Future[None]] = []
        to_send: list[K] = []
        for key in keys:
            if key in self._held:
                continue
            existing = self._inflight.get(key)
            if existing is not None:
                waiters.append(existing)
                continue
            fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._inflight[key] = fut
            to_send.append(key)
        return waiters, to_send

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

        A key that is being unsubscribed blocks the whole call. Nothing
        is reserved until that release finishes, and the call starts
        over: reserving a sibling first could deadlock against the
        release, and a key the release dropped has to be subscribed
        again rather than treated as held.
        """
        keys = first_seen(keys)
        if not keys:
            return

        while True:
            async with self._lock:
                blocking = [
                    self._releasing[key] for key in keys if key in self._releasing
                ]
                if not blocking:
                    generation = self._generation
                    waiters, to_send = self._reserve_locked(keys)
                    break
            await asyncio.gather(*blocking)

        if to_send:
            try:
                await send(to_send)
            except BaseException as exc:
                async with self._lock:
                    if generation == self._generation:
                        for key in to_send:
                            fut = self._inflight.pop(key, None)
                            if fut is not None and not fut.done():
                                fut.set_exception(exc)
                                fut.exception()
                    # else: clear() already failed the old waiters and
                    # swapped the dict. Do not pop the new leader's future.
                raise
            async with self._lock:
                if generation == self._generation:
                    self._held.update(to_send)
                    for key in to_send:
                        fut = self._inflight.pop(key, None)
                        if fut is not None and not fut.done():
                            fut.set_result(None)
                # else: clear() ran. The frame went to a dead socket;
                # do not mark the keys held, and the waiters already
                # failed when the dict was swapped out.

        if waiters:
            await asyncio.gather(*waiters)

    async def _wait_cycles(self, keys: Sequence[K]) -> None:
        """Block until none of ``keys`` is inside a book resync.

        The resync sends ``UNSUBSCRIBE`` then ``SUBSCRIBE`` itself.
        Releasing in between would drop the channel the snapshot is
        about to restore.
        """
        while True:
            async with self._lock:
                pending = [self._cycling[key] for key in keys if key in self._cycling]
            if not pending:
                return
            await asyncio.gather(*pending)

    def _finish_release(
        self,
        keys: Sequence[K],
        report: Mapping[K, ReleaseOutcome],
        generation: int,
    ) -> None:
        """Apply per-key outcomes. Caller holds the lock.

        A ``clear`` since the send started owns the futures already;
        writing here would mark a fresh socket with a dead ack.
        """
        if generation != self._generation:
            return
        for key in keys:
            outcome = report.get(key, ReleaseOutcome.UNKNOWN)
            if outcome is ReleaseOutcome.REJECTED:
                self._held.add(key)
            fut = self._releasing.pop(key, None)
            if fut is not None and not fut.done():
                fut.set_result(outcome)

    async def release(
        self,
        keys: Sequence[K],
        send: Callable[[Sequence[K]], Awaitable[Mapping[K, ReleaseOutcome]]],
        still_wanted: Callable[[K], bool],
    ) -> dict[K, ReleaseOutcome]:
        """Unsubscribe keys nobody reads.

        ``still_wanted`` is called under the lock, immediately before a
        key is taken, and must not await or touch this ledger. A key a
        reader reclaimed during the linger is left held and no frame
        goes out for it.

        ``send`` reports one outcome per key it attempted. A raise
        before that report means the batch is unknown: those keys are
        not held, and the exception propagates. A second call for a key
        already releasing awaits that attempt instead of sending again.
        """
        keys = first_seen(keys)
        results: dict[K, ReleaseOutcome] = {}
        if not keys:
            return results

        await self._wait_cycles(keys)

        joined: dict[K, asyncio.Future[ReleaseOutcome]] = {}
        to_send: list[K] = []
        async with self._lock:
            generation = self._generation
            for key in keys:
                existing = self._releasing.get(key)
                if existing is not None:
                    joined[key] = existing
                    continue
                if (
                    key not in self._held
                    or key in self._cycling
                    or key in self._inflight
                ):
                    if key not in self._held:
                        results[key] = ReleaseOutcome.ACKED
                    continue
                if still_wanted(key):
                    continue
                fut: asyncio.Future[ReleaseOutcome] = (
                    asyncio.get_running_loop().create_future()
                )
                self._releasing[key] = fut
                self._held.discard(key)
                to_send.append(key)

        if to_send:
            try:
                report = await send(to_send)
            except asyncio.CancelledError:
                async with self._lock:
                    self._finish_release(
                        to_send,
                        {key: ReleaseOutcome.UNKNOWN for key in to_send},
                        generation,
                    )
                raise
            except Exception:
                async with self._lock:
                    self._finish_release(
                        to_send,
                        {key: ReleaseOutcome.UNKNOWN for key in to_send},
                        generation,
                    )
                raise
            async with self._lock:
                if generation == self._generation:
                    for key in to_send:
                        results[key] = report.get(key, ReleaseOutcome.UNKNOWN)
                self._finish_release(to_send, report, generation)

        for key, fut in joined.items():
            results[key] = await fut
        return results

    @asynccontextmanager
    async def cycling(self, key: K):
        """Hold ``key`` so :meth:`release` will not unsubscribe it.

        Book resync speaks ``UNSUBSCRIBE`` then ``SUBSCRIBE`` on the
        wire itself. A release between those two frames would drop the
        channel the snapshot is meant to restore. Waits out an
        unsubscribe already in flight, and does not touch ``_held``.
        """
        while True:
            async with self._lock:
                releasing = self._releasing.get(key)
                if releasing is None and key not in self._cycling:
                    fut: asyncio.Future[None] = (
                        asyncio.get_running_loop().create_future()
                    )
                    self._cycling[key] = fut
                    break
            if releasing is not None:
                await releasing
        try:
            yield
        finally:
            async with self._lock:
                current = self._cycling.get(key)
                if current is fut:
                    self._cycling.pop(key, None)
                if not fut.done():
                    fut.set_result(None)


async def resync_channel[T: Hashable](
    ledger: WireLedger[T],
    key: T,
    *,
    still_wanted: Callable[[T], bool],
    unsubscribe: Callable[[], Awaitable[None]],
    subscribe: Callable[[], Awaitable[None]],
    drop_connection: Callable[[], Awaitable[None]],
    attempts: int = RESYNC_SUBSCRIBE_ATTEMPTS,
) -> ResyncResult:
    """End and restart one identity so the venue sends a fresh snapshot.

    On success the key stays held — routing the subscribe through
    ``acquire`` would no-op, and discarding it would let the next
    caller double-subscribe. If the unsubscribe is explicitly rejected
    the venue still has the channel, so the key stays held and the
    socket stays up. If the re-subscribe does not land, the key is
    discarded and the connection is dropped without shutting the socket
    down; the read loop reconnects and ``_restore`` resubscribes every
    reader still in ``_subs``.
    """
    if not still_wanted(key):
        return ResyncResult.DONE
    async with ledger.cycling(key):
        if not still_wanted(key):
            return ResyncResult.DONE
        try:
            await unsubscribe()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if classify_release(exc) is ReleaseOutcome.REJECTED:
                logger.warning("book resync unsubscribe rejected for %r", key)
                return ResyncResult.STILL_HELD
            logger.exception("book resync unsubscribe failed for %r", key)
            ledger.discard([key])
            await drop_connection()
            return ResyncResult.DROPPED
        if not still_wanted(key):
            ledger.discard([key])
            return ResyncResult.DROPPED
        last: BaseException | None = None
        for _ in range(attempts):
            if not still_wanted(key):
                ledger.discard([key])
                return ResyncResult.DROPPED
            try:
                await subscribe()
                return ResyncResult.DONE
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last = exc
                logger.warning("book resync subscribe failed for %r: %s", key, exc)
        logger.error(
            "book resync gave up on %r after %s attempts: %s", key, attempts, last
        )
        ledger.discard([key])
        await drop_connection()
        return ResyncResult.DROPPED


class IdleReleaser(Generic[K]):
    """One linger, then one ``release``, for idle keys on a single socket.

    ``_drop`` only enqueues. A burst of closes during the linger drains
    as one batch, and a reader that comes back before the sleep ends is
    still wanted when ``release`` checks, so no frame goes out. The task
    is stored here; :meth:`cancel` from teardown drops it.
    """

    def __init__(
        self,
        ledger: WireLedger[K],
        send: Callable[[Sequence[K]], Awaitable[Mapping[K, ReleaseOutcome]]],
        still_wanted: Callable[[K], bool],
        *,
        linger: float = RELEASE_LINGER,
    ) -> None:
        self._ledger = ledger
        self._send = send
        self._still_wanted = still_wanted
        self.linger = linger
        self._pending: set[K] = set()
        self._task: asyncio.Task[None] | None = None

    def enqueue(self, keys: Iterable[K]) -> None:
        added = False
        for key in keys:
            if key not in self._pending:
                self._pending.add(key)
                added = True
        if added:
            self._arm()

    def claim(self, keys: Iterable[K]) -> None:
        """Pull keys out of the linger so an explicit unsubscribe can send now."""
        for key in keys:
            self._pending.discard(key)

    def cancel(self) -> None:
        """Drop a queued release. Safe to call from synchronous teardown."""
        self._pending.clear()
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()

    async def drained(self) -> None:
        """Wait until the current flush, and any it re-arms, has finished."""
        while self._task is not None:
            task = self._task
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if self._task is task:
                    raise
            if self._task is task and task.done():
                self._task = None

    def _arm(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="wire-release")

    async def _run(self) -> None:
        try:
            await asyncio.sleep(self.linger)
            keys = list(self._pending)
            self._pending.clear()
            if not keys:
                return
            try:
                await self._ledger.release(keys, self._send, self._still_wanted)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("idle wire release failed")
        except asyncio.CancelledError:
            raise
        finally:
            if self._task is asyncio.current_task():
                self._task = None
            if self._pending and self._task is None:
                self._arm()


__all__ = [
    "RELEASE_LINGER",
    "RESYNC_SUBSCRIBE_ATTEMPTS",
    "IdleReleaser",
    "ReleaseOutcome",
    "ResyncResult",
    "WireLedger",
    "assert_last_reader",
    "classify_release",
    "first_seen",
    "map_release",
    "orphaned_keys",
    "raise_for_release",
    "resync_channel",
]

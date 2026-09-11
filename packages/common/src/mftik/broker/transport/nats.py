"""The NATS transport — core subjects only.

The hot path never waits for a stream leader. Live fan-out is
``nc.publish`` / ``nc.subscribe``. Request-reply is core NATS: nobody
serving is an immediate error. Session fencing is a heartbeat on those
same subjects, not a KV TTL.

There is no JetStream context. ``connect()`` opens a connection and
does not ensure streams or buckets. Tape lives on regional Redis;
ledger and OMS live in TD memory; session logs live in Postgres.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import AsyncIterator, Sequence
from typing import Any

import nats
import nats.errors
from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg

from mftik.broker.config import BrokerConfig
from mftik.broker.errors import BrokerNotConnectedError, RequestTimeoutError
from mftik.broker.transport.base import BrokerTransport, redacted_url

logger = logging.getLogger(__name__)

#: How long to wait before re-asking a subject that reported no responders.
_NO_RESPONDERS_GRACE_S = 0.05

#: How much of a caller's own timeout may go on re-asking, and the bounds on
#: that. A share rather than a count of attempts, because what "no responders"
#: is worth waiting through depends entirely on who is asking.
_NO_RESPONDERS_SHARE = 0.5
_NO_RESPONDERS_FLOOR_S = 0.1
_NO_RESPONDERS_CEILING_S = 1.0

#: How long :meth:`NatsTransport.close` waits for the outbound buffer.
_CLOSE_FLUSH_TIMEOUT_S = 2.0

#: What one subject token may not contain. ``.`` is absent on purpose: the
#: broker's topics are already dotted and those dots are meant as hierarchy.
_SUBJECT_BAD = re.compile(r"[\s*>]")


def _sanitize(name: str) -> str:
    """A queue-group name out of a subject.

    NATS forbids ``.``, ``*``, ``>``, whitespace and path separators in these,
    so the dots that make a subject readable have to go.
    """
    return re.sub(r"[^-a-zA-Z0-9]", "_", name)


def _check_subject(topic: str) -> str:
    """Refuse a topic that would not survive being a subject."""
    if not topic or _SUBJECT_BAD.search(topic) or ".." in topic:
        raise ValueError(
            f"{topic!r} cannot be a NATS subject: no whitespace, '*', '>' or "
            f"empty segments"
        )
    return topic


class NatsTransport(BrokerTransport):
    """A NATS connection and the subject names this node owns."""

    def __init__(
        self,
        config: BrokerConfig,
        *,
        connection: NatsClient | None = None,
    ) -> None:
        self.config = config
        self._nc = connection
        self._owns_connection = connection is None
        self._names: dict[str, str] = {}

    @property
    def _prefix(self) -> str:
        return self.config.key_prefix

    def _fanout_subject(self, topic: str) -> str:
        return f"{self._prefix}.ps.{_check_subject(topic)}"

    def _rpc_subject(self, subject: str) -> str:
        """Where a live request is asked. Core NATS, and no stream over it."""
        return f"{self._prefix}.rpc.{_check_subject(subject)}"

    def _topic_from_subject(self, subject: str) -> str:
        """The topic a caller asked for, back out of the subject it arrived on."""
        needle = f"{self._prefix}.ps."
        if subject.startswith(needle):
            return subject[len(needle) :]
        return subject

    def _named(self, kind: str, original: str) -> str:
        """A queue-group name for ``original``, unique within this node."""
        name = _sanitize(f"{self._prefix}_{kind}_{original}")
        taken = self._names.setdefault(name, original)
        if taken != original:
            raise ValueError(
                f"{original!r} and {taken!r} both name the NATS {kind} "
                f"{name!r}; one of them has to be spelled differently"
            )
        return name

    @property
    def nc(self) -> NatsClient:
        if self._nc is None:
            raise BrokerNotConnectedError(
                "Broker is not connected; call connect() first"
            )
        return self._nc

    async def connect(self) -> None:
        if self._nc is None:
            self._nc = await nats.connect(
                self.config.nats_url,
                max_reconnect_attempts=-1,
                pending_size=8 * 1024 * 1024,
            )
            self._owns_connection = True

    async def close(self) -> None:
        if self._nc is not None and self._owns_connection:
            with contextlib.suppress(Exception):
                await self._nc.flush(timeout=_CLOSE_FLUSH_TIMEOUT_S)
            with contextlib.suppress(Exception):
                await self._nc.close()
            self._nc = None

    def describe(self) -> str:
        """Which server this ended up on, with the credential taken out."""
        connected = getattr(self._nc, "connected_url", None)
        url = self.config.nats_url if connected is None else connected.geturl()
        return f"NATS at {redacted_url(url)}"

    async def publish(self, topic: str, raw: str) -> None:
        await self.nc.publish(self._fanout_subject(topic), raw.encode())

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        stop: asyncio.Event | None,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[tuple[str, str]]:
        subjects = [self._fanout_subject(t) for t in topics]
        async for item in self._consume(subjects, stop=stop, ready=ready):
            yield item

    async def psubscribe(
        self, patterns: Sequence[str], *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str]]:
        subjects = [f"{self._prefix}.ps.{p}" for p in patterns]
        async for item in self._consume(subjects, stop=stop, ready=None):
            yield item

    async def _drain_pending(self) -> None:
        """Write buffered commands to this server, without a PING/PONG.

        ``Client.flush`` parks a future in ``_pongs``, and a cancelled
        waiter is not removed. Forcing the pending write is enough for
        the SUB to leave this process.
        """
        await self.nc._flush_pending(force_flush=True)

    async def _consume(
        self,
        subjects: Sequence[str],
        *,
        stop: asyncio.Event | None,
        ready: asyncio.Event | None,
    ) -> AsyncIterator[tuple[str, str]]:
        """One core subscription per subject, merged, until ``stop``."""
        if not subjects:
            raise ValueError("a subscription needs at least one subject")

        inbound: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        async def handler(msg: Msg) -> None:
            topic = self._topic_from_subject(msg.subject)
            await inbound.put((topic, msg.data.decode()))

        subs = [
            await self.nc.subscribe(subject, cb=handler) for subject in subjects
        ]
        await self._drain_pending()
        if ready is not None:
            ready.set()
        try:
            async for item in _iter_until_stopped(inbound, stop=stop):
                yield item
        finally:
            for sub in subs:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()

    def reply_inbox(self, request_id: str) -> str | None:
        """``None``: NATS carries a reply subject of its own beside the message."""
        return None

    async def request(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """Ask, and give a subject a moment to have somebody on it."""
        return await self._ask(
            subject,
            raw,
            request_id=request_id,
            timeout=timeout,
            reask=min(
                max(timeout * _NO_RESPONDERS_SHARE, _NO_RESPONDERS_FLOOR_S),
                _NO_RESPONDERS_CEILING_S,
            ),
        )

    async def _ask(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        timeout: float,
        reask: float,
    ) -> str:
        """One core request, re-asked while nobody is on the subject."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        give_up_asking = loop.time() + reask
        subject_name = self._rpc_subject(subject)
        payload = raw.encode()
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                msg = await self.nc.request(
                    subject_name, payload, timeout=remaining
                )
            except nats.errors.NoRespondersError:
                if loop.time() + _NO_RESPONDERS_GRACE_S >= give_up_asking:
                    break
                await asyncio.sleep(_NO_RESPONDERS_GRACE_S)
                continue
            except (nats.errors.TimeoutError, TimeoutError):
                break
            return msg.data.decode()
        raise RequestTimeoutError(subject, request_id, timeout) from None

    async def probe(
        self,
        subject: str,
        raw: str,
        *,
        request_id: str,
        inbox: str | None,
        timeout: float,
    ) -> str:
        """:meth:`request` without the patience, and that is the point."""
        return await self._ask(
            subject,
            raw,
            request_id=request_id,
            timeout=timeout,
            reask=_NO_RESPONDERS_FLOOR_S,
        )

    async def serve(
        self, subject: str, *, stop: asyncio.Event | None
    ) -> AsyncIterator[tuple[str, str | None]]:
        """Core queue subscription. The queue group is what makes a pool."""
        live_subject = self._rpc_subject(subject)
        group = self._named("group", subject)
        inbound: asyncio.Queue[tuple[str, str | None]] = asyncio.Queue()

        async def handler(msg: Msg) -> None:
            await inbound.put((msg.data.decode(), msg.reply or None))

        live = await self.nc.subscribe(live_subject, queue=group, cb=handler)
        try:
            async for item in _iter_until_stopped(inbound, stop=stop):
                yield item
        finally:
            with contextlib.suppress(Exception):
                await live.unsubscribe()

    async def send_reply(self, inbox: str, raw: str) -> None:
        await self.nc.publish(inbox, raw.encode())


class _Stopped:
    """The stop event, in a shape a queue can carry."""

    __slots__ = ()


_STOPPED = _Stopped()


async def _tap_stop(stop: asyncio.Event, inbound: asyncio.Queue[Any]) -> None:
    """Turn the stop event into the last item on ``inbound``."""
    await stop.wait()
    inbound.put_nowait(_STOPPED)


async def _iter_until_stopped(
    inbound: asyncio.Queue[tuple[str, str | None]] | asyncio.Queue[tuple[str, str]],
    *,
    stop: asyncio.Event | None,
) -> AsyncIterator:
    """Yield from ``inbound`` until ``stop`` is set.

    The stop event arrives through the queue rather than beside it, so a
    cancellation reaches the one waiter and a cancelled ``Queue.get``
    leaves what it was about to take in the queue.
    """
    if stop is None:
        while True:
            yield await inbound.get()

    tap = asyncio.create_task(_tap_stop(stop, inbound))
    try:
        while True:
            item = await inbound.get()
            if item is _STOPPED:
                break
            yield item
        while not inbound.empty():
            yield inbound.get_nowait()
    finally:
        tap.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await tap

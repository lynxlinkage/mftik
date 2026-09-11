"""The broker — what a plane may say, over whichever transport it was given."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

from mftik.broker.config import BrokerConfig
from mftik.broker.link import LeasedSessionLink
from mftik.broker.request import IncomingRequest
from mftik.broker.transport import build as build_transport
from mftik.broker.transport.base import BrokerTransport
from mftik.protocol import (
    Envelope,
    Heartbeat,
    HeartbeatEnvelope,
    Topics,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)

Handler = Callable[[IncomingRequest], Awaitable[None]]


class Broker:
    """Async IPC client — the whole of what a plane may say.

    Six processes, none of which import each other, and this is what they
    share. Fan-out is best effort, a request nobody serves fails at once,
    and a session link expires after three missed heartbeats.

    The store underneath is a :class:`~mftik.broker.transport.base.BrokerTransport`
    from :func:`mftik.broker.transport.build`. Nothing above this class may
    know the store.
    """

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        transport: BrokerTransport | None = None,
    ) -> None:
        self.config = config or BrokerConfig.from_env()
        self._transport = transport or build_transport(self.config)

    @property
    def transport(self) -> BrokerTransport:
        """The store this broker is talking to.

        Here for the tests that need to break a transport on purpose to prove a
        serve loop survives it. A domain reaching this is going around the
        broker, and the guard test says so by name.
        """
        return self._transport

    async def connect(self) -> None:
        await self._transport.connect()
        logger.info("Connected to %s", self._transport.describe())

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> Broker:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def publish(self, topic: str, envelope: Envelope[Any]) -> None:
        """Publish an envelope to a fan-out topic.

        Nothing comes back. A message published while nobody is subscribed
        is gone. Session-log late replay is ``session_logs``; status late
        replay is the session list.
        """
        await self._transport.publish(topic, envelope.to_json())

    async def publish_log(
        self,
        topic: str,
        envelope: Envelope[Any],
        *,
        maxlen: int | None = None,
        ttl_seconds: int = 86_400,
    ) -> None:
        """Publish onto a log topic. Same as :meth:`publish`.

        ``maxlen`` and ``ttl_seconds`` are accepted and ignored so existing
        callers do not have to change on the same commit.
        """
        del maxlen, ttl_seconds
        await self.publish(topic, envelope)

    async def subscribe(
        self,
        topics: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
        ready: asyncio.Event | None = None,
    ) -> AsyncIterator[UntypedEnvelope]:
        """Yield envelopes from one or more fan-out topics until ``stop``.

        Messages published while not subscribed are gone. ``ready`` is set
        once this process's server has the subscription, before the first
        yield.
        """
        topic_list = (topics,) if isinstance(topics, str) else tuple(topics)
        if not topic_list:
            raise ValueError("subscribe requires at least one topic")
        async for _topic, raw in self._transport.subscribe(
            topic_list, stop=stop, ready=ready
        ):
            yield UntypedEnvelope.from_json(raw)

    async def psubscribe(
        self,
        patterns: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[tuple[str, UntypedEnvelope]]:
        """Yield ``(topic, envelope)`` from pattern subscriptions until ``stop``."""
        pattern_list = (patterns,) if isinstance(patterns, str) else tuple(patterns)
        if not pattern_list:
            raise ValueError("psubscribe requires at least one pattern")
        async for topic, raw in self._transport.psubscribe(pattern_list, stop=stop):
            yield topic, UntypedEnvelope.from_json(raw)

    def leased_link(self, **kwargs: Any) -> LeasedSessionLink:
        """A fenced session link. See :class:`LeasedSessionLink`."""
        return LeasedSessionLink(self, **kwargs)

    async def request(
        self,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float | None = None,
    ) -> UntypedEnvelope:
        """Send a request and wait for a single reply.

        A :class:`~mftik.broker.errors.RequestTimeoutError` is the caller's
        answer of "down" at least as often as it is a fault.
        """
        wait = self.config.request_timeout if timeout is None else timeout
        outbound = self._addressed(envelope)
        raw = await self._transport.request(
            subject,
            outbound.to_json(),
            request_id=outbound.id,
            inbox=outbound.reply_to,
            timeout=wait,
        )
        return UntypedEnvelope.from_json(raw)

    async def probe(
        self,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float | None = None,
    ) -> UntypedEnvelope:
        """Ask whether somebody is serving ``subject``, leaving nothing behind."""
        wait = self.config.request_timeout if timeout is None else timeout
        outbound = self._addressed(envelope)
        raw = await self._transport.probe(
            subject,
            outbound.to_json(),
            request_id=outbound.id,
            inbox=outbound.reply_to,
            timeout=wait,
        )
        return UntypedEnvelope.from_json(raw)

    def _addressed(self, envelope: Envelope[Any]) -> Envelope[Any]:
        """``envelope`` carrying the reply address, if this transport uses one."""
        inbox = self._transport.reply_inbox(envelope.id)
        if inbox is None or envelope.reply_to == inbox:
            return envelope
        return envelope.model_copy(update={"reply_to": inbox})

    async def serve(
        self,
        subject: str,
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[IncomingRequest]:
        """Yield incoming requests on a request-reply subject.

        Only ``stop`` ends this loop.
        """
        async for raw, inbox in self._transport.serve(subject, stop=stop):
            try:
                envelope = UntypedEnvelope.from_json(raw)
            except Exception:
                logger.exception(
                    "broker serve dropped an unreadable request subject=%s",
                    subject,
                )
                continue
            if inbox is not None and envelope.reply_to != inbox:
                envelope = envelope.model_copy(update={"reply_to": inbox})
            yield IncomingRequest(self, envelope)

    async def serve_handler(
        self,
        subject: str,
        handler: Handler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:
        """Run ``handler`` for each incoming request until ``stop``."""
        async for req in self.serve(subject, stop=stop):
            await handler(req)

    async def _send_reply(self, reply_to: str, envelope: Envelope[Any]) -> None:
        await self._transport.send_reply(reply_to, envelope.to_json())

    async def heartbeat_loop(
        self,
        source: str,
        *,
        interval: float = 5.0,
        stop: asyncio.Event | None = None,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        """Publish periodic heartbeats on the heartbeat fan-out topic."""
        while stop is None or not stop.is_set():
            envelope = HeartbeatEnvelope.wrap(
                Heartbeat(),
                type="heartbeat",
                source=source,
            )
            await self.publish(Topics.HEARTBEAT, envelope)
            if on_tick is not None:
                on_tick()
            try:
                if stop is not None:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                else:
                    await asyncio.sleep(interval)
            except TimeoutError:
                continue


# Back-compat alias used during the rename.
BrokerClient = Broker

"""Async Redis broker — pub/sub and request-reply IPC."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import redis.asyncio as redis

from mft.broker.config import BrokerConfig
from mft.broker.errors import BrokerNotConnectedError, RequestTimeoutError
from mft.broker.request import IncomingRequest
from mft.broker.stream import BidirectionalStream
from mft.protocol import (
    Envelope,
    Heartbeat,
    HeartbeatEnvelope,
    Topics,
    UntypedEnvelope,
)

logger = logging.getLogger(__name__)

Handler = Callable[[IncomingRequest], Awaitable[None]]


class Broker:
    """Async Redis IPC client.

    Three primitives:

    1. **Pub/Sub** — fan-out broadcast via Redis Pub/Sub
       (``publish`` / ``subscribe``).
    2. **Request-reply** — 1:1 RPC via Redis lists
       (``request`` / ``serve``).
    3. **Bidirectional stream** — duplex channel = pub + sub
       (``bistream``).
    """

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        redis_client: redis.Redis | None = None,
    ) -> None:
        self.config = config or BrokerConfig.from_env()
        self._redis = redis_client
        self._owns_redis = redis_client is None

    # --- lifecycle ---------------------------------------------------------

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            raise BrokerNotConnectedError(
                "Broker is not connected; call connect() first"
            )
        return self._redis

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = redis.from_url(
                self.config.redis_url, decode_responses=True
            )
            self._owns_redis = True
        await self._redis.ping()
        logger.info("Connected to Redis at %s", self.config.redis_url)

    async def close(self) -> None:
        if self._redis is not None and self._owns_redis:
            await self._redis.aclose()
            self._redis = None

    async def __aenter__(self) -> Broker:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # --- key helpers -------------------------------------------------------

    def _rpc_queue(self, subject: str) -> str:
        return f"{self.config.key_prefix}:rpc:{subject}"

    def _rpc_reply(self, request_id: str) -> str:
        return f"{self.config.key_prefix}:rpc:reply:{request_id}"

    def _log_buffer_key(self, topic: str) -> str:
        return f"{self.config.key_prefix}:logbuf:{topic}"

    # --- Pub/Sub -----------------------------------------------------------

    async def publish(self, topic: str, envelope: Envelope[Any]) -> int:
        """Publish an envelope to a pub/sub topic (fan-out)."""
        return int(await self.redis.publish(topic, envelope.to_json()))

    async def publish_log(
        self,
        topic: str,
        envelope: Envelope[Any],
        *,
        maxlen: int = 500,
        ttl_seconds: int = 86_400,
    ) -> int:
        """Publish a log line and append it to a Redis list for late subscribers.

        Redis Pub/Sub alone drops messages when nobody is listening (e.g. UI
        opens ``/ws/sts/...`` after deploy). The buffer is replayed on connect.
        """
        raw = envelope.to_json()
        key = self._log_buffer_key(topic)
        pipe = self.redis.pipeline()
        pipe.rpush(key, raw)
        pipe.ltrim(key, -maxlen, -1)
        pipe.expire(key, ttl_seconds)
        pipe.publish(topic, raw)
        results = await pipe.execute()
        return int(results[-1])

    async def fetch_log_buffer(self, topic: str) -> list[str]:
        """Return buffered log JSON lines for ``topic`` (oldest → newest)."""
        rows = await self.redis.lrange(self._log_buffer_key(topic), 0, -1)
        return list(rows)

    async def subscribe(
        self,
        topics: str | Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[UntypedEnvelope]:
        """Yield envelopes from one or more pub/sub topics until ``stop``.

        Uses Redis Pub/Sub. Messages published while not subscribed are lost
        unless they were also written via :meth:`publish_log`.
        """
        channel_list = (topics,) if isinstance(topics, str) else tuple(topics)
        if not channel_list:
            raise ValueError("subscribe requires at least one topic")

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(*channel_list)
        try:
            while stop is None or not stop.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    await asyncio.sleep(0.01)
                    continue
                data = message.get("data")
                if data is None:
                    continue
                yield UntypedEnvelope.from_json(data)
        finally:
            await pubsub.unsubscribe(*channel_list)
            await pubsub.aclose()

    def bistream(
        self,
        *,
        tx: str,
        rx: str,
    ) -> BidirectionalStream:
        """Open a bidirectional stream (publish on ``tx``, subscribe on ``rx``)."""
        return BidirectionalStream(self, tx=tx, rx=rx)

    def bistream_pair(
        self,
        name: str,
    ) -> tuple[BidirectionalStream, BidirectionalStream]:
        """Open both ends of a named bistream: ``(up, down)``.

        ``up`` publishes ``bistream.{name}.up`` and receives ``.down``;
        ``down`` is the complement.
        """
        up_topic, down_topic = BidirectionalStream.topics(name)
        up = self.bistream(tx=up_topic, rx=down_topic)
        down = self.bistream(tx=down_topic, rx=up_topic)
        return up, down

    # --- Request-reply -----------------------------------------------------

    async def request(
        self,
        subject: str,
        envelope: Envelope[Any],
        *,
        timeout: float | None = None,
    ) -> UntypedEnvelope:
        """Send a request and wait for a single reply.

        The envelope's ``id`` is used as the correlation id. A temporary
        reply list key is written into ``reply_to`` before enqueueing.
        """
        wait = self.config.request_timeout if timeout is None else timeout
        reply_key = self._rpc_reply(envelope.id)
        outbound = (
            envelope
            if envelope.reply_to == reply_key
            else envelope.model_copy(update={"reply_to": reply_key})
        )

        queue = self._rpc_queue(subject)
        await self.redis.rpush(queue, outbound.to_json())

        # BLPOP timeout is whole seconds; poll until the deadline for accuracy.
        deadline = time.monotonic() + wait
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RequestTimeoutError(subject, outbound.id, wait)
                result = await self.redis.blpop(reply_key, timeout=1)
                if result is None:
                    continue
                _key, data = result
                return UntypedEnvelope.from_json(data)
        finally:
            await self.redis.delete(reply_key)

    async def serve(
        self,
        subject: str,
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[IncomingRequest]:
        """Yield incoming requests on a request-reply subject.

        Call ``await req.reply(envelope)`` to respond. Competing consumers
        on the same subject share work via Redis list ``BLPOP``.
        """
        queue = self._rpc_queue(subject)
        while stop is None or not stop.is_set():
            result = await self.redis.blpop(queue, timeout=1)
            if result is None:
                continue
            _key, data = result
            envelope = UntypedEnvelope.from_json(data)
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
        await self.redis.rpush(reply_to, envelope.to_json())
        await self.redis.expire(reply_to, self.config.reply_ttl_seconds)

    # --- convenience -------------------------------------------------------

    async def heartbeat_loop(
        self,
        source: str,
        *,
        interval: float = 5.0,
        stop: asyncio.Event | None = None,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        """Publish periodic heartbeats on the heartbeat pub/sub topic."""
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

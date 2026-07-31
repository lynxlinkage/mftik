from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Sequence

import redis.asyncio as redis
from redis.asyncio.client import PubSub

from mft.broker.config import BrokerConfig
from mft.protocol import MessageEnvelope

logger = logging.getLogger(__name__)


class BrokerClient:
    """Async Redis broker: Pub/Sub + Streams (point-to-point)."""

    def __init__(self, config: BrokerConfig | None = None) -> None:
        self.config = config or BrokerConfig.from_env()
        self._redis: redis.Redis | None = None

    @property
    def redis(self) -> redis.Redis:
        if self._redis is None:
            raise RuntimeError("BrokerClient is not connected; call connect() first")
        return self._redis

    async def connect(self) -> None:
        self._redis = redis.from_url(self.config.redis_url, decode_responses=True)
        await self._redis.ping()
        logger.info("Connected to Redis at %s", self.config.redis_url)

    async def close(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def __aenter__(self) -> BrokerClient:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # --- Pub/Sub -----------------------------------------------------------

    async def publish(self, channel: str, envelope: MessageEnvelope) -> int:
        return int(await self.redis.publish(channel, envelope.to_json()))

    async def subscribe(self, channels: Sequence[str]) -> PubSub:
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(*channels)
        return pubsub

    async def listen(
        self,
        channels: Sequence[str],
        *,
        stop: asyncio.Event | None = None,
    ) -> AsyncIterator[MessageEnvelope]:
        pubsub = await self.subscribe(channels)
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
                yield MessageEnvelope.from_json(data)
        finally:
            await pubsub.unsubscribe(*channels)
            await pubsub.aclose()

    # --- Point-to-point (Redis Streams) ------------------------------------

    async def send(self, stream: str, envelope: MessageEnvelope) -> str:
        return str(await self.redis.xadd(stream, {"data": envelope.to_json()}))

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self.redis.xgroup_create(stream, group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def receive(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 10,
        block_ms: int = 1000,
    ) -> list[MessageEnvelope]:
        await self.ensure_group(stream, group)
        results = await self.redis.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=count,
            block=block_ms,
        )
        envelopes: list[MessageEnvelope] = []
        if not results:
            return envelopes
        for _stream_name, messages in results:
            for msg_id, fields in messages:
                raw = fields.get("data")
                if raw is None:
                    continue
                envelopes.append(MessageEnvelope.from_json(raw))
                await self.redis.xack(stream, group, msg_id)
        return envelopes

    async def heartbeat_loop(
        self,
        source: str,
        *,
        interval: float = 5.0,
        stop: asyncio.Event | None = None,
        on_tick: Callable[[], None] | None = None,
    ) -> None:
        """Publish periodic heartbeats until stop is set."""
        from mft.protocol import Topics

        while stop is None or not stop.is_set():
            envelope = MessageEnvelope(
                type="heartbeat",
                source=source,
                payload={"status": "ok"},
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

"""Per-feed first-publish tracking so an updater can wait before cutting over.

A feed is "published" the first time this process is handed a venue message
for it — whether or not the dedup gate then dropped it as a copy. That is
the signal that this MD is on the wire, not merely subscribed.

``role`` keeps the sidecar's set off the primary's: ``mirror`` while
``MD_MIRROR=1``, ``primary`` otherwise. The updater reads the same keys.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from mftik.broker import Broker

logger = logging.getLogger(__name__)

ROLE_PRIMARY = "primary"
ROLE_MIRROR = "mirror"


def published_key(key_prefix: str, role: str) -> str:
    return f"{key_prefix}:md:published:{role}"


def pinned_key(key_prefix: str, role: str) -> str:
    return f"{key_prefix}:md:pinned:{role}"


def ready_key(key_prefix: str, role: str) -> str:
    return f"{key_prefix}:md:ready:{role}"


class PublishTracker:
    """Redis sets: pinned feeds, published feeds, and a ready flag."""

    def __init__(
        self,
        broker: Broker,
        *,
        role: str,
        pinned: Iterable[str] = (),
    ) -> None:
        self._broker = broker
        self.role = role
        self.pinned = frozenset(pinned)

    def set_pinned(self, feeds: Iterable[str]) -> None:
        self.pinned = frozenset(feeds)

    @property
    def _prefix(self) -> str:
        return self._broker.config.key_prefix

    async def reset(self) -> None:
        """Drop last run's keys, then write this run's pin list."""
        redis = self._broker.redis
        await redis.delete(
            published_key(self._prefix, self.role),
            pinned_key(self._prefix, self.role),
            ready_key(self._prefix, self.role),
        )
        if self.pinned:
            await redis.sadd(pinned_key(self._prefix, self.role), *self.pinned)
        else:
            await redis.set(ready_key(self._prefix, self.role), "1")

    async def mark_published(self, feed: str) -> None:
        """Record that ``feed`` has produced at least one venue message."""
        redis = self._broker.redis
        await redis.sadd(published_key(self._prefix, self.role), feed)
        if not self.pinned:
            return
        if await self.all_published():
            await redis.set(ready_key(self._prefix, self.role), "1")

    async def published(self) -> set[str]:
        raw: Any = await self._broker.redis.smembers(
            published_key(self._prefix, self.role)
        )
        return {str(item) for item in raw}

    async def all_published(self) -> bool:
        if not self.pinned:
            return True
        return self.pinned <= await self.published()

    async def is_ready(self) -> bool:
        return bool(await self._broker.redis.get(ready_key(self._prefix, self.role)))

"""MD ↔ STS bridge — feed key → N session streams (no md.global)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mft.broker import Broker
from mft.protocol import Topics, UntypedEnvelope, publish_md_log

if TYPE_CHECKING:
    from mft_md.session.manager import StsLink

logger = logging.getLogger(__name__)

FeedKey = tuple[str, str, str]  # (venue, topic, symbol)

#: Dispatches between log lines on a feed. One line per message buries every
#: other md event at book speed, and the interesting part of a dispatch line —
#: how many sessions it reached — only changes when subscriptions do.
LOG_EVERY = 20


class Dispatcher:
    """Route venue market-data updates to subscribed STS session streams."""

    def __init__(self, broker: Broker) -> None:
        self._broker = broker
        self._subs: dict[FeedKey, set[str]] = {}
        self._links: dict[str, StsLink] = {}
        #: Messages dispatched per feed since it last had no subscribers.
        #: Drives the log sampling; reset with the subscription so a feed that
        #: comes back announces its first message again.
        self._dispatched: dict[FeedKey, int] = {}

    def register_link(self, link: StsLink) -> None:
        self._links[link.session_id] = link

    def unregister_link(self, session_id: str) -> None:
        self._links.pop(session_id, None)

    def subscribe(
        self, session_id: str, venue: str, topic: str, symbol: str
    ) -> tuple[bool, int]:
        """Add subscription. Returns ``(first_subscriber, new_refcount)``."""
        key = (venue, topic, symbol)
        subs = self._subs.setdefault(key, set())
        first = len(subs) == 0
        subs.add(session_id)
        return first, len(subs)

    def unsubscribe(
        self, session_id: str, venue: str, topic: str, symbol: str
    ) -> tuple[bool, int]:
        """Remove subscription. Returns ``(emptied, new_refcount)``."""
        key = (venue, topic, symbol)
        subs = self._subs.get(key)
        if not subs:
            return True, 0
        subs.discard(session_id)
        if not subs:
            del self._subs[key]
            self._dispatched.pop(key, None)
            return True, 0
        return False, len(subs)

    def unsubscribe_all(self, session_id: str) -> list[tuple[FeedKey, int, int]]:
        """Drop all feeds for ``session_id``.

        Returns ``[(key, old_refcount, new_refcount), ...]`` for keys that changed.
        """
        changed: list[tuple[FeedKey, int, int]] = []
        for key in list(self._subs):
            if session_id not in self._subs.get(key, ()):
                continue
            old = len(self._subs[key])
            _emptied, new = self.unsubscribe(session_id, *key)
            changed.append((key, old, new))
        self.unregister_link(session_id)
        return changed

    def subscribers(self, venue: str, topic: str, symbol: str) -> set[str]:
        return set(self._subs.get((venue, topic, symbol), ()))

    def refcount(self, venue: str, topic: str, symbol: str) -> int:
        return len(self._subs.get((venue, topic, symbol), ()))

    def refcounts(self) -> dict[str, int]:
        return {
            Topics.md_feed(v, t, s): len(subs)
            for (v, t, s), subs in self._subs.items()
        }

    async def publish(
        self,
        venue: str,
        topic: str,
        symbol: str,
        envelope: UntypedEnvelope,
    ) -> None:
        """Fan out one venue update to N live STS session streams."""
        targets = [
            sid
            for sid in list(self._subs.get((venue, topic, symbol), ()))
            if sid in self._links
        ]
        sent = 0
        for session_id in targets:
            try:
                await self._broker.publish(
                    Topics.md_session(session_id), envelope
                )
                sent += 1
            except Exception:
                logger.exception(
                    "MD dispatch failed session=%s feed=%s.%s.%s",
                    session_id,
                    venue,
                    topic,
                    symbol,
                )
        key = (venue, topic, symbol)
        count = self._dispatched.get(key, 0) + 1
        self._dispatched[key] = count
        # A partial fan-out means a session missed data, so it is never
        # sampled away — only the healthy repeats are.
        if sent == len(targets) and count > 1 and count % LOG_EVERY:
            return
        feed = Topics.md_feed(venue, topic, symbol)
        await publish_md_log(
            self._broker,
            venue,
            (
                f"dispatch {envelope.type} {feed} "
                f"→ {sent}/{len(targets)} sessions (#{count})"
            ),
            source="md",
        )

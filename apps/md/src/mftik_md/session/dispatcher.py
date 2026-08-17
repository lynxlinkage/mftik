"""MD ↔ STS bridge — feed key → N session streams (no md.global)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.broker import Broker
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import Topics, UntypedEnvelope, publish_md_log

from mftik_md.dedup import SEEN_TTL_S, event_id, seen_key

if TYPE_CHECKING:
    from mftik_md.publish_track import PublishTracker
    from mftik_md.session.manager import StsLink
    from mftik_md.tape import TapeRecorder

logger = logging.getLogger(__name__)

#: What a subscription is refcounted on. The ticker carries the venue, so the
#: key is two parts, not four — and is exactly what ``Topics.md_feed`` renders.
FeedKey = tuple[str, UniversalTicker]  # (topic, ticker)

#: Dispatches between log lines on a feed. One line per message buries every
#: other md event at book speed, and the interesting part of a dispatch line —
#: how many sessions it reached — only changes when subscriptions do.
LOG_EVERY = 20


class Dispatcher:
    """Route venue market-data updates to subscribed STS session streams."""

    def __init__(
        self,
        broker: Broker,
        *,
        recorder: TapeRecorder | None = None,
        tracker: PublishTracker | None = None,
    ) -> None:
        self._broker = broker
        #: Writes recorded feeds to their tape. None disables recording, which
        #: costs a strategy its warm-up and nothing else.
        self._recorder = recorder
        #: First-publish set the updater waits on. None still dedups; it just
        #: does not advertise readiness.
        self._tracker = tracker
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
        self, session_id: str, topic: str, ticker: UniversalTicker
    ) -> tuple[bool, int]:
        """Add subscription. Returns ``(first_subscriber, new_refcount)``."""
        key = (topic, ticker)
        subs = self._subs.setdefault(key, set())
        first = len(subs) == 0
        subs.add(session_id)
        return first, len(subs)

    def unsubscribe(
        self, session_id: str, topic: str, ticker: UniversalTicker
    ) -> tuple[bool, int]:
        """Remove subscription. Returns ``(emptied, new_refcount)``."""
        key = (topic, ticker)
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

    def subscribers(self, topic: str, ticker: UniversalTicker) -> set[str]:
        return set(self._subs.get((topic, ticker), ()))

    def refcount(self, topic: str, ticker: UniversalTicker) -> int:
        return len(self._subs.get((topic, ticker), ()))

    def refcounts(self) -> dict[str, int]:
        return {
            Topics.md_feed(topic, ticker): len(subs)
            for (topic, ticker), subs in self._subs.items()
        }

    async def publish(
        self,
        topic: str,
        ticker: UniversalTicker,
        envelope: UntypedEnvelope,
    ) -> None:
        """Fan out one venue update to N live STS session streams.

        Dedup runs first: a second MD overlapping this feed must not deliver
        the same print twice. A dropped copy still counts as this process
        having seen the feed — that is what the updater waits for.
        """
        feed = Topics.md_feed(topic, ticker)
        if self._tracker is not None:
            await self._tracker.mark_published(feed)
        if not await self._claim(topic, ticker, envelope.payload):
            return
        key = (topic, ticker)
        targets = [
            sid for sid in list(self._subs.get(key, ())) if sid in self._links
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
                    "MD dispatch failed session=%s feed=%s",
                    session_id,
                    Topics.md_feed(topic, ticker),
                )
        # After the fan-out, not before: the sessions waiting on this print are
        # trading on it, and recording is for a session that has not started
        # yet. It also runs whether or not anyone was listening — the feed is
        # pumping because *someone* holds it, and that is what makes the tape
        # continuous rather than a record of who happened to be attached.
        if self._recorder is not None:
            await self._recorder.append(topic, ticker, envelope.payload)
        count = self._dispatched.get(key, 0) + 1
        self._dispatched[key] = count
        # A partial fan-out means a session missed data, so it is never
        # sampled away — only the healthy repeats are.
        if sent == len(targets) and count > 1 and count % LOG_EVERY:
            return
        await publish_md_log(
            self._broker,
            ticker.venue,
            (
                f"dispatch {envelope.type} {feed} "
                f"→ {sent}/{len(targets)} sessions (#{count})"
            ),
            source="md",
        )

    async def _claim(
        self,
        topic: str,
        ticker: UniversalTicker,
        payload: object,
    ) -> bool:
        """Take the first delivery of this event. False means a peer already did.

        Redis unavailable is a publish, not a drop: losing a print is worse
        than a rare double during an outage, and the overlap this exists for
        has Redis up.
        """
        if not isinstance(payload, dict):
            return True
        ident = event_id(topic, payload)
        if ident is None:
            return True
        key = seen_key(self._broker.config.key_prefix, topic, ticker, ident)
        try:
            claimed = await self._broker.redis.set(key, "1", nx=True, ex=SEEN_TTL_S)
        except Exception:
            logger.exception(
                "MD dedup claim failed feed=%s — publishing",
                Topics.md_feed(topic, ticker),
            )
            return True
        return bool(claimed)

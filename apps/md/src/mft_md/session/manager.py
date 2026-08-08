"""MD session manager — venue feeds, STS attach, fencing lease, dispatcher."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from mft.broker import Broker
from mft.protocol import (
    MD_DETACH,
    MD_LEASE_ACK,
    MD_SUBSCRIBE,
    MD_UNSUBSCRIBE,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    ListSessionsRequest,
    MdAttachRequest,
    MdAttachResult,
    MdDetach,
    MdLeaseAck,
    MdSubscribe,
    MdUnsubscribe,
    SessionInfo,
    Topics,
    publish_md_log,
)
from mft_db.models.session import SessionDomain, SessionStatus

from mft_md.session.dispatcher import Dispatcher, FeedKey
from mft_md.session.factory import ConnectorFactory
from mft_md.session.venue import VenueSession

logger = logging.getLogger(__name__)

PersistLive = Callable[..., Awaitable[Any]]
MarkDone = Callable[..., Awaitable[Any]]
ListDbSessions = Callable[..., Awaitable[Sequence[Any]]]

LEASE_GRACE_S = 3.0


@dataclass
class StsLink:
    """One STS session attached to MD."""

    session_id: str
    created_by: int
    subscriptions: set[str] = field(default_factory=set)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    last_token: int = 0


class SessionManager:
    """Owns venue sessions + STS links; refcount feeds via Dispatcher."""

    def __init__(
        self,
        factory: ConnectorFactory,
        broker: Broker,
        *,
        persist_live: PersistLive | None = None,
        mark_done: MarkDone | None = None,
        list_db_sessions: ListDbSessions | None = None,
        lease_grace: float = LEASE_GRACE_S,
    ) -> None:
        self._factory = factory
        self._broker = broker
        self._persist_live = persist_live
        self._mark_done = mark_done
        self._list_db_sessions = list_db_sessions
        self._lease_grace = lease_grace
        self._dispatcher = Dispatcher(broker)
        self._venues: dict[str, VenueSession] = {}
        self._links: dict[str, StsLink] = {}

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher

    def feed_refcount(self, feed: str) -> int:
        return self._dispatcher.refcount(*Topics.parse_md_feed(feed))

    async def attach(self, request: MdAttachRequest) -> MdAttachResult:
        """Attach STS ``session_id`` with subscriptions (lease + refcount)."""
        existing = self._links.get(request.session_id)
        if existing is not None:
            return MdAttachResult(
                session_id=request.session_id,
                subscriptions=sorted(existing.subscriptions),
                refcounts=self._dispatcher.refcounts(),
            )

        link = StsLink(
            session_id=request.session_id,
            created_by=request.created_by,
        )
        ready = asyncio.Event()
        link.tasks = [
            asyncio.create_task(
                self._lease_loop(link, ready),
                name=f"md-lease-{request.session_id}",
            )
        ]

        try:
            await asyncio.wait_for(ready.wait(), timeout=request.timeout)
        except TimeoutError:
            link.stop.set()
            for t in link.tasks:
                t.cancel()
            await asyncio.gather(*link.tasks, return_exceptions=True)
            raise TimeoutError(
                f"timed out waiting for STS MD lease heartbeat "
                f"session={request.session_id}"
            ) from None

        self._links[request.session_id] = link
        self._dispatcher.register_link(link)

        for feed in request.subscriptions:
            await self._subscribe_feed(link, feed)

        feeds = sorted(link.subscriptions)
        venues = sorted(_venues_from_feeds(feeds))
        if self._persist_live is not None:
            await self._persist_live(
                session_id=request.session_id,
                created_by=request.created_by,
                venues=venues,
            )

        logger.info(
            "MD attached session=%s feeds=%s venues=%s",
            request.session_id,
            feeds,
            venues,
        )
        for venue in venues:
            await publish_md_log(
                self._broker,
                venue,
                (
                    f"sts attached session={request.session_id} "
                    f"feeds={feeds} refcounts={self._dispatcher.refcounts()}"
                ),
                source="md",
            )
        return MdAttachResult(
            session_id=request.session_id,
            subscriptions=feeds,
            refcounts=self._dispatcher.refcounts(),
        )

    async def detach(
        self, *, session_id: str, reason: str = "detach"
    ) -> None:
        link = self._links.pop(session_id, None)
        if link is None:
            return
        changed = self._dispatcher.unsubscribe_all(session_id)
        venues: set[str] = set()
        for (topic, ticker), old_rc, new_rc in changed:
            venues.add(ticker.venue)
            feed = Topics.md_feed(topic, ticker)
            await publish_md_log(
                self._broker,
                ticker.venue,
                (
                    f"refcount {feed} {old_rc}→{new_rc} "
                    f"(sts={session_id} detach reason={reason})"
                ),
                source="md",
            )
            if new_rc == 0:
                await self._stop_feed_if_unused((topic, ticker))
        await self._stop_link(link)
        if self._mark_done is not None:
            await self._mark_done(session_id=session_id)
        logger.info(
            "MD detached session=%s reason=%s", session_id, reason
        )
        for venue in venues or _venues_from_feeds(link.subscriptions):
            await publish_md_log(
                self._broker,
                venue,
                f"sts detached session={session_id} reason={reason}",
                source="md",
            )

    async def list_sessions(
        self, request: ListSessionsRequest
    ) -> list[SessionInfo]:
        if request.domain not in (None, SessionDomain.MD.value, "md"):
            return []

        if self._list_db_sessions is not None:
            db_rows = await self._list_db_sessions(
                status=request.status,
                created_by=request.created_by,
            )
            return [
                SessionInfo(
                    session_id=row.session_id,
                    domain=SessionDomain.MD.value,
                    created_by=row.created_by,
                    created_at=row.created_at.timestamp(),
                    finished_at=(
                        row.finished_at.timestamp()
                        if row.finished_at is not None
                        else None
                    ),
                    status=row.status,
                    sts_session_id=row.session_id,
                    venue=row.venue,
                )
                for row in db_rows
            ]

        if request.status not in (None, SessionStatus.LIVE.value, "live"):
            return []
        items: list[SessionInfo] = []
        for link in self._links.values():
            if (
                request.created_by is not None
                and link.created_by != request.created_by
            ):
                continue
            for venue in sorted(_venues_from_feeds(link.subscriptions)):
                items.append(
                    SessionInfo(
                        session_id=link.session_id,
                        domain=SessionDomain.MD.value,
                        created_by=link.created_by,
                        created_at=0.0,
                        status=SessionStatus.LIVE.value,
                        sts_session_id=link.session_id,
                        venue=venue,
                    )
                )
        return items

    async def close_all(self) -> None:
        for session_id in list(self._links):
            await self.detach(session_id=session_id, reason="shutdown")
        for venue in list(self._venues):
            await self._destroy_venue(venue)

    async def _subscribe_feed(self, link: StsLink, feed: str) -> None:
        topic, ticker = Topics.parse_md_feed(feed)
        first, new_rc = self._dispatcher.subscribe(link.session_id, topic, ticker)
        old_rc = new_rc - 1
        link.subscriptions.add(feed)
        await publish_md_log(
            self._broker,
            ticker.venue,
            (
                f"refcount {feed} {old_rc}→{new_rc} "
                f"(sts={link.session_id} subscribe)"
            ),
            source="md",
        )
        if first:
            venue_sess = await self._ensure_venue(ticker.venue)
            await venue_sess.ensure_feed(topic, ticker)
            await publish_md_log(
                self._broker,
                ticker.venue,
                f"feed pump started {feed}",
                source="md",
            )

    async def _unsubscribe_feed(self, link: StsLink, feed: str) -> None:
        topic, ticker = Topics.parse_md_feed(feed)
        old_rc = self._dispatcher.refcount(topic, ticker)
        emptied, new_rc = self._dispatcher.unsubscribe(
            link.session_id, topic, ticker
        )
        link.subscriptions.discard(feed)
        await publish_md_log(
            self._broker,
            ticker.venue,
            (
                f"refcount {feed} {old_rc}→{new_rc} "
                f"(sts={link.session_id} unsubscribe)"
            ),
            source="md",
        )
        if emptied:
            await self._stop_feed_if_unused((topic, ticker))

    async def _ensure_venue(self, venue: str) -> VenueSession:
        existing = self._venues.get(venue)
        if existing is not None:
            return existing
        public = await self._factory.create(venue)
        sess = VenueSession(
            venue,
            public,
            on_update=self._dispatcher.publish,
        )
        await sess.start()
        self._venues[venue] = sess
        await publish_md_log(
            self._broker,
            venue,
            "venue public client connected",
            source="md",
        )
        logger.info("MD venue public client connected venue=%s", venue)
        return sess

    async def _stop_feed_if_unused(self, key: FeedKey) -> None:
        topic, ticker = key
        if self._dispatcher.refcount(topic, ticker) > 0:
            return
        venue_sess = self._venues.get(ticker.venue)
        if venue_sess is None:
            return
        await venue_sess.stop_feed(topic, ticker)
        await publish_md_log(
            self._broker,
            ticker.venue,
            f"feed pump stopped {Topics.md_feed(topic, ticker)} (refcount 0)",
            source="md",
        )
        if venue_sess.feed_count == 0:
            await self._destroy_venue(ticker.venue)

    async def _destroy_venue(self, venue: str) -> None:
        sess = self._venues.pop(venue, None)
        if sess is None:
            return
        await sess.stop()
        await publish_md_log(
            self._broker,
            venue,
            "venue public client disconnected",
            source="md",
        )

    async def _stop_link(self, link: StsLink) -> None:
        link.stop.set()
        current = asyncio.current_task()
        for task in link.tasks:
            if task is not current:
                task.cancel()
        await asyncio.gather(
            *[t for t in link.tasks if t is not current],
            return_exceptions=True,
        )
        link.tasks.clear()

    async def _lease_loop(self, link: StsLink, ready: asyncio.Event) -> None:
        """Sub sts.md.{session_id}; ACK on md.{session_id}; enforce grace."""
        sts_topic = Topics.sts_md_session(link.session_id)
        md_topic = Topics.md_session(link.session_id)
        last_seen = asyncio.get_running_loop().time()

        async def _watch_timeout() -> None:
            nonlocal last_seen
            while not link.stop.is_set():
                await asyncio.sleep(0.5)
                if (
                    asyncio.get_running_loop().time() - last_seen
                    > self._lease_grace
                ):
                    logger.warning(
                        "MD lease expired session=%s", link.session_id
                    )
                    if self._links.get(link.session_id) is link:
                        # Detach on a sibling task — awaiting it here cancels
                        # this lease loop from inside its own watchdog and can
                        # recurse into RecursionError.
                        asyncio.create_task(
                            self.detach(
                                session_id=link.session_id,
                                reason="lease_expired",
                            ),
                            name=f"md-detach-{link.session_id}",
                        )
                    return

        watchdog = asyncio.create_task(
            _watch_timeout(), name=f"md-lease-wd-{link.session_id}"
        )
        try:
            async for env in self._broker.subscribe(sts_topic, stop=link.stop):
                if env.type == STS_LEASE_HEARTBEAT:
                    try:
                        hb = LeaseHeartbeat.model_validate(env.payload)
                    except Exception:
                        continue
                    last_seen = asyncio.get_running_loop().time()
                    link.last_token = hb.token
                    if not ready.is_set():
                        ready.set()
                    await self._broker.publish(
                        md_topic,
                        Envelope[MdLeaseAck].wrap(
                            MdLeaseAck(
                                session_id=link.session_id,
                                token=hb.token,
                            ),
                            type=MD_LEASE_ACK,
                            source="md",
                            session_id=link.session_id,
                        ),
                    )
                    continue

                if env.type == MD_SUBSCRIBE:
                    try:
                        msg = MdSubscribe.model_validate(env.payload)
                    except Exception:
                        continue
                    if msg.session_id != link.session_id:
                        continue
                    await self._subscribe_feed(link, msg.feed)
                    continue

                if env.type == MD_UNSUBSCRIBE:
                    try:
                        msg = MdUnsubscribe.model_validate(env.payload)
                    except Exception:
                        continue
                    if msg.session_id != link.session_id:
                        continue
                    await self._unsubscribe_feed(link, msg.feed)
                    continue

                if env.type == MD_DETACH:
                    try:
                        det = MdDetach.model_validate(env.payload)
                    except Exception:
                        continue
                    if det.session_id == link.session_id:
                        await self.detach(
                            session_id=link.session_id,
                            reason="sts_stop",
                        )
                        return
        finally:
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)


def _venues_from_feeds(feeds: set[str] | list[str]) -> set[str]:
    venues: set[str] = set()
    for feed in feeds:
        try:
            _topic, ticker = Topics.parse_md_feed(feed)
        except ValueError:
            continue
        venues.add(ticker.venue)
    return venues

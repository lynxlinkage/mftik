"""MD session manager — venue feeds, STS attach, fencing lease, dispatcher."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from mftik.broker import Broker, LeasedSessionLink
from mftik.exchange.models import FeedEnd
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    LEASE_HEARTBEAT_INTERVAL_S,
    LEASE_MISS_LIMIT,
    MD_DETACH,
    MD_FEED_END,
    MD_LEASE_ACK,
    MD_SUBSCRIBE,
    MD_UNSUBSCRIBE,
    Envelope,
    LeaseHeartbeat,
    ListSessionsRequest,
    MdAttachRequest,
    MdAttachResult,
    MdDetach,
    MdLeaseAck,
    MdSubscribe,
    MdUnsubscribe,
    QueryCode,
    SessionInfo,
    Topics,
    UntypedEnvelope,
    publish_md_log,
)
from mftik.symbols import SymbolClient, SymbolNotFoundError
from mftik_db.models.session import SessionDomain, SessionStatus

from mftik_md.session.dispatcher import Dispatcher, FeedKey
from mftik_md.session.factory import ConnectorFactory
from mftik_md.session.venue import Feed, VenueSession
from mftik_md.tape import TapeRecorder
from mftik_md.tape_store import TapeStore

logger = logging.getLogger(__name__)


class AttachError(Exception):
    """Attach refused one of the requested feeds. Nothing was left open.

    ``code`` is the specific refusal (``VENUE_SYMBOL_NOT_FOUND``,
    ``MD_VENUE_UNSUPPORTED_READ``, …). The RPC handler sends it as the
    error code so a deploy rolls back with that reason.
    """

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _sink_future_exception(fut: asyncio.Future[object]) -> None:
    """Read a stored exception so asyncio does not log it as abandoned."""
    if not fut.cancelled():
        fut.exception()


def _exc_reason(exc: BaseException) -> str:
    """Exception text, with a venue code or label when the text omits it."""
    detail = str(exc).strip() or type(exc).__name__
    extra: list[str] = []
    label = getattr(exc, "label", None)
    if isinstance(label, str) and label and label not in detail:
        extra.append(label)
    code = getattr(exc, "code", None)
    if code is not None and not isinstance(code, bool) and str(code) not in detail:
        extra.append(f"code={code}")
    if extra:
        return f"{detail} ({', '.join(extra)})"
    return detail

PersistLive = Callable[..., Awaitable[Any]]
MarkDone = Callable[..., Awaitable[Any]]
ListDbSessions = Callable[..., Awaitable[Sequence[Any]]]

LEASE_GRACE_S = LEASE_HEARTBEAT_INTERVAL_S * LEASE_MISS_LIMIT

#: How long a lease subscription waits before resubscribing after a transport
#: failure. Well inside :data:`LEASE_GRACE_S`: reconnecting must not spend so
#: much of the grace window that a still-live STS reads as expired.
RESUBSCRIBE_DELAY_S = 0.5

#: How many consecutive scans must agree before a row this instance owns
#: and does not hold locally is closed. Covers the window between persist
#: and the link landing in ``_links``.
_ORPHAN_STRIKES = 2

#: How many live rows one reap scan will consider. Well above any plausible
#: number of concurrent attaches, and named so the scan can say when it hit
#: the limit rather than truncating in silence.
_REAP_SCAN_LIMIT = 500

#: How long a failed symbol-plane read waits before the next try. A
#: dated book that attached during a brief outage must still be cut
#: at settlement; treating the failure as "no expiry" would leave
#: the pump up. Capped at 30s by the retry loop.
_LOOKUP_RETRY_S = 1.0


class _SymbolLookupError(Exception):
    """The symbol plane did not answer. Expiry is unknown, not absent."""


@dataclass
class StsLink:
    """One STS session attached to MD."""

    session_id: str
    created_by: int
    subscriptions: set[str] = field(default_factory=set)
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    last_token: int = 0
    #: Tail of the subscribe/unsubscribe chain for this link. Runtime
    #: subscribe waits on a symbol-plane read; unsubscribe must not
    #: overtake it or a quick sub-then-unsub leaves the feed up.
    feed_op: asyncio.Task[Any] | None = None


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
        recorder: TapeRecorder | None = None,
        instance: str = SessionDomain.MD.value,
        symbols: SymbolClient | None = None,
        expiry_lookup_retry_s: float = _LOOKUP_RETRY_S,
    ) -> None:
        self._factory = factory
        self._broker = broker
        #: Symbol plane, for listed settlement times. None leaves every
        #: feed running until detach — tests and a process without the
        #: plane never cut on expiry.
        self._symbols = symbols
        #: Which MD this is. Defaults to the plane name, which is what
        #: ``MFTIK_INSTANCE`` resolves to when nobody has set it and what
        #: migration 0031 declares.
        self._instance = instance
        self._persist_live = persist_live
        self._mark_done = mark_done
        self._list_db_sessions = list_db_sessions
        self._lease_grace = lease_grace
        #: Records the trade feeds so a strategy starting later can warm up.
        #: None leaves every feed unrecorded — MD serves live data exactly as
        #: before, and a warm-up simply finds nothing.
        self._recorder = recorder
        self._dispatcher = Dispatcher(broker, recorder=recorder)
        self._venues: dict[str, VenueSession] = {}
        self._links: dict[str, StsLink] = {}
        #: ``(instance, session_id)`` → consecutive scans that found no
        #: local link (or a link whose lease tasks have died). Keyed by the
        #: pair because one session can have rows from several instances and
        #: each is decided separately. See :data:`_ORPHAN_STRIKES`.
        self._orphan_strikes: dict[tuple[str, str], int] = {}
        #: Venue disconnects still running. Held so :meth:`close_all` can wait
        #: for them and a stray task cannot be garbage-collected mid-close.
        self._disconnects: set[asyncio.Task[Any]] = set()
        #: Per-instrument sleep until listed settlement. Cancelled when
        #: the last reader leaves before expiry, or when the cut runs.
        self._expiry_tasks: dict[UniversalTicker, asyncio.Task[Any]] = {}
        #: Instruments that have already been cut. ``ensure_feed`` is
        #: not called again for these; a later subscribe is notified
        #: and refused.
        self._expired: set[UniversalTicker] = set()
        self._expiry_at: dict[UniversalTicker, float] = {}
        #: Confirmed no listed settlement (spot, perp, unknown symbol).
        #: Stops a later subscribe from hitting the plane again.
        self._timeless: set[UniversalTicker] = set()
        self._resolve_waits: dict[
            UniversalTicker, asyncio.Future[float | None]
        ] = {}
        self._expiry_lock = asyncio.Lock()
        self._lookup_retry_s = expiry_lookup_retry_s
        #: Runtime ``md.subscribe`` work. Held so shutdown can cancel a
        #: lookup / ``ensure_feed`` that is no longer on the lease loop.
        self._subscribe_tasks: set[asyncio.Task[Any]] = set()
        #: The in-flight open of a key, and the attach sessions waiting
        #: on it. A joiner that does not wait (a runtime subscribe) is
        #: told with ``md.feed.end`` if the open fails. An attach that
        #: is still waiting fails its own RPC instead.
        self._opening: dict[FeedKey, asyncio.Future[AttachError | None]] = {}
        self._opening_waiters: dict[FeedKey, set[str]] = {}

    @property
    def dispatcher(self) -> Dispatcher:
        return self._dispatcher

    def feed_refcount(self, feed: str) -> int:
        return self._dispatcher.refcount(*Topics.parse_md_feed(feed))

    @property
    def tape_store(self) -> TapeStore | None:
        """This region's tape disk, or None when recording is off."""
        if self._recorder is None:
            return None
        return self._recorder.store

    async def trim_tapes(self) -> None:
        """Apply the retention window to every feed currently recording.

        Only the live ones need it. A feed nobody holds has stopped growing, so
        it can only shrink from here — and the key's TTL reclaims it outright
        once it is older than any warm-up would want.
        """
        if self._recorder is None:
            return
        await self._recorder.trim(self._dispatcher.refcounts())

    async def attach(self, request: MdAttachRequest) -> MdAttachResult:
        """Attach STS ``session_id`` with subscriptions (lease + refcount)."""
        existing = self._links.get(request.session_id)
        if existing is not None:
            return MdAttachResult(
                session_id=request.session_id,
                subscriptions=sorted(existing.subscriptions),
                refcounts=self._dispatcher.refcounts(),
                instance=self._instance,
            )

        # Before the lease. A key that does not parse is a bad request,
        # not a feed that ended: there is no ticker to put on ``FeedEnd``,
        # and failing after the link is stored lets the STS retry succeed
        # with the feed quietly missing.
        parsed: list[tuple[str, str, UniversalTicker]] = []
        try:
            for feed in request.subscriptions:
                topic, ticker = Topics.parse_md_feed(feed)
                parsed.append((feed, topic, ticker))
        except ValueError as exc:
            raise AttachError("invalid_feed", str(exc)) from exc

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

        missing, failed = await self._prefetch_expiries(request.subscriptions)
        if missing:
            names = ", ".join(str(ticker) for ticker in sorted(missing, key=str))
            await self.detach(
                session_id=request.session_id, reason="symbol not found"
            )
            raise AttachError(
                QueryCode.VENUE_SYMBOL_NOT_FOUND.name,
                f"symbol not found: {names}",
            )
        if failed:
            detail = "; ".join(
                f"{ticker}: {exc}"
                for ticker, exc in sorted(failed.items(), key=lambda item: str(item[0]))
            )
            await self.detach(
                session_id=request.session_id, reason="symbol lookup failed"
            )
            raise AttachError(
                QueryCode.MD_INTERNAL.name,
                f"symbol lookup failed: {detail}",
            )
        opened: set[UniversalTicker] = set()
        now = time.time()
        try:
            for feed, topic, ticker in parsed:
                listed = self._expiry_at.get(ticker)
                if ticker in self._expired or (
                    listed is not None and listed <= now
                ):
                    if ticker not in self._expired and listed is not None:
                        await self._expire_ticker(ticker, listed)
                    expiry = self._expiry_at[ticker]
                    await self._emit_feed_end(
                        [request.session_id],
                        ticker,
                        topic=topic,
                        state="expired",
                        code="expired",
                        reason=f"instrument expired at {expiry}",
                        expiry=expiry,
                    )
                    continue
                await self._subscribe_feed(
                    link, feed, arm=False, emit_failure=False
                )
                if feed in link.subscriptions:
                    opened.add(ticker)
        except AttachError:
            await self.detach(
                session_id=request.session_id, reason="attach refused"
            )
            raise
        for ticker in opened:
            self._schedule_arm(ticker)

        feeds = sorted(link.subscriptions)
        venues = sorted(_venues_from_feeds(feeds))
        if self._persist_live is not None:
            await self._persist_live(
                instance=self._instance,
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
                instance=self._instance,
            )
        return MdAttachResult(
            session_id=request.session_id,
            subscriptions=feeds,
            refcounts=self._dispatcher.refcounts(),
            instance=self._instance,
        )

    async def detach(
        self, *, session_id: str, reason: str = "detach"
    ) -> None:
        link = self._links.pop(session_id, None)
        if link is None:
            return
        changed = self._dispatcher.unsubscribe_all(session_id)
        venues: set[str] = set()
        tickers: set[UniversalTicker] = set()
        for (topic, ticker), old_rc, new_rc in changed:
            venues.add(ticker.venue)
            tickers.add(ticker)
            feed = Topics.md_feed(topic, ticker)
            await publish_md_log(
                self._broker,
                ticker.venue,
                (
                    f"refcount {feed} {old_rc}→{new_rc} "
                    f"(sts={session_id} detach reason={reason})"
                ),
                source="md",
                instance=self._instance,
            )
            if new_rc == 0:
                await self._stop_feed_if_unused((topic, ticker))
        for ticker in tickers:
            self._disarm_if_idle(ticker)
        await self._stop_link(link)
        if self._mark_done is not None:
            await self._mark_done(
                session_id=session_id, instance=self._instance
            )
        logger.info(
            "MD detached session=%s reason=%s", session_id, reason
        )
        for venue in venues or _venues_from_feeds(link.subscriptions):
            await publish_md_log(
                self._broker,
                venue,
                f"sts detached session={session_id} reason={reason}",
                source="md",
                instance=self._instance,
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

    async def reap_orphans(self) -> list[str]:
        """Close rows left ``live`` by an MD process that died silently.

        A row ends in :meth:`detach`, and every way into it needs this
        process running: a lease that expires, an ``MD_DETACH``, a shutdown.
        Kill MD outright and none of them happen, so the row goes on
        claiming a venue feed that no longer exists — and because
        :meth:`list_sessions` answers from the table, the API reports a feed
        nobody is pumping.

        A row is an orphan when it names this instance and this process
        does not have the link — or the lease tasks have died. Another
        instance's rows are left for that instance. Strikes cover the
        window between persist and ``_links``.

        Returns the session ids reaped, for logging and tests.
        """
        if self._list_db_sessions is None or self._mark_done is None:
            return []
        try:
            rows = await self._list_db_sessions(
                status=SessionStatus.LIVE.value,
                created_by=None,
                limit=_REAP_SCAN_LIMIT,
            )
        except Exception:
            logger.exception("MD orphan scan failed to list sessions")
            return []
        if len(rows) >= _REAP_SCAN_LIMIT:
            # Truncation is the one thing a scan must not do quietly: the
            # rows past the limit look exactly like rows with a live owner.
            logger.warning(
                "MD orphan scan hit its %d-row limit — there may be live "
                "rows it did not consider",
                _REAP_SCAN_LIMIT,
            )

        reaped: list[str] = []
        seen: set[tuple[str, str]] = set()
        for row in rows:
            session_id = getattr(row, "session_id", None)
            if session_id is None:
                continue
            # Whose row this is, not whose scan this is. The scan stays global
            # on purpose — an instance that dies outright leaves rows only some
            # *other* process can notice, and noticing them is what this loop
            # is for. What must not happen is deciding a peer's row against our
            # own liveness key: a perfectly healthy md-jp-2 would then be
            # closed by every scan md-jp-1 runs.
            instance = getattr(row, "instance", None) or self._instance
            if instance != self._instance:
                continue
            key = (instance, session_id)
            if key in seen:
                continue
            seen.add(key)
            link = self._links.get(session_id)
            if link is not None and not any(t.done() for t in link.tasks):
                self._orphan_strikes.pop(key, None)
                continue
            strikes = self._orphan_strikes.get(key, 0) + 1
            self._orphan_strikes[key] = strikes
            if strikes < _ORPHAN_STRIKES:
                continue
            if link is not None:
                logger.warning(
                    "MD reaping orphaned link session=%s", session_id
                )
                try:
                    await self.detach(
                        session_id=session_id, reason="lease_loop_died"
                    )
                except Exception:
                    logger.exception(
                        "MD orphan detach failed session=%s", session_id
                    )
                    continue
                reaped.append(session_id)
                continue

            # `done`, not `interrupted`: an md row follows its owning
            # strategy session rather than carrying an outcome of its own,
            # and nothing rebuilds from one — STS re-attaching on rebuild is
            # what writes the row live again.
            try:
                await self._mark_done(
                    session_id=session_id, instance=instance
                )
            except Exception:
                logger.exception(
                    "MD orphan reap failed instance=%s session=%s",
                    instance,
                    session_id,
                )
                continue
            reaped.append(session_id)
            logger.warning(
                "MD reaped orphaned session id=%s venue=%s",
                session_id,
                getattr(row, "venue", None),
            )
        self._orphan_strikes = {
            key: strikes
            for key, strikes in self._orphan_strikes.items()
            if key in seen
        }
        return reaped

    async def close_all(self) -> None:
        for task in list(self._subscribe_tasks):
            task.cancel()
        if self._subscribe_tasks:
            await asyncio.gather(
                *self._subscribe_tasks, return_exceptions=True
            )
        self._subscribe_tasks.clear()
        for task in list(self._expiry_tasks.values()):
            task.cancel()
        if self._expiry_tasks:
            await asyncio.gather(
                *self._expiry_tasks.values(), return_exceptions=True
            )
        self._expiry_tasks.clear()
        for session_id in list(self._links):
            await self.detach(session_id=session_id, reason="shutdown")
        for venue in list(self._venues):
            self._destroy_venue(venue)
        # Waited for here and nowhere else. A detach hands the disconnect to a
        # task because nothing is blocked on it; a shutdown is the one moment
        # something is — the process is about to end, and a socket closed by
        # exit rather than by handshake is one the venue has to time out.
        if self._disconnects:
            await asyncio.gather(*self._disconnects, return_exceptions=True)
        if self._recorder is not None:
            await self._recorder.aclose()

    def _enqueue_feed_op(
        self, link: StsLink, op: Callable[[], Awaitable[None]]
    ) -> None:
        """Run ``op`` after earlier subscribe/unsubscribe on this link.

        Runtime subscribe waits on the symbol plane. Unsubscribe is
        otherwise immediate, so a sub-then-unsub would apply backwards
        and leave the feed up. One chain per session keeps the order.
        """
        prev = link.feed_op

        async def _run() -> None:
            if prev is not None:
                await asyncio.gather(prev, return_exceptions=True)
            if self._links.get(link.session_id) is not link:
                return
            await op()

        task = asyncio.create_task(
            _run(), name=f"md-feed-op-{link.session_id}"
        )
        link.feed_op = task
        self._subscribe_tasks.add(task)
        task.add_done_callback(self._subscribe_tasks.discard)

    async def _subscribe_runtime(self, link: StsLink, feed: str) -> None:
        try:
            await self._subscribe_feed(link, feed, arm=True)
        except Exception:
            logger.exception(
                "MD subscribe failed session=%s feed=%s",
                link.session_id,
                feed,
            )

    async def _unsubscribe_runtime(self, link: StsLink, feed: str) -> None:
        try:
            await self._unsubscribe_feed(link, feed)
        except Exception:
            logger.exception(
                "MD unsubscribe failed session=%s feed=%s",
                link.session_id,
                feed,
            )

    async def _subscribe_feed(
        self,
        link: StsLink,
        feed: str,
        *,
        arm: bool = True,
        emit_failure: bool = True,
    ) -> None:
        topic, ticker = Topics.parse_md_feed(feed)
        if arm:
            try:
                listed = await self._try_resolve(ticker)
            except SymbolNotFoundError as exc:
                await self._emit_feed_end(
                    [link.session_id],
                    ticker,
                    topic=topic,
                    state="down",
                    code="symbol_not_found",
                    reason=_exc_reason(exc),
                )
                return
            if self._links.get(link.session_id) is not link:
                return
            if listed is not None and listed <= time.time():
                if ticker not in self._expired:
                    await self._expire_ticker(ticker, listed)
                await self._emit_feed_end(
                    [link.session_id],
                    ticker,
                    topic=topic,
                    state="expired",
                    code="expired",
                    reason=f"instrument expired at {listed}",
                    expiry=listed,
                )
                return
        first = False
        old_rc = 0
        new_rc = 0
        pending: asyncio.Future[AttachError | None] | None = None
        async with self._expiry_lock:
            if self._links.get(link.session_id) is not link:
                return
            if ticker in self._expired:
                expiry = self._expiry_at[ticker]
            else:
                expiry = None
                key = (topic, ticker)
                first, new_rc = self._dispatcher.subscribe(
                    link.session_id, topic, ticker
                )
                old_rc = new_rc - 1
                link.subscriptions.add(feed)
                if first:
                    pending = asyncio.get_running_loop().create_future()
                    self._opening[key] = pending
                else:
                    pending = self._opening.get(key)
                    if pending is not None and not emit_failure:
                        self._opening_waiters.setdefault(key, set()).add(
                            link.session_id
                        )
        if expiry is not None:
            await self._emit_feed_end(
                [link.session_id],
                ticker,
                topic=topic,
                state="expired",
                code="expired",
                reason=f"instrument expired at {expiry}",
                expiry=expiry,
            )
            return
        try:
            await self._open_subscribed(
                link,
                feed,
                topic,
                ticker,
                first=first,
                old_rc=old_rc,
                new_rc=new_rc,
                emit_failure=emit_failure,
                pending=pending,
            )
        except AttachError:
            raise
        except Exception as exc:
            logger.exception(
                "MD subscribe failed session=%s feed=%s",
                link.session_id,
                feed,
            )
            await self._refuse(
                topic,
                ticker,
                QueryCode.MD_INTERNAL.name,
                _exc_reason(exc),
                emit=emit_failure,
                state="down",
                feed_code="error",
                session_id=link.session_id,
            )
        if arm and feed in link.subscriptions:
            self._schedule_arm(ticker)

    async def _open_subscribed(
        self,
        link: StsLink,
        feed: str,
        topic: str,
        ticker: UniversalTicker,
        *,
        first: bool,
        old_rc: int,
        new_rc: int,
        emit_failure: bool = True,
        pending: asyncio.Future[AttachError | None] | None = None,
    ) -> None:
        """Finish a subscribe that already holds a refcount.

        Connect and ``_open`` failures retire every subscriber of the
        key — another session may have joined during the awaits.
        A runtime subscribe is told with ``md.feed.end``. An attach
        that is still waiting fails its own RPC. One whose reply
        already went out is told with ``md.feed.end``, because that
        reply listed a feed that never opened.
        """
        await publish_md_log(
            self._broker,
            ticker.venue,
            (
                f"refcount {feed} {old_rc}→{new_rc} "
                f"(sts={link.session_id} subscribe)"
            ),
            source="md",
        )
        if not first:
            if pending is not None and not emit_failure:
                outcome = await pending
                if isinstance(outcome, AttachError):
                    raise AttachError(outcome.code, str(outcome))
            return
        try:
            await self._open_first(
                link,
                feed,
                topic,
                ticker,
                emit_failure=emit_failure,
            )
        finally:
            self._finish_opening((topic, ticker), None)

    async def _open_first(
        self,
        link: StsLink,
        feed: str,
        topic: str,
        ticker: UniversalTicker,
        *,
        emit_failure: bool,
    ) -> None:
        if (
            ticker in self._expired
            or self._dispatcher.refcount(topic, ticker) == 0
        ):
            return
        try:
            venue_sess = await self._ensure_venue(ticker.venue)
        except Exception as exc:
            logger.exception(
                "MD venue connect failed venue=%s", ticker.venue
            )
            await self._refuse(
                topic,
                ticker,
                QueryCode.MD_VENUE_NOT_CONNECTED.name,
                _exc_reason(exc),
                emit=emit_failure,
                state="down",
                feed_code="connect",
                session_id=link.session_id,
            )
            return
        if (
            ticker in self._expired
            or self._dispatcher.refcount(topic, ticker) == 0
        ):
            if (
                venue_sess.feed_count == 0
                and self._venues.get(ticker.venue) is venue_sess
            ):
                self._destroy_venue(ticker.venue)
            return
        try:
            await venue_sess.ensure_feed(topic, ticker)
        except Exception as exc:
            if isinstance(exc, ValueError):
                logger.warning(
                    "MD feed rejected topic=%s ticker=%s: %s",
                    topic,
                    ticker,
                    exc,
                )
                code = QueryCode.MD_VENUE_UNSUPPORTED_READ.name
                feed_code = "unsupported"
                state = "rejected"
            elif isinstance(exc, SymbolNotFoundError):
                logger.warning(
                    "MD feed symbol not found topic=%s ticker=%s: %s",
                    topic,
                    ticker,
                    exc,
                )
                code = QueryCode.VENUE_SYMBOL_NOT_FOUND.name
                feed_code = "symbol_not_found"
                state = "down"
            else:
                logger.exception(
                    "MD feed rejected topic=%s ticker=%s", topic, ticker
                )
                code = QueryCode.VENUE_REJECTED.name
                feed_code = "unsupported"
                state = "rejected"
            await self._refuse(
                topic,
                ticker,
                code,
                _exc_reason(exc),
                emit=emit_failure,
                state=state,
                feed_code=feed_code,
                session_id=link.session_id,
            )
            return
        if (
            ticker in self._expired
            or self._dispatcher.refcount(topic, ticker) == 0
        ):
            await self._stop_feed_if_unused((topic, ticker))
            # Expiry may already have dropped this venue. The pump then
            # lives on the session ``ensure_feed`` just used, not on
            # ``_venues``, and still has to be stopped.
            if venue_sess.has_feed(topic, ticker):
                await venue_sess.stop_feed(topic, ticker)
            return
        # Stamped here rather than on the first record: this is the moment
        # continuity broke, and a feed that starts pumping into a silent
        # market would otherwise look like it had been recording all along.
        if self._recorder is not None and self._recorder.records(topic):
            await self._recorder.started(feed)
        await publish_md_log(
            self._broker,
            ticker.venue,
            f"feed pump started {feed}",
            source="md",
            instance=self._instance,
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
        self._disarm_if_idle(ticker)

    async def _prefetch_expiries(
        self, feeds: Sequence[str]
    ) -> tuple[set[UniversalTicker], dict[UniversalTicker, BaseException]]:
        """Resolve listed expiries.

        The first set is tickers the plane does not know. A miss is
        not cached. The second is tickers whose lookup failed for any
        other reason: attach fails rather than opening a pump that
        later reports ``symbol_not_found``.
        """
        missing: set[UniversalTicker] = set()
        failed: dict[UniversalTicker, BaseException] = {}
        if self._symbols is None:
            return missing, failed
        seen: list[UniversalTicker] = []
        for feed in feeds:
            try:
                _topic, ticker = Topics.parse_md_feed(feed)
            except ValueError:
                continue
            if ticker not in seen:
                seen.append(ticker)
        if not seen:
            return missing, failed
        results = await asyncio.gather(
            *(self._resolve_expiry(ticker) for ticker in seen),
            return_exceptions=True,
        )
        for ticker, result in zip(seen, results, strict=True):
            if isinstance(result, SymbolNotFoundError):
                missing.add(ticker)
            elif isinstance(result, Exception):
                logger.warning(
                    "MD symbol lookup failed ticker=%s: %s",
                    ticker,
                    result,
                )
                failed[ticker] = result
        return missing, failed

    async def _try_resolve(self, ticker: UniversalTicker) -> float | None:
        if self._symbols is None or ticker in self._timeless:
            return None
        if ticker in self._expiry_at:
            return self._expiry_at[ticker]
        try:
            return await self._resolve_expiry(ticker)
        except _SymbolLookupError:
            logger.warning("MD symbol lookup failed ticker=%s", ticker)
            return None

    async def _resolve_expiry(self, ticker: UniversalTicker) -> float | None:
        if self._symbols is None:
            self._timeless.add(ticker)
            return None
        loop = asyncio.get_running_loop()
        async with self._expiry_lock:
            if ticker in self._timeless:
                return None
            if ticker in self._expiry_at:
                return self._expiry_at[ticker]
            waiter = self._resolve_waits.get(ticker)
            mine = False
            if waiter is None:
                waiter = loop.create_future()
                self._resolve_waits[ticker] = waiter
                mine = True
        if not mine:
            return await waiter
        try:
            info = await self._symbols.get(ticker, include_inactive=True)
        except SymbolNotFoundError as exc:
            # Not timeless. A later subscribe has to be able to arm the
            # watch once the instrument exists.
            if not waiter.done():
                waiter.set_exception(exc)
                waiter.add_done_callback(_sink_future_exception)
            raise
        except Exception as exc:
            err = _SymbolLookupError(str(exc))
            if not waiter.done():
                waiter.set_exception(err)
                waiter.add_done_callback(_sink_future_exception)
            raise err from exc
        else:
            # Inactive rows are included: a settled option is deactivated
            # on the hourly refresh, and its expiry is how we still cut
            # it instead of treating the miss as "no listed time".
            expiry = info.expiry
            if expiry is None:
                self._timeless.add(ticker)
            else:
                self._expiry_at[ticker] = expiry
            if not waiter.done():
                waiter.set_result(expiry)
            return expiry
        finally:
            if self._resolve_waits.get(ticker) is waiter:
                self._resolve_waits.pop(ticker, None)

    def _schedule_arm(self, ticker: UniversalTicker) -> None:
        if self._symbols is None:
            return
        if ticker in self._expired or ticker in self._timeless:
            return
        if ticker in self._expiry_tasks:
            return
        self._expiry_tasks[ticker] = asyncio.create_task(
            self._run_expiry(ticker),
            name=f"md-expiry-{ticker}",
        )

    async def _run_expiry(self, ticker: UniversalTicker) -> None:
        try:
            expiry = await self._expiry_or_retry(ticker)
            if expiry is None:
                return
            delay = expiry - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            await self._expire_ticker(ticker, expiry)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MD expiry watch failed ticker=%s", ticker)
        finally:
            current = asyncio.current_task()
            if self._expiry_tasks.get(ticker) is current:
                self._expiry_tasks.pop(ticker, None)

    async def _expiry_or_retry(self, ticker: UniversalTicker) -> float | None:
        delay = self._lookup_retry_s
        while True:
            if ticker in self._expired:
                return self._expiry_at.get(ticker)
            if ticker in self._timeless:
                return None
            if ticker in self._expiry_at:
                return self._expiry_at[ticker]
            try:
                return await self._resolve_expiry(ticker)
            except (_SymbolLookupError, SymbolNotFoundError):
                logger.warning(
                    "MD symbol lookup failed ticker=%s; retry in %.1fs",
                    ticker,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _expire_ticker(
        self, ticker: UniversalTicker, expiry: float
    ) -> None:
        async with self._expiry_lock:
            if ticker in self._expired:
                return
            keys = self._dispatcher.feeds_for_ticker(ticker)
            held: dict[str, list[str]] = {}
            released: list[Feed] = []
            topics = sorted({topic for topic, _tk in keys})
            self._expired.add(ticker)
            self._expiry_at[ticker] = expiry
            task = self._expiry_tasks.pop(ticker, None)
            if task is not None and task is not asyncio.current_task():
                task.cancel()
            for topic, tk in keys:
                session_ids, feed = self._drop_key_locked(topic, tk, cancel=True)
                for session_id in session_ids:
                    held.setdefault(session_id, []).append(topic)
                if feed is not None:
                    released.append(feed)
        for feed in released:
            if feed.task is not None and feed.task is not asyncio.current_task():
                await asyncio.gather(feed.task, return_exceptions=True)
        for topic, _tk in keys:
            await self._stamp_stopped(topic, ticker)
        self._disarm_if_idle(ticker)
        venue = self._venues.get(ticker.venue)
        if venue is not None and venue.feed_count == 0:
            self._destroy_venue(ticker.venue)
        reason = f"instrument expired at {expiry}"
        for session_id, session_topics in held.items():
            for topic in sorted(session_topics):
                await self._emit_feed_end(
                    [session_id],
                    ticker,
                    topic=topic,
                    state="expired",
                    code="expired",
                    reason=reason,
                    expiry=expiry,
                )
        await publish_md_log(
            self._broker,
            ticker.venue,
            (
                f"instrument expired {ticker} topics={topics} "
                f"sessions={sorted(held)}"
            ),
            source="md",
            instance=self._instance,
        )

    async def _on_feed_end(
        self,
        session: VenueSession,
        feed: Feed,
        state: str,
        code: str,
        reason: str,
    ) -> None:
        """Pump task finished on its own. Retire the key, then tell subscribers.

        Only this key. A sibling still in ``_feeds`` is left alone: it is
        either a healthy feed on another socket of the same venue, or a
        pump that has not left ``on_update`` yet. Cancelling it would
        skip its notify and leave the refcount. The connector is dropped
        only once this session itself has no feeds left, and only if it
        is still the session mapped for the venue.
        """
        targets = await self._retire_key(
            (feed.topic, feed.ticker),
            stop_pump=False,
            owner=session,
        )
        if targets is None:
            return
        await self._emit_feed_end(
            targets,
            feed.ticker,
            topic=feed.topic,
            state=state,
            code=code,
            reason=reason,
        )

    def _finish_opening(
        self, key: FeedKey, result: AttachError | None
    ) -> None:
        """Release attach sessions waiting on this open. Once only."""
        fut = self._opening.pop(key, None)
        if fut is None:
            return
        self._opening_waiters.pop(key, None)
        if not fut.done():
            fut.set_result(result)

    async def _refuse(
        self,
        topic: str,
        ticker: UniversalTicker,
        code: str,
        reason: str,
        *,
        emit: bool,
        state: str,
        feed_code: str,
        session_id: str,
    ) -> None:
        """Drop a key that never started pumping.

        Sessions still inside their attach wait on the open and fail
        that RPC. Anyone else who already holds the key — a runtime
        subscribe, or an attach that already answered — is told with
        ``md.feed.end``.
        """
        err = AttachError(code, reason)
        key = (topic, ticker)
        async with self._expiry_lock:
            waiters = self._opening_waiters.pop(key, set())
            fut = self._opening.pop(key, None)
        if fut is not None and not fut.done():
            fut.set_result(err)
        targets = await self._retire_key(key, stop_pump=True) or []
        if emit:
            notify = [sid for sid in targets if sid not in waiters]
        else:
            notify = [
                sid
                for sid in targets
                if sid not in waiters and sid != session_id
            ]
        if notify:
            await self._emit_feed_end(
                notify,
                ticker,
                topic=topic,
                state=state,
                code=feed_code,
                reason=reason,
            )
        if not emit:
            raise err

    def _drop_key_locked(
        self,
        topic: str,
        ticker: UniversalTicker,
        *,
        cancel: bool,
        owner: VenueSession | None = None,
    ) -> tuple[list[str], Feed | None]:
        """Clear one key. Caller holds ``_expiry_lock`` and must not await.

        Returns the sessions that held it, and the feed if one was still
        in ``_feeds`` (already asked to stop when ``cancel`` is set).

        ``owner`` is the session the pump ran on. Releasing on whatever
        is currently in ``_venues`` would cancel a feed a newer session
        has already opened for the same key.
        """
        session_ids = list(self._dispatcher.subscribers(topic, ticker))
        for session_id in session_ids:
            self._dispatcher.unsubscribe(session_id, topic, ticker)
            link = self._links.get(session_id)
            if link is not None:
                link.subscriptions.discard(Topics.md_feed(topic, ticker))
        venue = owner if owner is not None else self._venues.get(ticker.venue)
        released = None
        if venue is not None:
            released = venue.release_feed(topic, ticker, cancel=cancel)
        return session_ids, released

    async def _retire_key(
        self,
        key: FeedKey,
        *,
        stop_pump: bool,
        only_if_unused: bool = False,
        owner: VenueSession | None = None,
    ) -> list[str] | None:
        """Drop subscribers and the feed before any further await can subscribe.

        ``stop_pump`` awaits a task this call cancelled. A pump reporting
        its own end passes False: it has already popped itself, and
        waiting on that task would deadlock.

        ``only_if_unused`` leaves the key alone when a subscriber arrived
        after the caller decided it was idle. Returns None in that case.
        The check is inside the lock: a refcount read before the lock can
        go stale, and dropping that new subscriber would not tell them.
        """
        topic, ticker = key
        async with self._expiry_lock:
            if only_if_unused and self._dispatcher.refcount(topic, ticker) > 0:
                return None
            session_ids, released = self._drop_key_locked(
                topic, ticker, cancel=stop_pump, owner=owner
            )
        if (
            stop_pump
            and released is not None
            and released.task is not None
            and released.task is not asyncio.current_task()
        ):
            await asyncio.gather(released.task, return_exceptions=True)
        await self._stamp_stopped(topic, ticker)
        self._disarm_if_idle(ticker)
        # Drop the connector only when the session that owned this key
        # is idle, and only if a later subscribe has not already replaced
        # it. A transport end does not take the venue down while another
        # feed is still in ``_feeds``.
        current = self._venues.get(ticker.venue)
        idle = owner if owner is not None else current
        if idle is not None and current is idle and idle.feed_count == 0:
            self._destroy_venue(ticker.venue)
        return session_ids

    async def _stamp_stopped(self, topic: str, ticker: UniversalTicker) -> None:
        # The tape is left where it is. Two hours of history does not stop
        # being true because nobody is subscribed any more — it stops being
        # *current*, and saying so is what this stamp is for.
        if self._recorder is not None and self._recorder.records(topic):
            await self._recorder.stopped(Topics.md_feed(topic, ticker))

    async def _emit_feed_end(
        self,
        session_ids: Sequence[str],
        ticker: UniversalTicker,
        *,
        topic: str,
        state: str,
        code: str,
        reason: str,
        expiry: float | None = None,
    ) -> None:
        """Publish one ``md.feed.end`` to each session whose lease is up."""
        env = UntypedEnvelope.wrap(
            FeedEnd(
                universal_ticker=str(ticker),
                topic=topic,
                state=state,
                code=code,
                reason=reason,
                expiry=expiry,
            ).model_dump(mode="json"),
            type=MD_FEED_END,
            source="md",
        )
        for session_id in session_ids:
            if session_id not in self._links:
                continue
            try:
                await self._broker.publish(Topics.md_session(session_id), env)
            except Exception:
                logger.exception(
                    "MD feed end notify failed session=%s topic=%s ticker=%s",
                    session_id,
                    topic,
                    ticker,
                )

    def _disarm_if_idle(self, ticker: UniversalTicker) -> None:
        if self._dispatcher.feeds_for_ticker(ticker):
            return
        task = self._expiry_tasks.pop(ticker, None)
        if task is not None:
            task.cancel()

    async def _ensure_venue(self, venue: str) -> VenueSession:
        existing = self._venues.get(venue)
        if existing is not None:
            return existing
        public = await self._factory.create(venue)
        sess = VenueSession(
            venue,
            public,
            on_update=self._dispatcher.publish,
            on_end=self._on_feed_end,
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
        retired = await self._retire_key(
            key, stop_pump=True, only_if_unused=True
        )
        if retired is None:
            return
        await publish_md_log(
            self._broker,
            ticker.venue,
            f"feed pump stopped {Topics.md_feed(topic, ticker)} (refcount 0)",
            source="md",
        )

    def _destroy_venue(self, venue: str) -> None:
        """Drop the venue and disconnect it in the background.

        The pop is what matters and it is synchronous: from here on nothing can
        reach this session, and an attach arriving a moment later builds a
        fresh one rather than waiting behind a closing socket.

        Disconnecting is the slow part and nobody is waiting on it. A venue
        socket takes seconds to close — the server may never answer the close
        frame, and there is one connection per traffic class to get through —
        and this used to run inline on the RPC loop, which serves one request
        at a time. A detach therefore held every other attach, list and detach
        behind a websocket handshake with a venue nobody was subscribed to any
        more.
        """
        sess = self._venues.pop(venue, None)
        if sess is None:
            return
        task = asyncio.create_task(
            self._disconnect_venue(venue, sess), name=f"md-disconnect-{venue}"
        )
        self._disconnects.add(task)
        task.add_done_callback(self._disconnects.discard)

    async def _disconnect_venue(self, venue: str, sess: VenueSession) -> None:
        try:
            await sess.stop()
        except Exception:
            # Nothing to recover: the session is already unreachable, and the
            # connection dies with the process at worst.
            logger.exception("MD venue disconnect failed venue=%s", venue)
            return
        try:
            await publish_md_log(
                self._broker,
                venue,
                "venue public client disconnected",
                source="md",
                instance=self._instance,
            )
        except Exception:
            logger.exception("MD venue disconnect log failed venue=%s", venue)

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

        async def _on_heartbeat(hb: LeaseHeartbeat) -> None:
            link.last_token = hb.token

        async def _on_message(env: Any) -> bool:
            if env.type == MD_SUBSCRIBE:
                try:
                    msg = MdSubscribe.model_validate(env.payload)
                except Exception:
                    return False
                if msg.instance is not None and msg.instance != self._instance:
                    return False
                if msg.session_id != link.session_id:
                    return False
                feed = msg.feed
                self._enqueue_feed_op(
                    link, lambda: self._subscribe_runtime(link, feed)
                )
                return False
            if env.type == MD_UNSUBSCRIBE:
                try:
                    msg = MdUnsubscribe.model_validate(env.payload)
                except Exception:
                    return False
                if msg.session_id != link.session_id:
                    return False
                feed = msg.feed
                self._enqueue_feed_op(
                    link, lambda: self._unsubscribe_runtime(link, feed)
                )
                return False
            if env.type == MD_DETACH:
                try:
                    det = MdDetach.model_validate(env.payload)
                except Exception:
                    return False
                if det.session_id == link.session_id:
                    await self.detach(
                        session_id=link.session_id,
                        reason="sts_stop",
                    )
                    return True
            return False

        async def _expire() -> None:
            if self._links.get(link.session_id) is link:
                await self.detach(
                    session_id=link.session_id,
                    reason="lease_expired",
                )

        async def _died() -> None:
            if self._links.get(link.session_id) is link:
                await self.detach(
                    session_id=link.session_id,
                    reason="lease_loop_died",
                )

        def _ack(hb: LeaseHeartbeat) -> Envelope[MdLeaseAck]:
            return Envelope[MdLeaseAck].wrap(
                MdLeaseAck(
                    session_id=link.session_id,
                    token=hb.token,
                    instance=self._instance,
                ),
                type=MD_LEASE_ACK,
                source="md",
                session_id=link.session_id,
            )

        await LeasedSessionLink(
            self._broker,
            rx=Topics.sts_md_session(link.session_id),
            tx=Topics.md_session(link.session_id),
            stop=link.stop,
            grace=self._lease_grace,
            ready=ready,
            ack=_ack,
            on_heartbeat=_on_heartbeat,
            on_message=_on_message,
            on_expired=_expire,
            on_died=_died,
            resubscribe_delay=RESUBSCRIBE_DELAY_S,
            name=f"md-lease-{link.session_id}",
        ).run()


def _venues_from_feeds(feeds: set[str] | list[str]) -> set[str]:
    venues: set[str] = set()
    for feed in feeds:
        try:
            _topic, ticker = Topics.parse_md_feed(feed)
        except ValueError:
            continue
        venues.add(ticker.venue)
    return venues

"""Session manager — trading sessions + STS attach with fencing lease."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from functools import partial
from typing import Any

from mft.broker import Broker, IncomingRequest
from mft.exchange.errors import ExchangeError
from mft.exchange.models import (
    Order,
    OrderStatus,
    PlaceOrderRequest,
    is_terminal,
)
from mft.exchange.oms import OmsView
from mft.exchange.tickers import InvalidTickerError, UniversalTicker
from mft.protocol import (
    STS_DETACH,
    STS_ENSURE_LEVERAGE,
    STS_LEASE_HEARTBEAT,
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    STS_RECON,
    TD_LEASE_ACK,
    TD_LEVERAGE_ACK,
    TD_ORDER_ACK,
    TD_RECON_DONE,
    Envelope,
    EnsureLeverage,
    LeaseAck,
    LeaseHeartbeat,
    LeverageAck,
    ListSessionsRequest,
    OrderAck,
    OrderAckEnvelope,
    OrderCancel,
    OrderSubmit,
    Recon,
    ReconDone,
    RejectCode,
    SessionInfo,
    StsDetach,
    TdAttachRequest,
    TdAttachResult,
    Topics,
    publish_td_log,
)
from mft.protocol.reject_codes import describe
from mft_db.models.session import SessionDomain, SessionStatus

from mft_td.errors import normalize
from mft_td.session.factory import SessionFactory
from mft_td.session.session import Session

logger = logging.getLogger(__name__)

PersistLive = Callable[..., Awaitable[Any]]
MarkDone = Callable[..., Awaitable[Any]]
ListDbSessions = Callable[..., Awaitable[Sequence[Any]]]

#: STS heartbeats every ~1s; grace must tolerate brief lease-loop stalls
#: (e.g. a slow venue round-trip) without false expiry.
LEASE_GRACE_S = 5.0

#: How long a parked ``sts.recon`` waits for the book to come clean before TD
#: answers with it as-is. Comfortably past the forced venue recon behind it,
#: so this only fires when that could not settle the book either.
RECON_WAIT_TIMEOUT_S = 30.0



@dataclass
class StsLink:
    """One STS session attached to a trading api_id."""

    session_id: str
    api_id: int
    created_by: int
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    last_token: int = 0


@dataclass
class TradingAccount:
    """In-memory trading session + STS links for one api_id."""

    api_id: int
    trading: Session
    links: dict[str, StsLink] = field(default_factory=dict)
    global_task: asyncio.Task[Any] | None = None
    global_stop: asyncio.Event = field(default_factory=asyncio.Event)
    #: Serves STS order entry on ``td.order.{api_id}``. Shares ``global_stop``
    #: — both live exactly as long as the account does.
    order_task: asyncio.Task[Any] | None = None
    #: Serves STS account reads on ``td.account.{api_id}`` (e.g. leverage).
    #: Kept off the order loop so a venue round-trip cannot stall submit acks.
    account_task: asyncio.Task[Any] | None = None
    #: ``client_order_id`` → the STS ``session_id`` that submitted it, kept
    #: until the order reaches a terminal state. Advisory only: a cancel from
    #: another session is logged, never blocked. Recon-discovered orders have
    #: no entry and are cancelled silently.
    cid_owner: dict[str, str] = field(default_factory=dict)
    #: STS links waiting for a clean book snapshot (``ReconDone``). Filled
    #: when ``sts.recon`` arrives while UNKNOWN is still open; drained when
    #: the book settles — never by starting a venue recon for the STS.
    recon_waiters: list[tuple[StsLink, str]] = field(default_factory=list)
    #: Answers parked waiters with the book as it stands when UNKNOWN will
    #: not clear at all. One per account, armed on the first park.
    recon_deadline_task: asyncio.Task[Any] | None = None

    @property
    def refcount(self) -> int:
        return len(self.links)


class SessionManager:
    """Owns trading sessions keyed by api_id and STS attach links."""

    def __init__(
        self,
        factory: SessionFactory,
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
        self._accounts: dict[int, TradingAccount] = {}

    def get(self, api_id: int) -> Session | None:
        acct = self._accounts.get(api_id)
        return acct.trading if acct else None

    @property
    def active_api_ids(self) -> list[int]:
        return list(self._accounts)

    async def attach(self, request: TdAttachRequest) -> TdAttachResult:
        """Attach ``api_id`` to STS ``session_id`` (refcount + lease)."""
        acct = self._accounts.get(request.api_id)
        if acct is None:
            trading = await self._factory.create(request.api_id)
            await trading.start()
            acct = TradingAccount(api_id=request.api_id, trading=trading)
            self._accounts[request.api_id] = acct
            trading.on_order(partial(self._on_order_settled, acct))
            trading._on_book_settled = partial(self._flush_recon_waiters, acct)
            acct.global_task = asyncio.create_task(
                self._global_keepalive(acct),
                name=f"td-global-{request.api_id}",
            )
            acct.order_task = asyncio.create_task(
                self._serve_orders(acct),
                name=f"td-orders-{request.api_id}",
            )
            acct.account_task = asyncio.create_task(
                self._serve_account(acct),
                name=f"td-account-{request.api_id}",
            )
            logger.info("TD trading started api_id=%s (refcount 0→1)", request.api_id)
            await publish_td_log(
                self._broker,
                request.api_id,
                f"trading started (refcount 0→1) sts={request.session_id}",
                source="td",
            )

        if request.session_id in acct.links:
            return TdAttachResult(
                session_id=request.session_id,
                api_id=request.api_id,
                refcount=acct.refcount,
            )

        link = StsLink(
            session_id=request.session_id,
            api_id=request.api_id,
            created_by=request.created_by,
        )
        ready = asyncio.Event()
        link.tasks = [
            asyncio.create_task(
                self._lease_loop(acct, link, ready),
                name=f"td-lease-{request.api_id}-{request.session_id}",
            )
        ]

        try:
            await asyncio.wait_for(ready.wait(), timeout=request.timeout)
        except TimeoutError:
            link.stop.set()
            for t in link.tasks:
                t.cancel()
            await asyncio.gather(*link.tasks, return_exceptions=True)
            if acct.refcount == 0:
                await self._destroy_account(request.api_id)
            raise TimeoutError(
                f"timed out waiting for STS lease heartbeat "
                f"session={request.session_id} api_id={request.api_id}"
            ) from None

        acct.links[request.session_id] = link
        if self._persist_live is not None:
            await self._persist_live(
                session_id=request.session_id,
                created_by=request.created_by,
                api_id=request.api_id,
            )

        logger.info(
            "TD attached session=%s api_id=%s refcount=%s",
            request.session_id,
            request.api_id,
            acct.refcount,
        )
        await publish_td_log(
            self._broker,
            request.api_id,
            f"attached sts={request.session_id} refcount={acct.refcount}",
            source="td",
        )
        return TdAttachResult(
            session_id=request.session_id,
            api_id=request.api_id,
            refcount=acct.refcount,
        )

    # Alias for older call sites
    async def create_session(self, request: TdAttachRequest) -> TdAttachResult:
        return await self.attach(request)

    async def list_sessions(
        self, request: ListSessionsRequest
    ) -> list[SessionInfo]:
        if request.domain not in (None, SessionDomain.TD.value, "td"):
            return []

        if self._list_db_sessions is not None:
            db_rows = await self._list_db_sessions(
                status=request.status,
                created_by=request.created_by,
            )
            return [
                SessionInfo(
                    session_id=row.session_id,
                    domain=SessionDomain.TD.value,
                    created_by=row.created_by,
                    created_at=row.created_at.timestamp() if row.created_at else 0.0,
                    finished_at=(
                        row.finished_at.timestamp() if row.finished_at else None
                    ),
                    status=row.status,
                    api_id=row.api_id,
                    sts_session_id=row.session_id,
                )
                for row in db_rows
            ]

        rows: list[SessionInfo] = []
        for acct in self._accounts.values():
            for link in acct.links.values():
                if (
                    request.created_by is not None
                    and link.created_by != request.created_by
                ):
                    continue
                if request.status not in (None, SessionStatus.LIVE.value, "live"):
                    continue
                rows.append(
                    SessionInfo(
                        session_id=link.session_id,
                        domain=SessionDomain.TD.value,
                        created_by=link.created_by,
                        created_at=0.0,
                        finished_at=None,
                        status=SessionStatus.LIVE.value,
                        api_id=link.api_id,
                        sts_session_id=link.session_id,
                    )
                )
        return rows

    async def detach(
        self, *, session_id: str, api_id: int, reason: str = "detach"
    ) -> None:
        acct = self._accounts.get(api_id)
        if acct is None:
            # Process may have already dropped the account; still close the
            # DB row so the UI cannot show a live TD with no STS.
            if self._mark_done is not None:
                await self._mark_done(session_id=session_id, api_id=api_id)
            return

        link = acct.links.pop(session_id, None)
        before = acct.refcount + (1 if link is not None else 0)
        after = acct.refcount
        if link is not None:
            acct.recon_waiters = [
                (waiter, topic)
                for waiter, topic in acct.recon_waiters
                if waiter.session_id != session_id
            ]

        if link is not None:
            try:
                await self._stop_link(link)
            except Exception:
                logger.exception(
                    "TD stop_link failed session=%s api_id=%s",
                    session_id,
                    api_id,
                )

        # Always mark done — even on a retry after a partial detach — so a
        # crashed lease teardown cannot leave a permanent live DB row.
        if self._mark_done is not None:
            try:
                await self._mark_done(session_id=session_id, api_id=api_id)
            except Exception:
                logger.exception(
                    "TD mark_done failed session=%s api_id=%s",
                    session_id,
                    api_id,
                )

        if link is not None:
            await publish_td_log(
                self._broker,
                api_id,
                f"{reason} sts={session_id} refcount {before}→{after}",
                source="td",
            )
            logger.info(
                "TD detached session=%s api_id=%s refcount %s→%s (%s)",
                session_id,
                api_id,
                before,
                after,
                reason,
            )
        elif after == 0:
            logger.info(
                "TD orphan cleanup session=%s api_id=%s reason=%s",
                session_id,
                api_id,
                reason,
            )

        if after == 0 and api_id in self._accounts:
            await self._destroy_account(api_id)
            await publish_td_log(
                self._broker,
                api_id,
                f"trading destroyed (refcount 0) last_sts={session_id}",
                source="td",
            )

    async def close(self, api_id: int) -> None:
        await self._destroy_account(api_id)

    async def close_all(self) -> None:
        for api_id in list(self._accounts):
            await self._destroy_account(api_id)

    async def _stop_link(self, link: StsLink) -> None:
        """Stop a link's tasks without cancelling the caller (if it is one)."""
        link.stop.set()
        current = asyncio.current_task()
        others = [t for t in link.tasks if t is not current]
        for t in others:
            t.cancel()
        if others:
            await asyncio.gather(*others, return_exceptions=True)

    async def _destroy_account(self, api_id: int) -> None:
        acct = self._accounts.pop(api_id, None)
        if acct is None:
            return
        acct.global_stop.set()
        current = asyncio.current_task()
        if (
            acct.recon_deadline_task is not None
            and acct.recon_deadline_task is not current
        ):
            acct.recon_deadline_task.cancel()
            await asyncio.gather(
                acct.recon_deadline_task, return_exceptions=True
            )
            acct.recon_deadline_task = None
        if acct.global_task is not None and acct.global_task is not current:
            acct.global_task.cancel()
            await asyncio.gather(acct.global_task, return_exceptions=True)
        # The order / account loops park in a blocking BLPOP. Cancelling them
        # there leaves the unread reply on the pooled connection and corrupts
        # whatever runs next on it, so let them retire on their own: ``serve``
        # rechecks the stop event between polls, which bounds this to about a
        # second.
        if acct.order_task is not None and acct.order_task is not current:
            await asyncio.gather(acct.order_task, return_exceptions=True)
        if acct.account_task is not None and acct.account_task is not current:
            await asyncio.gather(acct.account_task, return_exceptions=True)
        for link in list(acct.links.values()):
            await self._stop_link(link)
            if self._mark_done is not None:
                await self._mark_done(
                    session_id=link.session_id, api_id=link.api_id
                )
        acct.links.clear()
        await acct.trading.destroy()
        logger.info("TD trading destroyed api_id=%s", api_id)

    async def _global_keepalive(self, acct: TradingAccount) -> None:
        """Publish a lightweight keepalive on td.{api_id}.global while live."""
        topic = Topics.td_global(acct.api_id)
        while not acct.global_stop.is_set():
            try:
                await self._broker.publish(
                    topic,
                    Envelope[dict].wrap(
                        {"api_id": acct.api_id},
                        type="td.global.keepalive",
                        source="td",
                        session_id=str(acct.api_id),
                    ),
                )
            except Exception:
                logger.exception("TD global publish failed api_id=%s", acct.api_id)
                return
            try:
                await asyncio.wait_for(acct.global_stop.wait(), timeout=5.0)
            except TimeoutError:
                continue

    async def _lease_loop(
        self,
        acct: TradingAccount,
        link: StsLink,
        ready: asyncio.Event,
    ) -> None:
        """Sub sts.td.{session_id}; ACK on td.{api_id}.{session_id}; enforce grace."""
        sts_topic = Topics.sts_td_session(link.session_id)
        td_topic = Topics.td_session(link.api_id, link.session_id)
        last_seen = asyncio.get_running_loop().time()

        async def _watch_timeout() -> None:
            nonlocal last_seen
            while not link.stop.is_set():
                await asyncio.sleep(0.5)
                if asyncio.get_running_loop().time() - last_seen > self._lease_grace:
                    logger.warning(
                        "TD lease expired session=%s api_id=%s",
                        link.session_id,
                        link.api_id,
                    )
                    if acct.links.get(link.session_id) is link:
                        # Detach on a sibling task — awaiting it here cancels
                        # this lease loop from inside its own watchdog and
                        # previously blew up with RecursionError, leaving the
                        # DB row live forever.
                        asyncio.create_task(
                            self.detach(
                                session_id=link.session_id,
                                api_id=link.api_id,
                                reason="lease_expired",
                            ),
                            name=(
                                f"td-detach-{link.api_id}-{link.session_id}"
                            ),
                        )
                    return

        watchdog = asyncio.create_task(
            _watch_timeout(), name=f"td-lease-wd-{link.api_id}-{link.session_id}"
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
                        td_topic,
                        Envelope[LeaseAck].wrap(
                            LeaseAck(
                                api_id=link.api_id,
                                session_id=link.session_id,
                                token=hb.token,
                            ),
                            type=TD_LEASE_ACK,
                            source="td",
                            session_id=link.session_id,
                        ),
                    )
                    continue

                # Venue I/O must not stall heartbeat reads — otherwise the
                # watchdog false-expires a still-live STS.
                if env.type == STS_RECON:
                    asyncio.create_task(
                        self._handle_recon(acct, link, td_topic, env.payload),
                        name=f"td-recon-{link.api_id}-{link.session_id}",
                    )
                    continue

                # Order entry does not come through here: it is request-reply
                # on td.order.{api_id} so the strategy learns whether TD took
                # the order. This loop carries lease/recon/detach only.

                if env.type == STS_DETACH:
                    try:
                        det = StsDetach.model_validate(env.payload)
                    except Exception:
                        continue
                    if (
                        det.api_id == link.api_id
                        and det.session_id == link.session_id
                    ):
                        await self.detach(
                            session_id=link.session_id,
                            api_id=link.api_id,
                            reason="sts_stop",
                        )
                        return
        finally:
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)

    def _on_order_settled(self, acct: TradingAccount, order: Order) -> None:
        """Release what an order was holding once the venue owns the outcome.

        Registered on the trading session's order callbacks. Two things end
        here: cid ownership (so the map does not grow without bound) and the
        pre-lock (so the funds go back to available).

        The release happens as soon as the venue acknowledges the order at
        all, not only at a terminal state: from ``NEW`` onward the venue is
        reporting the money as locked itself, and keeping our reservation too
        would double-count it. Releasing on a cid we no longer hold is a
        no-op, so repeated updates for the same order are harmless.
        """
        cid = order.client_order_id
        if not cid:
            return
        if order.status is not OrderStatus.PENDING_NEW:
            asyncio.create_task(
                acct.trading.release(cid),
                name=f"td-release-{acct.api_id}-{cid}",
            )
        if is_terminal(order.status):
            acct.cid_owner.pop(cid, None)

    async def _handle_recon(
        self,
        acct: TradingAccount,
        link: StsLink,
        td_topic: str,
        payload: object,
    ) -> None:
        """Hand STS the current book — do not venue-recon for the attach.

        ``sts.recon`` is an async snapshot request. When the book is clean,
        answer immediately from OMS. When UNKNOWN is open, park the link as a
        waiter and let TD's own resolve / reconnect recon settle the book;
        ``_flush_recon_waiters`` delivers ``ReconDone`` then.
        """
        try:
            recon = Recon.model_validate(payload)
        except Exception:
            logger.warning(
                "TD ignore invalid recon session=%s api_id=%s",
                link.session_id,
                link.api_id,
            )
            return
        if recon.api_id != link.api_id or recon.session_id != link.session_id:
            return
        try:
            if acct.trading.book_ready_for_snapshot():
                await self._publish_recon_done(
                    acct, link, td_topic, acct.trading.view_for_sts()
                )
                return
            # An STS that resends ``sts.recon`` while parked must not collect
            # a second waiter, or it gets a ReconDone per resend at flush.
            if not any(
                waiter.session_id == link.session_id and topic == td_topic
                for waiter, topic in acct.recon_waiters
            ):
                acct.recon_waiters.append((link, td_topic))
                self._arm_recon_deadline(acct)
            logger.info(
                "TD recon waiting session=%s api_id=%s (book not ready)",
                link.session_id,
                link.api_id,
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                f"recon waiting sts={link.session_id} (book not ready)",
                source="td",
            )
            # Kick resolve for any UNKNOWN already sitting in the book; do
            # not start a full venue reconcile — that stays TD-owned.
            # Single-flight: concurrent sts.recon waiters share one pass.
            if acct.trading.has_unknown():
                acct.trading.kick_resolve_all_unknown()
        except Exception as exc:
            logger.exception(
                "TD recon failed session=%s api_id=%s",
                link.session_id,
                link.api_id,
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                f"recon failed sts={link.session_id}: {exc}",
                source="td",
                level="error",
            )

    async def _publish_recon_done(
        self,
        acct: TradingAccount,
        link: StsLink,
        td_topic: str,
        view: OmsView,
    ) -> None:
        await self._broker.publish(
            td_topic,
            Envelope[ReconDone].wrap(
                ReconDone(
                    session_id=link.session_id,
                    api_id=link.api_id,
                    oms=view,
                ),
                type=TD_RECON_DONE,
                source="td",
                session_id=link.session_id,
            ),
        )
        await publish_td_log(
            self._broker,
            link.api_id,
            (
                f"recon done sts={link.session_id} "
                f"orders={len(view.orders)} "
                f"balances={len(view.balances)} "
                f"positions={len(view.positions)}"
            ),
            source="td",
        )
        logger.info(
            "TD recon done session=%s api_id=%s",
            link.session_id,
            link.api_id,
        )

    def _arm_recon_deadline(self, acct: TradingAccount) -> None:
        """Bound how long a parked STS waits for the book to come clean.

        Resolve and the forced recon behind it retry indefinitely, which is
        right — but a venue that can never answer (revoked key, delisted
        instrument) would otherwise park the strategy forever with nothing
        but log lines to show for it. Late and honest beats silent.
        """
        task = acct.recon_deadline_task
        if task is not None and not task.done():
            return

        async def _expire() -> None:
            try:
                await asyncio.sleep(RECON_WAIT_TIMEOUT_S)
                if not acct.recon_waiters:
                    return
                logger.warning(
                    "TD recon wait expired api_id=%s — answering with the "
                    "book as it stands, UNKNOWN still open",
                    acct.api_id,
                )
                await publish_td_log(
                    self._broker,
                    acct.api_id,
                    (
                        f"recon wait expired after {RECON_WAIT_TIMEOUT_S}s — "
                        "snapshot still contains UNKNOWN orders"
                    ),
                    source="td",
                    level="warn",
                )
                await self._flush_recon_waiters(acct, force=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "TD recon deadline failed api_id=%s", acct.api_id
                )

        acct.recon_deadline_task = asyncio.create_task(
            _expire(), name=f"td-recon-deadline-{acct.api_id}"
        )

    async def _flush_recon_waiters(
        self, acct: TradingAccount, *, force: bool = False
    ) -> None:
        """Deliver ``ReconDone`` to every STS waiting on a clean book.

        ``force`` answers with the book as it stands, UNKNOWN and all — the
        deadline path, where waiting longer has stopped being useful.
        """
        if not force and not acct.trading.book_ready_for_snapshot():
            return
        waiters = acct.recon_waiters
        if not waiters:
            return
        acct.recon_waiters = []
        view = acct.trading.view_for_sts()
        for link, td_topic in waiters:
            if link.session_id not in acct.links:
                continue
            try:
                await self._publish_recon_done(acct, link, td_topic, view)
            except Exception:
                logger.exception(
                    "TD recon flush failed session=%s api_id=%s",
                    link.session_id,
                    link.api_id,
                )

    async def _serve_orders(self, acct: TradingAccount) -> None:
        """Answer STS order entry on ``td.order.{api_id}`` while the account lives."""
        subject = Topics.td_order(acct.api_id)
        logger.info("TD order RPC listening api_id=%s subject=%s", acct.api_id, subject)
        try:
            async for req in self._broker.serve(subject, stop=acct.global_stop):
                await self._handle_order_rpc(acct, req)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("TD order RPC loop failed api_id=%s", acct.api_id)

    async def _serve_account(self, acct: TradingAccount) -> None:
        """Answer STS account reads on ``td.account.{api_id}``."""
        subject = Topics.td_account(acct.api_id)
        logger.info(
            "TD account RPC listening api_id=%s subject=%s", acct.api_id, subject
        )
        try:
            async for req in self._broker.serve(subject, stop=acct.global_stop):
                await self._handle_account_rpc(acct, req)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("TD account RPC loop failed api_id=%s", acct.api_id)

    async def _handle_account_rpc(
        self, acct: TradingAccount, req: IncomingRequest
    ) -> None:
        """Reply to :class:`EnsureLeverage` after the venue (or cache) answers."""
        env = req.envelope
        if env.type != STS_ENSURE_LEVERAGE:
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                "",
                False,
                None,
                f"unsupported request {env.type!r}",
                RejectCode.TD_UNSUPPORTED_REQUEST,
            )
            return
        try:
            payload = EnsureLeverage.model_validate(env.payload or {})
        except Exception as exc:
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                "",
                False,
                None,
                f"invalid payload: {exc}",
                RejectCode.TD_INVALID_REQUEST,
            )
            return
        if payload.api_id != acct.api_id:
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                payload.universal_ticker,
                False,
                None,
                f"wrong api_id {payload.api_id}",
                RejectCode.TD_WRONG_API_ID,
            )
            return
        if payload.session_id not in acct.links:
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                payload.universal_ticker,
                False,
                None,
                f"session {payload.session_id!r} not attached to "
                f"api_id={acct.api_id}",
                RejectCode.TD_SESSION_NOT_ATTACHED,
            )
            return
        refusal = _wrong_instrument(payload.universal_ticker, acct)
        if refusal is not None:
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                payload.universal_ticker,
                False,
                None,
                refusal,
                RejectCode.TD_WRONG_INSTRUMENT,
            )
            return
        try:
            ticker = UniversalTicker.parse(payload.universal_ticker)
            leverage = await acct.trading.ensure_leverage(ticker)
        except ExchangeError as exc:
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                payload.universal_ticker,
                False,
                None,
                str(exc),
                RejectCode.TD_LEVERAGE_UNAVAILABLE,
            )
            return
        except Exception as exc:
            logger.exception(
                "TD ensure_leverage failed api_id=%s ticker=%s",
                acct.api_id,
                payload.universal_ticker,
            )
            await self._reply_leverage_ack(
                req,
                acct.api_id,
                payload.universal_ticker,
                False,
                None,
                str(exc),
                RejectCode.TD_INTERNAL,
            )
            return
        await self._reply_leverage_ack(
            req,
            acct.api_id,
            payload.universal_ticker,
            True,
            leverage,
            "",
            RejectCode.NONE,
        )

    async def _reply_leverage_ack(
        self,
        req: IncomingRequest,
        api_id: int,
        universal_ticker: str,
        ok: bool,
        leverage: Decimal | None,
        reason: str,
        error_code: int | str,
    ) -> None:
        await req.reply(
            Envelope[LeverageAck].wrap(
                LeverageAck(
                    api_id=api_id,
                    universal_ticker=universal_ticker,
                    ok=ok,
                    leverage=leverage,
                    reason=reason,
                    error_code=error_code,
                ),
                type=TD_LEVERAGE_ACK,
                source="td",
            )
        )

    async def _handle_order_rpc(
        self, acct: TradingAccount, req: IncomingRequest
    ) -> None:
        """Ack an order request, then run the venue call out of band.

        The reply goes out before any venue I/O on purpose: this is the same
        constraint the pub/sub path has — a slow round-trip must not stall the
        loop — and it is what makes the ack cheap enough to be synchronous.
        ``accepted`` therefore says TD took the request, nothing about the
        venue's answer, which still comes back on ``td.{api_id}.global``.
        """
        env = req.envelope
        if env.type == STS_ORDER_SUBMIT:
            model: type[OrderSubmit] | type[OrderCancel] = OrderSubmit
            handler = self._handle_order_submit
        elif env.type == STS_ORDER_CANCEL:
            model = OrderCancel
            handler = self._handle_order_cancel
        else:
            await self._reply_order_ack(
                req,
                acct.api_id,
                "",
                False,
                f"unsupported request {env.type!r}",
                RejectCode.TD_UNSUPPORTED_REQUEST,
            )
            return

        try:
            payload = model.model_validate(env.payload or {})
        except Exception as exc:
            await self._reply_order_ack(
                req,
                acct.api_id,
                "",
                False,
                f"invalid payload: {exc}",
                RejectCode.TD_INVALID_REQUEST,
            )
            return

        cid = payload.client_order_id
        if payload.api_id != acct.api_id:
            await self._reply_order_ack(
                req,
                acct.api_id,
                cid,
                False,
                f"wrong api_id {payload.api_id}",
                RejectCode.TD_WRONG_API_ID,
            )
            return

        link = acct.links.get(payload.session_id)
        if link is None:
            await self._reply_order_ack(
                req,
                acct.api_id,
                cid,
                False,
                f"session {payload.session_id!r} not attached to "
                f"api_id={acct.api_id}",
                RejectCode.TD_SESSION_NOT_ATTACHED,
            )
            return

        # A submit commits funds. Pre-lock them before the ack so the caller
        # cannot be told True for an order the account cannot afford, and so a
        # second submit in the same round-trip sees the money already spoken
        # for. A refusal here means nothing was sent and no order event will
        # ever follow — the False is the whole story.
        if isinstance(payload, OrderSubmit):
            # The instrument, checked before anything is committed. STS says
            # which one; this is the only place that can tell whether it is one
            # this session's venue trades, and an order for another venue's
            # instrument cannot be made to work by trying.
            refusal = _wrong_instrument(payload.universal_ticker, acct)
            if refusal is not None:
                await self._reply_order_ack(
                    req,
                    acct.api_id,
                    cid,
                    False,
                    refusal,
                    RejectCode.TD_WRONG_INSTRUMENT,
                )
                return
            request = PlaceOrderRequest(
                universal_ticker=payload.universal_ticker,
                side=payload.side,
                type=payload.type,
                qty=payload.qty,
                price=payload.price,
                tif=payload.tif,
                client_order_id=cid,
            )
            refusal = await acct.trading.reserve(request)
            if refusal is not None:
                await self._reply_order_ack(
                    req,
                    acct.api_id,
                    cid,
                    False,
                    f"insufficient balance: {refusal}",
                    RejectCode.TD_INSUFFICIENT_BALANCE,
                )
                return
            # Book it PENDING_NEW and get it into Redis before the ack. A
            # strategy told True can then read the order it just placed; if
            # this write fails the ack is False and no order event follows,
            # so False stays a complete answer.
            try:
                await acct.trading.record_pending_new(request)
            except Exception as exc:
                await acct.trading.release(cid)
                logger.exception(
                    "TD could not record PENDING_NEW api_id=%s cid=%s",
                    acct.api_id,
                    cid,
                )
                await self._reply_order_ack(
                    req,
                    acct.api_id,
                    cid,
                    False,
                    f"state write failed: {exc}",
                    RejectCode.TD_STATE_WRITE_FAILED,
                )
                return
        else:
            # A cancel has no funds to commit, but it does have a state the
            # order must legally be in. PENDING_NEW has no venue id to cancel
            # against and a finished order has nothing left to cancel, so both
            # are refused here rather than sent and rejected by the venue.
            try:
                refusal = await acct.trading.record_pending_cancel(cid)
            except Exception as exc:
                logger.exception(
                    "TD could not record PENDING_CANCEL api_id=%s cid=%s",
                    acct.api_id,
                    cid,
                )
                await self._reply_order_ack(
                    req,
                    acct.api_id,
                    cid,
                    False,
                    f"state write failed: {exc}",
                    RejectCode.TD_STATE_WRITE_FAILED,
                )
                return
            if refusal is not None:
                await self._reply_order_ack(
                    req,
                    acct.api_id,
                    cid,
                    False,
                    refusal,
                    RejectCode.TD_NOT_CANCELABLE,
                )
                return

        await self._reply_order_ack(req, acct.api_id, cid, True, "")
        asyncio.create_task(
            handler(acct, link, env.payload),
            name=f"td-order-{acct.api_id}-{link.session_id}",
        )

    async def _reply_order_ack(
        self,
        req: IncomingRequest,
        api_id: int,
        client_order_id: str,
        accepted: bool,
        reason: str,
        error_code: int | str = RejectCode.NONE,
    ) -> None:
        """Answer an order request.

        Every refusal on this path is TD's own — the venue has not been asked
        yet — so ``error_code`` is always in the ``1xx`` band.
        """
        if not accepted:
            logger.warning(
                "TD order request refused api_id=%s cid=%s code=%s: %s",
                api_id,
                client_order_id,
                describe(error_code),
                reason,
            )
        await req.reply(
            OrderAckEnvelope.wrap(
                OrderAck(
                    api_id=api_id,
                    client_order_id=client_order_id,
                    accepted=accepted,
                    reason=reason,
                    error_code=error_code,
                ),
                type=TD_ORDER_ACK,
                source="td",
            )
        )

    async def _handle_order_submit(
        self,
        acct: TradingAccount,
        link: StsLink,
        payload: object,
    ) -> None:
        try:
            req = OrderSubmit.model_validate(payload)
        except Exception:
            logger.warning(
                "TD ignore invalid order submit session=%s api_id=%s",
                link.session_id,
                link.api_id,
            )
            return
        if req.api_id != link.api_id or req.session_id != link.session_id:
            return
        # Claim ownership before awaiting the venue: another session's lease
        # loop can interleave here, and an unclaimed cid is cancelable by all.
        acct.cid_owner[req.client_order_id] = link.session_id
        try:
            await acct.trading.private.place_order(
                PlaceOrderRequest(
                    universal_ticker=req.universal_ticker,
                    side=req.side,
                    type=req.type,
                    qty=req.qty,
                    price=req.price,
                    tif=req.tif,
                    client_order_id=req.client_order_id,
                )
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                (
                    f"order submitted sts={link.session_id} "
                    f"cid={req.client_order_id} {req.side} {req.qty} "
                    f"{req.universal_ticker}"
                ),
                source="td",
            )
        except ExchangeError as exc:
            acct.cid_owner.pop(req.client_order_id, None)
            code = normalize(exc, venue=acct.trading.venue)
            # Settle the PENDING_NEW we booked before announcing the refusal.
            await acct.trading.record_rejected(req.client_order_id)
            await acct.trading.publish_order_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
                universal_ticker=req.universal_ticker,
                error_code=code,
            )
        except Exception as exc:
            logger.exception(
                "TD order submit failed session=%s api_id=%s cid=%s",
                link.session_id,
                link.api_id,
                req.client_order_id,
            )
            # Send broke — the order may or may not have landed. Mark UNKNOWN
            # and ask the venue; only reject when resolve proves it never did.
            # A None settle means resolve could not answer (often the same dead
            # transport) — leave UNKNOWN and keep ownership; do not pretend
            # the submit was refused.
            settled = await acct.trading.mark_unknown_and_resolve(
                req.client_order_id,
                if_missing=OrderStatus.REJECTED,
            )
            if settled is not None and settled.status is OrderStatus.REJECTED:
                acct.cid_owner.pop(req.client_order_id, None)
                await acct.trading.publish_order_reject(
                    reason=str(exc),
                    client_order_id=req.client_order_id,
                    universal_ticker=req.universal_ticker,
                    error_code=RejectCode.TD_SEND_FAILED,
                )
            elif settled is None:
                await publish_td_log(
                    self._broker,
                    link.api_id,
                    (
                        f"order UNKNOWN sts={link.session_id} "
                        f"cid={req.client_order_id} "
                        f"(send failed, resolve deferred): {exc}"
                    ),
                    source="td",
                    level="warn",
                )

    async def _handle_order_cancel(
        self,
        acct: TradingAccount,
        link: StsLink,
        payload: object,
    ) -> None:
        try:
            req = OrderCancel.model_validate(payload)
        except Exception:
            logger.warning(
                "TD ignore invalid order cancel session=%s api_id=%s",
                link.session_id,
                link.api_id,
            )
            return
        if req.api_id != link.api_id or req.session_id != link.session_id:
            return
        owner = acct.cid_owner.get(req.client_order_id)
        if owner is not None and owner != link.session_id:
            # Warn, but let it through. Blocking would strand any order whose
            # owning session is gone or wedged with nobody able to clear it,
            # which costs more than the occasional cross-session cancel.
            live = "live" if owner in acct.links else "detached"
            logger.warning(
                "TD cross-session cancel session=%s api_id=%s cid=%s owner=%s (%s)",
                link.session_id,
                link.api_id,
                req.client_order_id,
                owner,
                live,
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                (
                    f"cross-session cancel sts={link.session_id} "
                    f"cid={req.client_order_id} owner={owner} ({live})"
                ),
                source="td",
                level="warn",
            )
        try:
            canceled = await acct.trading.private.cancel_by_client_order_id(
                req.client_order_id
            )
            # Apply the ack immediately. cancel_settled alone would drop the
            # PENDING_CANCEL watchdog while leaving the book open if the
            # confirming stream push never arrives.
            await acct.trading.accept_venue_order(canceled)
        except ExchangeError as exc:
            # The venue refused it, so the order is still working: put it back
            # before telling anyone, or PENDING_CANCEL sticks forever.
            code = normalize(exc, venue=acct.trading.venue)
            await acct.trading.revert_pending_cancel(req.client_order_id)
            await acct.trading.publish_cancel_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
                error_code=code,
            )
        except Exception as exc:
            logger.exception(
                "TD order cancel failed session=%s api_id=%s cid=%s",
                link.session_id,
                link.api_id,
                req.client_order_id,
            )
            # Send broke — cancel may already have landed. Do not restore the
            # prior working status; mark UNKNOWN and ask the venue. Still tell
            # STS the cancel attempt failed so it does not assume success.
            await acct.trading.publish_cancel_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
                error_code=RejectCode.TD_SEND_FAILED,
            )
            await acct.trading.mark_unknown_and_resolve(
                req.client_order_id,
                if_missing=OrderStatus.CANCELED,
            )


def _wrong_instrument(universal_ticker: str, acct: TradingAccount) -> str | None:
    """Why ``universal_ticker`` cannot be traded on this account, or None.

    Two failures, both a strategy's: a ticker that is not well formed, and one
    naming a venue this session does not hold a credential for. MD makes the
    same check on its own feeds (``VenueSession._open``); TD could not until
    the order carried an instrument rather than a bare symbol.
    """
    try:
        ticker = UniversalTicker.parse(universal_ticker)
    except InvalidTickerError as exc:
        return str(exc)
    venue = acct.trading.venue
    if ticker.venue != venue:
        return (
            f"api_id={acct.api_id} trades {venue}; "
            f"{universal_ticker} is a {ticker.venue} instrument"
        )
    return None

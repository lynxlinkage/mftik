"""Session manager — trading sessions + STS attach with fencing lease."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
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
from mft.protocol import (
    STS_DETACH,
    STS_LEASE_HEARTBEAT,
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    STS_RECON,
    TD_LEASE_ACK,
    TD_ORDER_ACK,
    TD_RECON_DONE,
    Envelope,
    LeaseAck,
    LeaseHeartbeat,
    ListSessionsRequest,
    OrderAck,
    OrderAckEnvelope,
    OrderCancel,
    OrderSubmit,
    Recon,
    ReconDone,
    SessionInfo,
    StsDetach,
    TdAttachRequest,
    TdAttachResult,
    Topics,
    publish_td_log,
)
from mft_db.models.session import SessionDomain, SessionStatus

from mft_td.session.factory import SessionFactory
from mft_td.session.session import Session

logger = logging.getLogger(__name__)

PersistLive = Callable[..., Awaitable[Any]]
MarkDone = Callable[..., Awaitable[Any]]
ListDbSessions = Callable[..., Awaitable[Sequence[Any]]]

#: STS heartbeats every ~1s; grace must tolerate brief lease-loop stalls
#: (e.g. a slow venue round-trip) without false expiry.
LEASE_GRACE_S = 5.0



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
    #: ``client_order_id`` → the STS ``session_id`` that submitted it, kept
    #: until the order reaches a terminal state. Advisory only: a cancel from
    #: another session is logged, never blocked. Recon-discovered orders have
    #: no entry and are cancelled silently.
    cid_owner: dict[str, str] = field(default_factory=dict)

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
            acct.global_task = asyncio.create_task(
                self._global_keepalive(acct),
                name=f"td-global-{request.api_id}",
            )
            acct.order_task = asyncio.create_task(
                self._serve_orders(acct),
                name=f"td-orders-{request.api_id}",
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
        if acct.global_task is not None and acct.global_task is not current:
            acct.global_task.cancel()
            await asyncio.gather(acct.global_task, return_exceptions=True)
        # The order loop parks in a blocking BLPOP. Cancelling it there leaves
        # the unread reply on the pooled connection and corrupts whatever runs
        # next on it, so let it retire on its own: ``serve`` rechecks the stop
        # event between polls, which bounds this to about a second.
        if acct.order_task is not None and acct.order_task is not current:
            await asyncio.gather(acct.order_task, return_exceptions=True)
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
            view = await acct.trading.reconcile()
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
                req, acct.api_id, "", False, f"unsupported request {env.type!r}"
            )
            return

        try:
            payload = model.model_validate(env.payload or {})
        except Exception as exc:
            await self._reply_order_ack(
                req, acct.api_id, "", False, f"invalid payload: {exc}"
            )
            return

        cid = payload.client_order_id
        if payload.api_id != acct.api_id:
            await self._reply_order_ack(
                req, acct.api_id, cid, False, f"wrong api_id {payload.api_id}"
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
            )
            return

        # A submit commits funds. Pre-lock them before the ack so the caller
        # cannot be told True for an order the account cannot afford, and so a
        # second submit in the same round-trip sees the money already spoken
        # for. A refusal here means nothing was sent and no order event will
        # ever follow — the False is the whole story.
        if isinstance(payload, OrderSubmit):
            request = PlaceOrderRequest(
                symbol=payload.symbol,
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
                    req, acct.api_id, cid, False, f"insufficient balance: {refusal}"
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
                    req, acct.api_id, cid, False, f"state write failed: {exc}"
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
                    req, acct.api_id, cid, False, f"state write failed: {exc}"
                )
                return
            if refusal is not None:
                await self._reply_order_ack(req, acct.api_id, cid, False, refusal)
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
    ) -> None:
        if not accepted:
            logger.warning(
                "TD order request refused api_id=%s cid=%s: %s",
                api_id,
                client_order_id,
                reason,
            )
        await req.reply(
            OrderAckEnvelope.wrap(
                OrderAck(
                    api_id=api_id,
                    client_order_id=client_order_id,
                    accepted=accepted,
                    reason=reason,
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
                    symbol=req.symbol,
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
                    f"cid={req.client_order_id} {req.side} {req.qty} {req.symbol}"
                ),
                source="td",
            )
        except ExchangeError as exc:
            acct.cid_owner.pop(req.client_order_id, None)
            # Settle the PENDING_NEW we booked before announcing the refusal.
            await acct.trading.record_rejected(req.client_order_id)
            await acct.trading.publish_order_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
                symbol=req.symbol,
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                (
                    f"order rejected sts={link.session_id} "
                    f"cid={req.client_order_id}: {exc}"
                ),
                source="td",
                level="warn",
            )
        except Exception as exc:
            acct.cid_owner.pop(req.client_order_id, None)
            logger.exception(
                "TD order submit failed session=%s api_id=%s cid=%s",
                link.session_id,
                link.api_id,
                req.client_order_id,
            )
            await acct.trading.record_rejected(req.client_order_id)
            await acct.trading.publish_order_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
                symbol=req.symbol,
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
            await acct.trading.private.cancel_by_client_order_id(
                req.client_order_id
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                (
                    f"order canceled sts={link.session_id} "
                    f"cid={req.client_order_id}"
                ),
                source="td",
            )
        except ExchangeError as exc:
            # The venue refused it, so the order is still working: put it back
            # before telling anyone, or PENDING_CANCEL sticks forever.
            await acct.trading.revert_pending_cancel(req.client_order_id)
            await acct.trading.publish_cancel_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
            )
            await publish_td_log(
                self._broker,
                link.api_id,
                (
                    f"cancel rejected sts={link.session_id} "
                    f"cid={req.client_order_id}: {exc}"
                ),
                source="td",
                level="warn",
            )
        except Exception as exc:
            logger.exception(
                "TD order cancel failed session=%s api_id=%s cid=%s",
                link.session_id,
                link.api_id,
                req.client_order_id,
            )
            await acct.trading.revert_pending_cancel(req.client_order_id)
            await acct.trading.publish_cancel_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
            )

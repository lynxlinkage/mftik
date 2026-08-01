"""Session manager — trading sessions + STS attach with fencing lease."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from mft.broker import Broker
from mft.exchange.errors import ExchangeError
from mft.exchange.models import PlaceOrderRequest
from mft.protocol import (
    STS_DETACH,
    STS_LEASE_HEARTBEAT,
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    STS_RECON,
    TD_LEASE_ACK,
    TD_RECON_DONE,
    Envelope,
    LeaseAck,
    LeaseHeartbeat,
    ListSessionsRequest,
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

LEASE_GRACE_S = 3.0


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
            acct.global_task = asyncio.create_task(
                self._global_keepalive(acct),
                name=f"td-global-{request.api_id}",
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
            return
        link = acct.links.pop(session_id, None)
        if link is None:
            return
        before = acct.refcount + 1  # just popped
        after = acct.refcount
        await self._stop_link(link)
        if self._mark_done is not None:
            await self._mark_done(session_id=session_id, api_id=api_id)
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
        if after == 0:
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
        """Sub sts.{session_id}; ACK on td.{api_id}.{session_id}; enforce grace."""
        sts_topic = Topics.sts_session(link.session_id)
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
                        await self.detach(
                            session_id=link.session_id,
                            api_id=link.api_id,
                            reason="lease_expired",
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

                if env.type == STS_RECON:
                    await self._handle_recon(acct, link, td_topic, env.payload)
                    continue

                if env.type == STS_ORDER_SUBMIT:
                    await self._handle_order_submit(acct, link, env.payload)
                    continue

                if env.type == STS_ORDER_CANCEL:
                    await self._handle_order_cancel(acct, link, env.payload)
                    continue

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
        try:
            await acct.trading.private.place_order(
                PlaceOrderRequest(
                    symbol=req.symbol,
                    side=req.side,
                    type=req.type,
                    qty=req.qty,
                    price=req.price,
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
            logger.exception(
                "TD order submit failed session=%s api_id=%s cid=%s",
                link.session_id,
                link.api_id,
                req.client_order_id,
            )
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
            await acct.trading.publish_cancel_reject(
                reason=str(exc),
                client_order_id=req.client_order_id,
            )

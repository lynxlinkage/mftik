"""Handlers for one account worker (IF-02, §7.1).

A handler is a :class:`~mftik.broker.handler.Handler`: one decoded
message in, one reply or ``None`` out. It does not see the broker
(H1). The behaviour tests call these methods directly.

**Which subject.**

* ``td.order.{api_id}`` (:meth:`~mftik.protocol.Topics.td_order`):
  submit, cancel, and ``td.order.cancel_session``.
* ``td.account.{api_id}`` (:meth:`~mftik.protocol.Topics.td_account`):
  ``oms.view`` (including ``settled``), ``oms.order``, ``ledger.view``,
  ``td.account.trading`` (B6-02), and ``td.backfill`` (B6-05).

``td.oms.{api_id}`` and ``td.ledger.{api_id}`` stay fan-out subjects
(:meth:`~mftik.protocol.Topics.td_oms`,
:meth:`~mftik.protocol.Topics.td_ledger`). §3.1's "服務 ``td.oms.*``"
names the service. The request subject is the one ``Topics`` and the
strategy client already use. This module does not move it.

The TD process does not mount these handlers. The account worker
process does, on its own subjects. The order path never goes through
the controller (§7.1).

**cancel_session (F10, C1–C4).**

* **C1.** Every OMS order whose ``client_order_id`` session field equals
  the request's ``session_id`` is in scope, and so is every order the
  venue still lists as open for that session. A recon can drop a
  ``PENDING_NEW`` while ``place_order`` is in flight; the ack is then
  ignored and the order rests at the venue with nothing in the book.
  After this session's in-flight submits return, one
  ``fetch_open_orders`` puts those listed orders back on the book.
  That is not a second :meth:`~mftik_td.account.session.Session.reconcile`:
  reconcile replaces the book, which is what drops the order.
  Other sessions are not in scope.
  The field is :func:`mftik.strategy.client_order_id.session_id_of`.
* **C2.** ``PENDING_NEW`` and ``UNKNOWN`` stay in scope. They are
  resolved first, then treated like any other order: resting is
  cancelled, terminal is confirmed. Where the connector has
  ``fetch_order_by_client_order_id``, ``UNKNOWN`` is
  :meth:`~mftik_td.account.session.Session.resolve_unknown`.
  ``PENDING_NEW`` with no venue ``order_id`` is
  :meth:`~mftik_td.account.session.Session.mark_unknown_and_resolve`
  (``if_missing=REJECTED``: the submit never landed). One that already
  has a venue id — OKX, Bybit, Bitget, and sometimes Deribit ack
  ``place_order`` as ``PENDING_NEW`` — is not turned into ``REJECTED``
  when that lookup misses. It is cancelled with ``cancel_order`` on
  the venue id, or left ``unconfirmed``.
  Where the connector has no per-cid lookup (paper's remote client,
  on purpose), one
  :meth:`~mftik_td.account.session.Session.reconcile` inside the
  timeout is the venue answer: an in-scope cid absent from the open
  orders is not resting, one still listed is cancelled.
* **C3.** The reply is :class:`~mftik.protocol.TdCancelSessionResult`.
  ``ok`` is true only when every in-scope order is confirmed. On
  timeout ``ok`` is false and ``unconfirmed`` lists the
  ``client_order_id`` values still outstanding, sorted.
* **C4.** Positions are not cancelled. There is nothing to cancel them
  with.

Confirmed means the OMS status is ``CANCELED``, ``FILLED`` or
``REJECTED`` and that status came from a venue answer: the cancel
reply, the stream, ``fetch_order_by_client_order_id``, or a recon.
``FILLED`` is confirmed — it is not resting. ``PENDING_CANCEL``,
``PENDING_NEW``, ``UNKNOWN``, ``NEW`` and ``PARTIALLY_FILLED`` are not.
When the answer is missing, the cid is ``unconfirmed``.

A submit can be inside ``place_order`` when this runs. The handler
remembers those in-flight submits per session and waits for them,
inside the same timeout, before it takes the scope. One that has not
returned when the timeout ends is ``unconfirmed``. After the cancels
it scans again, so a submit that landed during the call is cancelled
or listed too. ``ok`` is true only on a pass that finds nothing open.

One ``cancel_session`` per session runs at a time on this worker. A
second request for the same session waits, then runs its own pass; it
does not reuse the first reply and it does not interleave cancels for
the same cids. Two sessions run concurrently. The wait is not
:attr:`~mftik_td.account.session.Session._recon_lock`: that lock is
held across ``reconcile``'s venue calls, and this path must not take
it.

**oms.view settled (F13, V1–V3).**

* **V1.** ``settled=False`` answers from memory at once, ``UNKNOWN``
  included, and does not wait on the venue.
* **V2.** ``settled=True`` on a book with no ``UNKNOWN`` answers from
  memory at once. TD does not start a venue pass on a reader's behalf.
* **V3.** ``settled=True`` with ``UNKNOWN`` orders waits until they
  converge or :data:`WAIT_TIMEOUT_S` elapses, then answers with the
  book as it stands. That wait is
  :func:`mftik_td.session.settled.view_when_settled`. A late answer is
  the book including whatever ``UNKNOWN`` is left, not an error. The
  wait does not run on the account subject's serve loop: ``settled=True``
  returns :class:`~mftik.broker.handler.Detached`, and
  :func:`mftik.broker.handler.serve` sends that reply from a task.
  Every other type on ``td.account.{api_id}`` stays sequential.

The trading layer being down is not an empty book. ``settled=True``
while :attr:`~mftik_td.account.trading.TradingLayer.active` is false,
the session is missing, not started, or destroyed, answers
``TD_VENUE_NOT_CONNECTED`` — the same refusal a submit gets there —
as a :class:`~mftik.protocol.RpcError` envelope. A strategy must not
read "no orders" off an account that is simply closed.

Paper submit, cancel, ``oms.view`` (settled and not), ``ledger.view``
and ``cancel_session`` (B6-03) answer. ``oms.order`` still raises
``NotImplementedError("IF-11")``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mftik.broker.handler import Detached, Handler, Reply
from mftik.clock import Clock, SystemClock
from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import Order, OrderStatus, PlaceOrderRequest
from mftik.exchange.oms import LedgerView, OmsView
from mftik.exchange.order_check import REDUCE_ONLY, VENUE, classify
from mftik.exchange.tickers import InvalidTickerError, UniversalTicker
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    TD_ACCOUNT_TRADING,
    TD_BACKFILL,
    TD_ERROR,
    TD_LEDGER_VIEW,
    TD_OMS_ORDER,
    TD_OMS_VIEW,
    TD_ORDER_ACK,
    TD_ORDER_CANCEL_SESSION,
    Envelope,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    RpcError,
    RpcErrorEnvelope,
    TdCancelSessionRequest,
    TdCancelSessionResult,
    TdLedgerViewRequest,
    TdOmsOrderRequest,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
)
from mftik.protocol.reject_codes import is_td_internal
from mftik.strategy.client_order_id import session_id_of
from pydantic import ValidationError

from mftik_td.account._ticket import TICKET
from mftik_td.account.session import UNKNOWN_RESOLVE_TIMEOUT_S
from mftik_td.errors import is_unfilled_immediate, normalize
from mftik_td.session.settled import view_when_settled

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mftik_td.account.session import Session
    from mftik_td.account.worker import AccountWorker

#: How long ``cancel_session`` and ``settled=True`` wait.
#:
#: The plan says both time out and answer with what is left; it does
#: not name a duration. The settled helper's own default is the same
#: 30 seconds (:data:`~mftik_td.session.settled.SETTLED_WAIT_TIMEOUT_S`).
#: :meth:`OmsHandler.view` passes this budget into that helper, so the
#: two waits share one number here. A test locks them together. B6 can
#: split them if a measurement says they differ.
WAIT_TIMEOUT_S = 30.0

#: ``RpcError.code`` when a settled read is refused because the private
#: book is not up. The same refusal a submit gets
#: (:attr:`~mftik.protocol.RejectCode.TD_VENUE_NOT_CONNECTED`).
#: :class:`~mftik.protocol.RpcError.code` is a string, so the int is
#: rendered rather than sent as a number.
_VENUE_OFF = str(int(RejectCode.TD_VENUE_NOT_CONNECTED))


class TradingClosed(RuntimeError):
    """A settled read was asked while the private book is not up.

    The answer is an error, not an empty snapshot. An empty snapshot
    would tell a strategy it holds nothing on an account that is closed.
    """

    def __init__(self) -> None:
        super().__init__("venue is not connected")


def _timeout(timeout: float) -> float:
    if type(timeout) is bool or not isinstance(timeout, int | float) or timeout < 0:
        raise ValueError(f"timeout must be a number >= 0, got {timeout!r}")
    return float(timeout)


def _error(message: UntypedEnvelope, code: str, text: str) -> Reply:
    return RpcErrorEnvelope.wrap(
        RpcError(code=code, message=text),
        type=TD_ERROR,
        source="td",
        session_id=message.session_id,
    )


def _ack(message: UntypedEnvelope, ack: OrderAck) -> Reply:
    return Envelope[OrderAck].wrap(
        ack,
        type=TD_ORDER_ACK,
        source="td",
        session_id=message.session_id,
    )


_RESTING = frozenset({OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED})
_AMBIGUOUS = frozenset({OrderStatus.PENDING_NEW, OrderStatus.UNKNOWN})


@dataclass(frozen=True, slots=True)
class _CancelSent:
    """What one shared cancel attempt did to the book.

    ``exchange_error`` means the venue refused the cancel and the
    pending cancel was reverted. ``settled`` is the order the attempt
    left behind when it already asked the venue (the cancel ack, or the
    resolve after a transport failure).
    """

    ack: OrderAck
    exchange_error: bool = False
    settled: Order | None = None


@dataclass
class _CancelRun:
    """Bookkeeping for one ``cancel_session`` call. Not the book."""

    session_id: str
    confirmed: set[str] = field(default_factory=set)
    seen: set[str] = field(default_factory=set)
    skipped: set[str] = field(default_factory=set)
    chased: set[str] = field(default_factory=set)
    cancel_tried: set[str] = field(default_factory=set)
    recon_done: bool = False
    listed: bool = False


class OrderHandler:
    """``td.order.{api_id}``: submit, cancel, cancel_session."""

    TYPES = frozenset({STS_ORDER_SUBMIT, STS_ORDER_CANCEL, TD_ORDER_CANCEL_SESSION})

    def __init__(self, worker: AccountWorker, *, clock: Clock | None = None) -> None:
        self._worker = worker
        self._clock: Clock = clock if clock is not None else SystemClock()
        #: session_id → cid → future that completes when ``place_order``
        #: has returned and the book update for that submit has finished.
        #: The future's result is a resting order the book did not keep
        #: (recon dropped it while the call was in flight), else ``None``.
        self._inflight: dict[str, dict[str, asyncio.Future[Order | None]]] = {}
        #: One ``cancel_session`` body per session. Not ``_recon_lock``.
        self._gates: dict[str, asyncio.Lock] = {}

    @staticmethod
    def subject(api_id: int) -> str:
        """The request subject for this account's order entry."""
        return Topics.td_order(api_id)

    async def __call__(self, message: UntypedEnvelope) -> Reply | None:
        """Dispatch one order-subject message.

        A payload that does not parse is an error envelope:
        :func:`mftik.broker.handler.serve` logs a raised exception and
        sends nothing (H5). ``cancel_session`` answers with
        :class:`~mftik.protocol.TdCancelSessionResult` on the same type.
        The wire request has no timeout; the wait is
        :data:`WAIT_TIMEOUT_S`.
        """
        if message.type == TD_ORDER_CANCEL_SESSION:
            try:
                request = TdCancelSessionRequest.model_validate(message.payload or {})
            except ValidationError as exc:
                return _error(message, "invalid_payload", str(exc))
            result = await self.cancel_session(request)
            return Envelope[TdCancelSessionResult].wrap(
                result,
                type=TD_ORDER_CANCEL_SESSION,
                source="td",
                session_id=message.session_id,
            )
        if message.type == STS_ORDER_SUBMIT:
            try:
                request = OrderSubmit.model_validate(message.payload or {})
            except ValidationError as exc:
                return _error(message, "invalid_payload", str(exc))
            return _ack(message, await self.submit(request))
        if message.type == STS_ORDER_CANCEL:
            try:
                request = OrderCancel.model_validate(message.payload or {})
            except ValidationError as exc:
                return _error(message, "invalid_payload", str(exc))
            return _ack(message, await self.cancel(request))
        return _error(message, "unknown_type", f"unknown type: {message.type}")

    async def submit(self, request: OrderSubmit) -> OrderAck:
        """Accept or refuse a submit.

        ``accepted`` means this worker took the request, not that the
        venue did. A refusal on the ack is TD's own ``1xx``: the request
        never left the process. The venue outcome is still an event on
        ``td.{api_id}.global``.
        """
        if not isinstance(request, OrderSubmit):
            raise TypeError("submit expects OrderSubmit")
        refused = self._offline(request.api_id, request.client_order_id)
        if refused is not None:
            return refused
        instrument = self._instrument(
            request.api_id, request.client_order_id, request.universal_ticker
        )
        if instrument is not None:
            return instrument
        try:
            order = PlaceOrderRequest(
                universal_ticker=request.universal_ticker,
                side=request.side,
                type=request.type,
                qty=request.qty,
                quote_qty=request.quote_qty,
                price=request.price,
                tif=request.tif,
                reduce_only=request.reduce_only,
                client_order_id=request.client_order_id,
            )
        except ValueError as exc:
            return self._refused(
                request.api_id,
                request.client_order_id,
                RejectCode.TD_INVALID_REQUEST,
                str(exc),
            )
        found = classify(order)
        if found is not None:
            kind, reason = found
            if kind == REDUCE_ONLY:
                code = RejectCode.TD_REDUCE_ONLY_UNSUPPORTED
            elif kind == VENUE:
                code = RejectCode.TD_UNSUPPORTED_ORDER_SHAPE
            else:
                code = RejectCode.TD_INVALID_REQUEST
            return self._refused(request.api_id, request.client_order_id, code, reason)
        session = self._session()
        reason = await session.reserve(order)
        if reason is not None:
            return self._refused(
                request.api_id,
                request.client_order_id,
                RejectCode.TD_INSUFFICIENT_BALANCE,
                reason,
            )
        # Registered before the book write. ``record_pending_new``
        # awaits, and a ``cancel_session`` that runs in that gap would
        # otherwise reconcile a PENDING_NEW the venue has not listed yet.
        key = self._inflight_key(request.session_id, request.client_order_id)
        done: asyncio.Future[Order | None] = asyncio.get_running_loop().create_future()
        self._inflight.setdefault(key, {})[request.client_order_id] = done
        placed: Order | None = None
        try:
            await session.record_pending_new(order, session_id=request.session_id)
            try:
                placed = await session.private.place_order(order)
            except ExchangeError as exc:
                return await self._submit_failed(session, request, exc)
            except Exception as exc:
                return await self._submit_ambiguous(session, request, exc)
            await session.accept_venue_order(placed)
            return OrderAck(
                api_id=request.api_id,
                client_order_id=request.client_order_id,
                accepted=True,
            )
        finally:
            unbooked: Order | None = None
            if (
                placed is not None
                and not placed.status.is_terminal()
                and session.oms.get_order(request.client_order_id) is None
            ):
                unbooked = placed
            self._finish_submit(key, request.client_order_id, done, unbooked)

    async def cancel(self, request: OrderCancel) -> OrderAck:
        """Accept or refuse one cancel."""
        if not isinstance(request, OrderCancel):
            raise TypeError("cancel expects OrderCancel")
        refused = self._offline(request.api_id, request.client_order_id)
        if refused is not None:
            return refused
        return (await self._cancel_sent(self._session(), request)).ack

    async def _cancel_sent(self, session: Session, request: OrderCancel) -> _CancelSent:
        """The cancel ``cancel`` and ``cancel_session`` share.

        ``record_pending_cancel``, then the venue cancel, then
        ``accept_venue_order``. An :class:`~mftik.exchange.errors.ExchangeError`
        reverts the pending cancel. Any other exception takes the
        UNKNOWN path and does not revert. This does not resolve an
        exchange refusal; ``cancel_session`` does that itself, and only
        confirms a terminal answer.
        """
        reason = await session.record_pending_cancel(request.client_order_id)
        if reason is not None:
            return _CancelSent(
                self._refused(
                    request.api_id,
                    request.client_order_id,
                    RejectCode.TD_NOT_CANCELABLE,
                    reason,
                )
            )
        try:
            cancelled = await session.private.cancel_by_client_order_id(
                request.client_order_id
            )
        except ExchangeError as exc:
            await session.revert_pending_cancel(request.client_order_id)
            return _CancelSent(
                await self._cancel_failed(session, request, exc),
                exchange_error=True,
            )
        except Exception as exc:
            ack, settled = await self._cancel_ambiguous(session, request, exc)
            return _CancelSent(ack, settled=settled)
        await session.accept_venue_order(cancelled)
        return _CancelSent(
            OrderAck(
                api_id=request.api_id,
                client_order_id=request.client_order_id,
                accepted=True,
            ),
            settled=cancelled,
        )

    async def cancel_session(
        self,
        request: TdCancelSessionRequest,
        *,
        timeout: float = WAIT_TIMEOUT_S,
    ) -> TdCancelSessionResult:
        """Cancel one session's resting orders and wait (C1–C4).

        ``ok`` is true only when every in-scope order is confirmed not
        resting by a venue answer. ``timeout`` bounds the whole call,
        including venue round trips and the wait for this session's
        in-flight submits. A second call for the same session waits for
        this one to finish, then runs its own pass.
        """
        if not isinstance(request, TdCancelSessionRequest):
            raise TypeError("cancel_session expects TdCancelSessionRequest")
        timeout = _timeout(timeout)
        run = _CancelRun(request.session_id)
        try:
            await self._bound(self._cancel_locked(run), timeout)
        except TimeoutError:
            logger.warning(
                "cancel_session timed out api_id=%s session_id=%s",
                self._worker.api_id,
                request.session_id,
            )
        unconfirmed = self._unconfirmed(run)
        if unconfirmed:
            logger.warning(
                "cancel_session incomplete api_id=%s session_id=%s unconfirmed=%s",
                self._worker.api_id,
                request.session_id,
                unconfirmed,
            )
        else:
            logger.info(
                "cancel_session confirmed api_id=%s session_id=%s",
                self._worker.api_id,
                request.session_id,
            )
        return TdCancelSessionResult(
            session_id=request.session_id,
            ok=not unconfirmed,
            unconfirmed=unconfirmed,
        )

    async def _bound(self, work: Awaitable[None], timeout: float) -> None:
        """Stop ``work`` when ``timeout`` elapses. Do not undo it.

        The system clock uses :func:`asyncio.wait_for`, which is the
        event-loop clock the wire path already runs on. A
        :class:`~mftik.clock.FakeClock` is what component tests advance;
        its sleep does not call :func:`asyncio.sleep`.
        """
        if timeout == 0 or isinstance(self._clock, SystemClock):
            await asyncio.wait_for(work, timeout)
            return
        task = asyncio.ensure_future(work)
        sleeper = asyncio.ensure_future(self._clock.sleep(timeout))
        try:
            done, _pending = await asyncio.wait(
                {task, sleeper}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            task.cancel()
            sleeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            with contextlib.suppress(asyncio.CancelledError):
                await sleeper
            raise
        if task in done:
            sleeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sleeper
            task.result()
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        raise TimeoutError

    def _gate(self, session_id: str) -> asyncio.Lock:
        lock = self._gates.get(session_id)
        if lock is None:
            lock = asyncio.Lock()
            self._gates[session_id] = lock
        return lock

    async def _cancel_locked(self, run: _CancelRun) -> None:
        async with self._gate(run.session_id):
            await self._cancel_body(run)

    async def _cancel_body(self, run: _CancelRun) -> None:
        session = self._session()
        while True:
            # No recon and no cancel while a submit for this session is
            # inside place_order. A new one that starts during the wait
            # is waited on too.
            for order in await self._wait_inflight(run.session_id):
                cid = order.client_order_id
                if cid:
                    run.seen.add(cid)
                    await self._cancel_one(session, run, cid)
            # The book is not the whole scope. A recon (this account's
            # other session, startup, reconnect) can drop a PENDING_NEW
            # while place_order is out, and the ack is then ignored.
            # The venue listing is what still has the live order.
            await self._adopt_listed(session, run)
            self._scope(session, run)
            if self._settled(session, run):
                return
            ambiguous = self._ambiguous_todo(session, run)
            if ambiguous:
                # True: the book may have changed; look again before
                # cancelling. False: these cids cannot be resolved in
                # this call. Resting orders are still cancelled.
                if await self._resolve_ambiguous(session, run, ambiguous):
                    continue
            resting = self._resting_todo(session, run)
            if resting:
                for order in resting:
                    cid = order.client_order_id
                    if cid:
                        await self._cancel_one(session, run, cid)
                continue
            return

    async def _adopt_listed(self, session: Session, run: _CancelRun) -> None:
        """Book this session's venue open orders that the book has lost.

        Once per call, after this session's in-flight submits have
        returned. ``fetch_open_orders`` is the listing; it does not
        replace the book the way :meth:`Session.reconcile` does.
        A connector with no such method keeps the book as its scope
        (the contract fakes). An order already on the book is left
        alone. One that does not decode, or belongs to another
        session, is not booked.
        """
        if run.listed:
            return
        run.listed = True
        fetch = getattr(session.private, "fetch_open_orders", None)
        if fetch is None:
            return
        listed = await fetch()
        for order in listed:
            cid = order.client_order_id
            if not cid or self._session_of(cid) != run.session_id:
                continue
            if order.status.is_terminal():
                continue
            if cid in run.confirmed or cid in run.cancel_tried:
                continue
            if session.oms.get_order(cid) is not None:
                continue
            session.oms.handle_order(order)
            run.seen.add(cid)

    async def _wait_inflight(self, session_id: str) -> list[Order]:
        """Wait until this session has no submit inside ``place_order``.

        ``asyncio.wait`` does not cancel those futures when this wait
        is cancelled, so a timeout does not abort the submit.
        """
        unbooked: list[Order] = []
        while True:
            pending = dict(self._inflight.get(session_id, {}))
            if not pending:
                return unbooked
            done, _pending = await asyncio.wait(list(pending.values()))
            for fut in done:
                if fut.cancelled():
                    continue
                order = fut.result()
                if order is not None:
                    unbooked.append(order)

    async def _resolve_ambiguous(
        self, session: Session, run: _CancelRun, orders: list[Order]
    ) -> bool:
        """Resolve ``PENDING_NEW`` / ``UNKNOWN``. True if the book may have moved.

        A connector with ``fetch_order_by_client_order_id`` uses the
        chase path. One without (paper) gets a single ``reconcile``
        for the whole call. False means nothing further can be done
        for these cids inside this call.
        """
        fetch = getattr(session.private, "fetch_order_by_client_order_id", None)
        if fetch is None:
            if run.recon_done:
                for order in orders:
                    if order.client_order_id:
                        run.chased.add(order.client_order_id)
                return False
            prior = {
                order.client_order_id: order
                for order in orders
                if order.client_order_id
            }
            inflight = set(self._inflight.get(run.session_id, {}))
            await session.reconcile()
            run.recon_done = True
            for cid, order in prior.items():
                run.chased.add(cid)
                run.seen.add(cid)
                if cid in inflight:
                    continue
                if session.oms.get_order(cid) is not None:
                    continue
                # UNKNOWN was already given a terminal update inside
                # reconcile. PENDING_NEW was only dropped; record the
                # same "not resting" answer so its pre-lock can drop.
                # A cid that is back on the book is still resting.
                if order.status is OrderStatus.PENDING_NEW:
                    absent = await session.confirm_absent(
                        order, status=OrderStatus.REJECTED
                    )
                    if not absent:
                        continue
                run.confirmed.add(cid)
            return True
        for order in orders:
            cid = order.client_order_id
            if not cid:
                continue
            run.chased.add(cid)
            if order.status is OrderStatus.UNKNOWN:
                settled = await session.resolve_unknown(order)
            else:
                settled = await self._resolve_pending_new(session, order)
            self._confirm_if_terminal(session, run, cid, settled)
        return True

    async def _resolve_pending_new(
        self, session: Session, order: Order
    ) -> Order | None:
        """Settle ``PENDING_NEW`` without inventing ``REJECTED``.

        A venue id means ``place_order`` already accepted the order and
        the book is still ``PENDING_NEW`` because the private stream has
        not moved it. ``fetch_order_by_client_order_id`` returning
        ``None`` is the index lag, not proof the submit never landed.
        Cancel by that id. A cancel that is not a terminal answer leaves
        the order where it is, so the cid is ``unconfirmed``. No venue
        id still means the submit never landed.
        """
        cid = order.client_order_id
        if not cid:
            return None
        if not order.order_id:
            return await session.mark_unknown_and_resolve(
                cid, if_missing=OrderStatus.REJECTED
            )
        found = await self._lookup_order(session, order)
        if found is not None and found.status is not OrderStatus.PENDING_NEW:
            await session.accept_venue_order(found)
            current = session.oms.get_order(cid)
            return current if current is not None else found
        return await self._cancel_by_venue_id(session, order)

    async def _lookup_order(self, session: Session, order: Order) -> Order | None:
        fetch = getattr(session.private, "fetch_order_by_client_order_id", None)
        cid = order.client_order_id
        if fetch is None or not cid:
            return None
        try:
            return await asyncio.wait_for(
                fetch(cid, ticker=order.ticker),
                UNKNOWN_RESOLVE_TIMEOUT_S,
            )
        except Exception:
            logger.exception(
                "cancel_session lookup failed api_id=%s cid=%s",
                self._worker.api_id,
                cid,
            )
            return None

    async def _cancel_by_venue_id(
        self, session: Session, order: Order
    ) -> Order | None:
        cid = order.client_order_id
        cancel = getattr(session.private, "cancel_order", None)
        if cancel is None or not order.order_id or not cid:
            return None
        try:
            cancelled = await cancel(order.order_id)
        except Exception:
            logger.exception(
                "cancel_session cancel by venue id failed api_id=%s cid=%s "
                "order_id=%s",
                self._worker.api_id,
                cid,
                order.order_id,
            )
            return None
        if cancelled is None:
            return None
        await session.accept_venue_order(cancelled)
        if not cancelled.status.is_terminal():
            return None
        return cancelled

    async def _cancel_one(self, session: Session, run: _CancelRun, cid: str) -> None:
        if cid in run.cancel_tried:
            return
        run.cancel_tried.add(cid)
        run.seen.add(cid)
        sent = await self._cancel_sent(
            session,
            OrderCancel(
                session_id=run.session_id,
                api_id=self._worker.api_id,
                client_order_id=cid,
            ),
        )
        if sent.exchange_error:
            # The venue refused, or does not know the order. The pending
            # cancel is already reverted. Confirm only a terminal answer.
            settled = await session.mark_unknown_and_resolve(
                cid, if_missing=OrderStatus.CANCELED
            )
            self._confirm_if_terminal(session, run, cid, settled)
            return
        self._confirm_if_terminal(session, run, cid, sent.settled)

    def _scope(self, session: Session, run: _CancelRun) -> None:
        for cid in session.oms.view().orders:
            owner = self._session_of(cid)
            if owner is None:
                if cid not in run.skipped:
                    run.skipped.add(cid)
                    logger.warning(
                        "cancel_session skipping cid that does not decode "
                        "api_id=%s cid=%s",
                        self._worker.api_id,
                        cid,
                    )
                continue
            if owner == run.session_id:
                run.seen.add(cid)

    def _ambiguous_todo(self, session: Session, run: _CancelRun) -> list[Order]:
        return self._todo(session, run, _AMBIGUOUS, run.chased)

    def _resting_todo(self, session: Session, run: _CancelRun) -> list[Order]:
        return self._todo(session, run, _RESTING, run.cancel_tried)

    def _todo(
        self,
        session: Session,
        run: _CancelRun,
        statuses: frozenset[OrderStatus],
        skip: set[str],
    ) -> list[Order]:
        inflight = self._inflight.get(run.session_id, {})
        found: list[Order] = []
        for cid, order in session.oms.view().orders.items():
            if self._session_of(cid) != run.session_id:
                continue
            if cid in inflight or cid in skip or cid in run.confirmed:
                continue
            if order.status in statuses:
                found.append(order)
        return found

    def _settled(self, session: Session, run: _CancelRun) -> bool:
        """True when a pass finds nothing in scope still open."""
        if self._inflight.get(run.session_id):
            return False
        for cid, order in session.oms.view().orders.items():
            if self._session_of(cid) != run.session_id:
                continue
            if cid in run.confirmed or order.status.is_terminal():
                continue
            return False
        for cid in run.seen:
            if cid in run.confirmed:
                continue
            current = session.oms.get_order(cid)
            if current is None or not current.status.is_terminal():
                return False
        return True

    def _confirm_if_terminal(
        self,
        session: Session,
        run: _CancelRun,
        cid: str,
        settled: Order | None,
    ) -> None:
        if settled is None or not settled.status.is_terminal():
            return
        current = session.oms.get_order(cid)
        if current is not None and not current.status.is_terminal():
            return
        run.confirmed.add(cid)

    def _unconfirmed(self, run: _CancelRun) -> list[str]:
        session = self._worker.trading.session
        found: set[str] = set()
        if session is not None:
            for cid, order in session.oms.view().orders.items():
                if self._session_of(cid) != run.session_id:
                    continue
                if cid in run.confirmed or order.status.is_terminal():
                    continue
                found.add(cid)
            for cid in run.seen:
                if cid in run.confirmed:
                    continue
                current = session.oms.get_order(cid)
                if current is None or not current.status.is_terminal():
                    found.add(cid)
        for cid in self._inflight.get(run.session_id, {}):
            if cid not in run.confirmed:
                found.add(cid)
        return sorted(found)

    def _session_of(self, client_order_id: str) -> str | None:
        try:
            return session_id_of(client_order_id)
        except (ValueError, TypeError):
            return None

    def _inflight_key(self, session_id: str, client_order_id: str) -> str:
        owner = self._session_of(client_order_id)
        return owner if owner is not None else session_id

    def _finish_submit(
        self,
        session_id: str,
        client_order_id: str,
        done: asyncio.Future[Order | None],
        unbooked: Order | None,
    ) -> None:
        book = self._inflight.get(session_id)
        if book is not None:
            book.pop(client_order_id, None)
            if not book:
                self._inflight.pop(session_id, None)
        if not done.done():
            done.set_result(unbooked)

    def _offline(self, api_id: int, client_order_id: str) -> OrderAck | None:
        if api_id != self._worker.api_id:
            return self._refused(
                api_id,
                client_order_id,
                RejectCode.TD_WRONG_API_ID,
                f"api_id {api_id} is not this worker",
            )
        trading = self._worker.trading
        private = trading.private
        connected = bool(getattr(private, "connected", False))
        if not trading.active or trading.session is None or not connected:
            return self._refused(
                api_id,
                client_order_id,
                RejectCode.TD_VENUE_NOT_CONNECTED,
                "venue is not connected",
            )
        return None

    def _instrument(
        self, api_id: int, client_order_id: str, universal_ticker: str
    ) -> OrderAck | None:
        try:
            ticker = UniversalTicker.parse(universal_ticker)
        except InvalidTickerError as exc:
            return self._refused(
                api_id,
                client_order_id,
                RejectCode.TD_WRONG_INSTRUMENT,
                str(exc),
            )
        if ticker.venue != self._worker.venue:
            return self._refused(
                api_id,
                client_order_id,
                RejectCode.TD_WRONG_INSTRUMENT,
                f"{universal_ticker} is not a {self._worker.venue} instrument",
            )
        return None

    def _session(self) -> Session:
        session = self._worker.trading.session
        if session is None:
            raise RuntimeError("order path requires a session")
        return session

    def _refused(
        self,
        api_id: int,
        client_order_id: str,
        code: int | str,
        reason: str,
    ) -> OrderAck:
        return OrderAck(
            api_id=api_id,
            client_order_id=client_order_id,
            accepted=False,
            reason=reason,
            error_code=code,
        )

    async def _submit_failed(
        self, session: Session, request: OrderSubmit, exc: BaseException
    ) -> OrderAck:
        if is_unfilled_immediate(exc, venue=self._worker.venue):
            await session.record_unfilled(request.client_order_id, reason=str(exc))
            return OrderAck(
                api_id=request.api_id,
                client_order_id=request.client_order_id,
                accepted=True,
            )
        code = normalize(exc, venue=self._worker.venue)
        await session.record_rejected(request.client_order_id)
        if is_td_internal(code):
            return self._refused(
                request.api_id, request.client_order_id, code, str(exc)
            )
        await session.publish_order_reject(
            reason=str(exc),
            client_order_id=request.client_order_id,
            universal_ticker=request.universal_ticker,
            error_code=code,
        )
        return OrderAck(
            api_id=request.api_id,
            client_order_id=request.client_order_id,
            accepted=True,
        )

    async def _cancel_failed(
        self, session: Session, request: OrderCancel, exc: BaseException
    ) -> OrderAck:
        code = normalize(exc, venue=self._worker.venue)
        if is_td_internal(code):
            return self._refused(
                request.api_id, request.client_order_id, code, str(exc)
            )
        await session.publish_cancel_reject(
            reason=str(exc),
            client_order_id=request.client_order_id,
            error_code=code,
        )
        return OrderAck(
            api_id=request.api_id,
            client_order_id=request.client_order_id,
            accepted=True,
        )

    async def _submit_ambiguous(
        self, session: Session, request: OrderSubmit, exc: BaseException
    ) -> OrderAck:
        """The send failed. The order may already be resting.

        An :class:`~mftik.exchange.errors.ExchangeError` is the venue
        saying no, and that path rejects. Anything else is the transport
        (RM-06): mark ``UNKNOWN`` and ask the venue. Publish
        ``TD_SEND_FAILED`` only when that lookup proves the order never
        landed. No answer leaves the order ``UNKNOWN`` for recon.
        """
        logger.exception(
            "TD order submit failed api_id=%s cid=%s",
            request.api_id,
            request.client_order_id,
        )
        settled = await session.mark_unknown_and_resolve(
            request.client_order_id,
            if_missing=OrderStatus.REJECTED,
        )
        if settled is not None and settled.status is OrderStatus.REJECTED:
            await session.publish_order_reject(
                reason=str(exc),
                client_order_id=request.client_order_id,
                universal_ticker=request.universal_ticker,
                error_code=RejectCode.TD_SEND_FAILED,
            )
        elif settled is None:
            logger.warning(
                "order UNKNOWN api_id=%s cid=%s (send failed, resolve deferred): %s",
                request.api_id,
                request.client_order_id,
                exc,
            )
        return OrderAck(
            api_id=request.api_id,
            client_order_id=request.client_order_id,
            accepted=True,
        )

    async def _cancel_ambiguous(
        self, session: Session, request: OrderCancel, exc: BaseException
    ) -> tuple[OrderAck, Order | None]:
        """The cancel send failed. Do not put the order back to working.

        The cancel may already have landed. Tell STS the attempt is
        ambiguous, then mark ``UNKNOWN``. ``if_missing=CANCELED`` is
        used only when the venue can say the order is gone. The second
        value is whatever that resolve settled, or ``None`` when the
        order is still ``UNKNOWN``.
        """
        logger.exception(
            "TD order cancel failed api_id=%s cid=%s",
            request.api_id,
            request.client_order_id,
        )
        await session.publish_cancel_reject(
            reason=str(exc),
            client_order_id=request.client_order_id,
            error_code=RejectCode.TD_SEND_FAILED,
        )
        settled = await session.mark_unknown_and_resolve(
            request.client_order_id,
            if_missing=OrderStatus.CANCELED,
        )
        return (
            OrderAck(
                api_id=request.api_id,
                client_order_id=request.client_order_id,
                accepted=True,
            ),
            settled,
        )


class OmsHandler:
    """``oms.view`` and ``oms.order`` on ``td.account.{api_id}``."""

    TYPES = frozenset({TD_OMS_VIEW, TD_OMS_ORDER})

    def __init__(self, worker: AccountWorker) -> None:
        self._worker = worker

    @staticmethod
    def subject(api_id: int) -> str:
        """The request subject. Not ``td.oms.{api_id}``, which is fan-out."""
        return Topics.td_account(api_id)

    async def __call__(self, message: UntypedEnvelope) -> Reply | Detached | None:
        """Dispatch one OMS read.

        Unsettled ``oms.view`` answers from memory, on the subject
        loop. ``settled=True`` returns :class:`Detached` so the wait
        does not hold ``td.account.{api_id}``. ``oms.order`` is still
        ``NotImplementedError("IF-11")``.
        """
        if message.type == TD_OMS_ORDER:
            raise NotImplementedError(TICKET)
        if message.type != TD_OMS_VIEW:
            raise NotImplementedError(TICKET)
        try:
            request = TdOmsViewRequest.model_validate(message.payload or {})
        except ValidationError as exc:
            return _error(message, "invalid_payload", str(exc))
        if not request.settled:
            return _oms_reply(message, await self.view(request))
        try:
            self._require_trading()
        except TradingClosed as exc:
            return _venue_off(message, exc)
        return Detached(self._settled_reply(message, request))

    async def _settled_reply(
        self, message: UntypedEnvelope, request: TdOmsViewRequest
    ) -> Reply:
        """The settled snapshot, or the venue-off error if the book closed."""
        try:
            view = await self.view(request)
        except TradingClosed as exc:
            return _venue_off(message, exc)
        return _oms_reply(message, view)

    def _require_trading(self) -> None:
        """Refuse a settled read that would describe a closed account.

        ``active`` is false until :meth:`Session.start` has returned,
        and false again after deactivate. A missing, unstarted or
        destroyed session is the same refusal: there is no live book
        to call settled.
        """
        trading = self._worker.trading
        session = trading.session
        if (
            not trading.active
            or session is None
            or session.destroyed
            or not session.started
        ):
            raise TradingClosed()

    async def view(
        self,
        request: TdOmsViewRequest,
        *,
        timeout: float = WAIT_TIMEOUT_S,
    ) -> OmsView:
        """The live book. ``request.settled`` selects V1 or V2/V3.

        ``timeout`` bounds the settled wait only. A non-settled read
        ignores it and does not touch the venue (V1). A settled read
        of a book that is not up raises :class:`TradingClosed` rather
        than returning whatever is in memory.
        """
        if not isinstance(request, TdOmsViewRequest):
            raise TypeError("view expects TdOmsViewRequest")
        _timeout(timeout)
        if not request.settled:
            return self._worker.trading.oms.view()
        self._require_trading()
        session = self._worker.trading.session
        # ``_require_trading`` just rejected a missing session.
        assert session is not None
        view = await view_when_settled(session, timeout=timeout)
        # Deactivate can land while the wait is parked. The snapshot
        # taken after that is not a settled read of a live account.
        self._require_trading()
        return view

    async def order(self, request: TdOmsOrderRequest) -> Order | None:
        """One live order by ``client_order_id``, or ``None`` if it is gone.

        A memory read. B6-02. ``None`` here means the cid is not on the
        book, which this stub does not answer: it raises instead, so a
        caller cannot mistake "not implemented" for "not resting".
        """
        if not isinstance(request, TdOmsOrderRequest):
            raise TypeError("order expects TdOmsOrderRequest")
        raise NotImplementedError(TICKET)


class LedgerHandler:
    """``ledger.view`` on ``td.account.{api_id}``."""

    TYPES = frozenset({TD_LEDGER_VIEW})

    def __init__(self, worker: AccountWorker) -> None:
        self._worker = worker

    @staticmethod
    def subject(api_id: int) -> str:
        """The request subject. Not ``td.ledger.{api_id}``, which is fan-out."""
        return Topics.td_account(api_id)

    async def __call__(self, message: UntypedEnvelope) -> Reply | None:
        """Dispatch one ledger read. A memory read; it does not touch the venue."""
        if message.type != TD_LEDGER_VIEW:
            raise NotImplementedError(TICKET)
        try:
            request = TdLedgerViewRequest.model_validate(message.payload or {})
        except ValidationError as exc:
            return _error(message, "invalid_payload", str(exc))
        view = await self.view(request)
        return Envelope[LedgerView].wrap(
            view,
            type=TD_LEDGER_VIEW,
            source="td",
            session_id=message.session_id,
        )

    async def view(self, request: TdLedgerViewRequest) -> LedgerView:
        """The ledger in memory. Must not wait on the venue.

        ``asset`` set names one row; omitted, the whole ledger.
        """
        if not isinstance(request, TdLedgerViewRequest):
            raise TypeError("view expects TdLedgerViewRequest")
        balances = dict(self._worker.trading.ledger.snapshot())
        if request.asset is not None:
            row = balances.get(request.asset)
            balances = {request.asset: row} if row is not None else {}
        return LedgerView(api_id=self._worker.api_id, balances=balances)


def _oms_reply(message: UntypedEnvelope, view: OmsView) -> Reply:
    return Envelope[OmsView].wrap(
        view,
        type=TD_OMS_VIEW,
        source="td",
        session_id=message.session_id,
    )


def _venue_off(message: UntypedEnvelope, exc: TradingClosed) -> Reply:
    return _error(message, _VENUE_OFF, str(exc))


def account_subject_handler(worker: AccountWorker) -> Handler:
    """``td.account.{api_id}``: OMS, the ledger, backfill, the trading bit."""

    async def handle(message: UntypedEnvelope) -> Reply | Detached | None:
        if message.type == TD_ACCOUNT_TRADING:
            return await worker.trading.handle(message)
        if message.type == TD_BACKFILL:
            return await worker.resident.handle_backfill(message)
        if message.type in worker.oms.TYPES:
            return await worker.oms(message)
        if message.type in worker.ledger.TYPES:
            return await worker.ledger(message)
        return _error(message, "unknown_type", f"unknown type: {message.type}")

    return handle

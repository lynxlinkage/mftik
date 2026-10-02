"""Handlers for one account worker (IF-02, §7.1).

A handler is a :class:`~mftik.broker.handler.Handler`: one decoded
message in, one reply or ``None`` out. It does not see the broker
(H1). The behaviour tests call these methods directly.

**Which subject.**

* ``td.order.{api_id}`` (:meth:`~mftik.protocol.Topics.td_order`):
  submit, cancel, and ``td.order.cancel_session``.
* ``td.account.{api_id}`` (:meth:`~mftik.protocol.Topics.td_account`):
  ``oms.view`` (including ``settled``), ``oms.order``, ``ledger.view``.

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
  the request's ``session_id`` is in scope. Other sessions are not.
  The field is :func:`mftik.strategy.client_order_id.session_id_of`.
* **C2.** ``PENDING_NEW`` and ``UNKNOWN`` stay in scope. They are
  handled after chase converges them, the same chase
  :meth:`~mftik_td.account.session.Session.chase_unknown` already runs:
  ``trading.private.fetch_order_by_client_order_id`` when the connector
  has it. An order chase finds already terminal is confirmed, not
  cancelled again. One chase finds still resting is cancelled.
* **C3.** The reply is :class:`~mftik.protocol.TdCancelSessionResult`.
  ``ok`` is true only when every in-scope order is confirmed. On
  timeout ``ok`` is false and ``unconfirmed`` lists the
  ``client_order_id`` values still outstanding.
* **C4.** Positions are not cancelled. There is nothing to cancel them
  with.

**oms.view settled (F13, V1–V3).**

* **V1.** ``settled=False`` answers from memory at once, ``UNKNOWN``
  included, and does not wait on the venue.
* **V2.** ``settled=True`` on a book with no ``UNKNOWN`` answers from
  memory at once. TD does not start a venue pass on a reader's behalf.
* **V3.** ``settled=True`` with ``UNKNOWN`` orders waits until they
  converge or :data:`WAIT_TIMEOUT_S` elapses, then answers with the
  book as it stands. That wait is
  :func:`mftik_td.session.settled.view_when_settled`, which B6-08
  points at this handler. A late answer is the book including whatever
  ``UNKNOWN`` is left, not an error.

Paper submit, cancel, unsettled ``oms.view`` and ``ledger.view`` are
B4-05. ``cancel_session`` (B6-03), ``oms.order`` (B6-02) and
``settled=True`` (B6-08) still raise ``NotImplementedError("IF-11")``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mftik.broker.handler import Handler, Reply
from mftik.exchange.models import Order, PlaceOrderRequest
from mftik.exchange.oms import LedgerView, OmsView
from mftik.exchange.order_check import REDUCE_ONLY, VENUE, classify
from mftik.exchange.tickers import InvalidTickerError, UniversalTicker
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
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
from pydantic import ValidationError

from mftik_td.account._ticket import TICKET
from mftik_td.errors import is_unfilled_immediate, normalize

if TYPE_CHECKING:
    from mftik_td.account.session import Session
    from mftik_td.account.worker import AccountWorker

#: How long ``cancel_session`` and ``settled=True`` wait.
#:
#: The plan says both time out and answer with what is left; it does
#: not name a duration. The settled read already waits 30 seconds
#: (:data:`~mftik_td.session.settled.SETTLED_WAIT_TIMEOUT_S`). This is
#: that same budget, copied so this package does not import
#: :mod:`mftik_td.session`. B6-08 will call into that helper from here,
#: and an import the other way would cycle. A test locks the two
#: numbers together. B6 can split them if a measurement says they differ.
WAIT_TIMEOUT_S = 30.0


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


class OrderHandler:
    """``td.order.{api_id}``: submit, cancel, cancel_session."""

    TYPES = frozenset({STS_ORDER_SUBMIT, STS_ORDER_CANCEL, TD_ORDER_CANCEL_SESSION})

    def __init__(self, worker: AccountWorker) -> None:
        self._worker = worker

    @staticmethod
    def subject(api_id: int) -> str:
        """The request subject for this account's order entry."""
        return Topics.td_order(api_id)

    async def __call__(self, message: UntypedEnvelope) -> Reply | None:
        """Dispatch one order-subject message.

        ``cancel_session`` is B6-03 and still raises. A payload that
        does not parse is an error envelope: :func:`mftik.broker.handler.serve`
        logs a raised exception and sends nothing (H5).
        """
        if message.type == TD_ORDER_CANCEL_SESSION:
            raise NotImplementedError(TICKET)
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
            return self._refused(
                request.api_id, request.client_order_id, code, reason
            )
        session = self._session()
        reason = await session.reserve(order)
        if reason is not None:
            return self._refused(
                request.api_id,
                request.client_order_id,
                RejectCode.TD_INSUFFICIENT_BALANCE,
                reason,
            )
        await session.record_pending_new(order, session_id=request.session_id)
        try:
            placed = await session.private.place_order(order)
        except Exception as exc:
            return await self._submit_failed(session, request, exc)
        await session.accept_venue_order(placed)
        return OrderAck(
            api_id=request.api_id,
            client_order_id=request.client_order_id,
            accepted=True,
        )

    async def cancel(self, request: OrderCancel) -> OrderAck:
        """Accept or refuse one cancel."""
        if not isinstance(request, OrderCancel):
            raise TypeError("cancel expects OrderCancel")
        refused = self._offline(request.api_id, request.client_order_id)
        if refused is not None:
            return refused
        session = self._session()
        reason = await session.record_pending_cancel(request.client_order_id)
        if reason is not None:
            return self._refused(
                request.api_id,
                request.client_order_id,
                RejectCode.TD_NOT_CANCELABLE,
                reason,
            )
        try:
            cancelled = await session.private.cancel_by_client_order_id(
                request.client_order_id
            )
        except Exception as exc:
            await session.revert_pending_cancel(request.client_order_id)
            return await self._cancel_failed(session, request, exc)
        await session.accept_venue_order(cancelled)
        return OrderAck(
            api_id=request.api_id,
            client_order_id=request.client_order_id,
            accepted=True,
        )

    async def cancel_session(
        self,
        request: TdCancelSessionRequest,
        *,
        timeout: float = WAIT_TIMEOUT_S,
    ) -> TdCancelSessionResult:
        """Cancel one session's resting orders and wait (C1–C4). B6-03.

        Chase and cancel go through ``trading.private``:
        ``fetch_order_by_client_order_id`` for ``PENDING_NEW`` and
        ``UNKNOWN``, then ``cancel_by_client_order_id`` for whatever is
        still resting. The book they read and update is ``trading.oms``.
        """
        if not isinstance(request, TdCancelSessionRequest):
            raise TypeError("cancel_session expects TdCancelSessionRequest")
        _timeout(timeout)
        raise NotImplementedError(TICKET)

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


class OmsHandler:
    """``oms.view`` and ``oms.order`` on ``td.account.{api_id}``."""

    TYPES = frozenset({TD_OMS_VIEW, TD_OMS_ORDER})

    def __init__(self, worker: AccountWorker) -> None:
        self._worker = worker

    @staticmethod
    def subject(api_id: int) -> str:
        """The request subject. Not ``td.oms.{api_id}``, which is fan-out."""
        return Topics.td_account(api_id)

    async def __call__(self, message: UntypedEnvelope) -> Reply | None:
        """Dispatch one OMS read.

        Unsettled ``oms.view`` answers from memory. ``oms.order`` is
        B6-02 and ``settled=True`` is B6-08; both still raise.
        """
        if message.type == TD_OMS_ORDER:
            raise NotImplementedError(TICKET)
        if message.type != TD_OMS_VIEW:
            raise NotImplementedError(TICKET)
        try:
            request = TdOmsViewRequest.model_validate(message.payload or {})
        except ValidationError as exc:
            return _error(message, "invalid_payload", str(exc))
        view = await self.view(request)
        return Envelope[OmsView].wrap(
            view,
            type=TD_OMS_VIEW,
            source="td",
            session_id=message.session_id,
        )

    async def view(
        self,
        request: TdOmsViewRequest,
        *,
        timeout: float = WAIT_TIMEOUT_S,
    ) -> OmsView:
        """The live book. ``request.settled`` selects V1 or V2/V3.

        ``timeout`` bounds the settled wait only. A non-settled read
        ignores it and does not touch the venue (V1).
        """
        if not isinstance(request, TdOmsViewRequest):
            raise TypeError("view expects TdOmsViewRequest")
        _timeout(timeout)
        if request.settled:
            raise NotImplementedError(TICKET)
        return self._worker.trading.oms.view()

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


def account_subject_handler(worker: AccountWorker) -> Handler:
    """``td.account.{api_id}``: OMS reads and the ledger, one subject."""

    async def handle(message: UntypedEnvelope) -> Reply | None:
        if message.type in worker.oms.TYPES:
            return await worker.oms(message)
        if message.type in worker.ledger.TYPES:
            return await worker.ledger(message)
        return _error(message, "unknown_type", f"unknown type: {message.type}")

    return handle

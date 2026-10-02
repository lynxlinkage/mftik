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

The TD process does not mount these handlers. B6 does.

**cancel_session (F10, C1–C4).**

* **C1.** Every OMS order whose ``client_order_id`` session field equals
  the request's ``session_id`` is in scope. Other sessions are not.
  The field is :func:`mftik.strategy.client_order_id.session_id_of`.
* **C2.** ``PENDING_NEW`` and ``UNKNOWN`` stay in scope. They are
  handled after chase converges them, the same chase
  :meth:`~mftik_td.session.session.Session.chase_unknown` already runs:
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

Null until B6-02, B6-03 and B6-08. Every method raises
``NotImplementedError("IF-11")``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mftik.broker.handler import Reply
from mftik.exchange.models import Order
from mftik.exchange.oms import LedgerView, OmsView
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    TD_LEDGER_VIEW,
    TD_OMS_ORDER,
    TD_OMS_VIEW,
    TD_ORDER_CANCEL_SESSION,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    TdCancelSessionRequest,
    TdCancelSessionResult,
    TdLedgerViewRequest,
    TdOmsOrderRequest,
    TdOmsViewRequest,
    Topics,
    UntypedEnvelope,
)

from mftik_td.account._ticket import TICKET

if TYPE_CHECKING:
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
        """Dispatch one order-subject message. B6-02 / B6-03."""
        raise NotImplementedError(TICKET)

    async def submit(self, request: OrderSubmit) -> OrderAck:
        """Accept or refuse a submit before the venue sees it. B6-02.

        ``accepted`` means this worker took the request, not that the
        venue did. The venue outcome is still an event on
        ``td.{api_id}.global``.
        """
        if not isinstance(request, OrderSubmit):
            raise TypeError("submit expects OrderSubmit")
        raise NotImplementedError(TICKET)

    async def cancel(self, request: OrderCancel) -> OrderAck:
        """Accept or refuse one cancel. B6-02."""
        if not isinstance(request, OrderCancel):
            raise TypeError("cancel expects OrderCancel")
        raise NotImplementedError(TICKET)

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
        """Dispatch one OMS read. B6-02 / B6-08."""
        raise NotImplementedError(TICKET)

    async def view(
        self,
        request: TdOmsViewRequest,
        *,
        timeout: float = WAIT_TIMEOUT_S,
    ) -> OmsView:
        """The live book. ``request.settled`` selects V1 or V2/V3. B6-08.

        ``timeout`` bounds the settled wait only. A non-settled read
        ignores it and does not touch the venue (V1).
        """
        if not isinstance(request, TdOmsViewRequest):
            raise TypeError("view expects TdOmsViewRequest")
        _timeout(timeout)
        raise NotImplementedError(TICKET)

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
        """Dispatch one ledger read. B6-02."""
        raise NotImplementedError(TICKET)

    async def view(self, request: TdLedgerViewRequest) -> LedgerView:
        """The ledger in memory. Must not wait on the venue. B6-02.

        ``asset`` set names one row; omitted, the whole ledger.
        """
        if not isinstance(request, TdLedgerViewRequest):
            raise TypeError("view expects TdLedgerViewRequest")
        raise NotImplementedError(TICKET)

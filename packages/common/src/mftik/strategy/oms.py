"""Strategy-side OMS mirror — snapshots + order entry by client_order_id."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mftik.broker.errors import RequestTimeoutError
from mftik.exchange.models import (
    Order,
    OrderType,
    Side,
    TimeInForce,
)
from mftik.exchange.oms import OmsView
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    TD_OMS_ORDER,
    TD_OMS_VIEW,
    Envelope,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    TdOmsOrderRequest,
    TdOmsViewRequest,
    Topics,
)
from mftik.protocol.reject_codes import describe
from mftik.strategy.client_order_id import ClientOrderIdFactory
from mftik.strategy.eventlog import session_log

if TYPE_CHECKING:
    from mftik.strategy.base import Strategy

logger = logging.getLogger(__name__)

#: An ack is answered before TD touches the venue, so it should come back in
#: about one broker round-trip. Generous enough to ride out a GC pause without
#: leaving a strategy blocked for long.
ORDER_ACK_TIMEOUT_S = 2.0

#: Reject codes where the venue outcome is unknown. The cid stays inflight
#: so a cancel is still refused — the order may be resting.
_TRANSPORT_AMBIGUOUS = frozenset(
    {
        RejectCode.TD_SEND_FAILED,
        RejectCode.TD_NO_ACK,
        RejectCode.TD_VENUE_NOT_CONNECTED,
    }
)


class StrategyOms:
    """Order entry, and reads of TD's book over ``td.account.{api_id}``.

    ``view`` and ``order`` are request-reply against the TD that holds the
    account. One writer, one picture. A miss on ``order`` after submit is
    still a money bug — it is a miss on the authority, not a lagging cache.

    ``on_order_update`` and friends are the cue to go and look, not a
    stream to fold into a private mirror — rebuilding state from a fan-out
    that other sessions also feed is how the two sides drift apart.

    Order entry is request-reply on ``td.order.{api_id}``: submit and cancel
    both wait for TD's ack and return whether it took the request. Venue
    outcomes are separate and still arrive asynchronously on
    ``td.{api_id}.global`` as order updates, fills or rejects — a ``True`` here
    does **not** mean the venue accepted anything. A submit TD accepts is
    pending until :meth:`note_order` sees a non-pending status;
    :meth:`cancel_order` will not send for those cids.

    ``submit_order`` mints the uint64 ``client_order_id`` itself::

        [ver 4][session_id 24][ts_sec_from_2026-01-01 28][seq 8]

    and leaves it in :attr:`last_client_order_id` for the caller to keep.
    """

    def __init__(self, *, ack_timeout: float = ORDER_ACK_TIMEOUT_S) -> None:
        self._strategy: Strategy | None = None
        self._cid_factory: ClientOrderIdFactory | None = None
        self._ack_timeout = ack_timeout
        self._last_cid: str | None = None
        self._last_reason: str = ""
        self._last_code: int | str = RejectCode.NONE
        #: Cids whose submit or cancel is still on the wire. Not a book
        #: mirror — only what this session itself sent and has not seen
        #: leave :meth:`~mftik.exchange.models.OrderStatus.is_pending`.
        self._inflight: set[str] = set()
        #: Cids that have already left pending for good. A late
        #: ``PENDING_NEW`` snapshot must not revive them — reject can
        #: overtake the local book announcement.
        self._done: set[str] = set()

    def bind(self, strategy: Strategy) -> None:
        if strategy.session is None:
            raise RuntimeError("strategy OMS bind requires a session")
        self._strategy = strategy
        self._cid_factory = None
        self._inflight.clear()
        self._done.clear()

    def is_inflight(self, client_order_id: str | int) -> bool:
        """Whether this session's submit or cancel for ``cid`` is still open."""
        return str(client_order_id) in self._inflight

    def note_order(self, order: Order) -> None:
        """Fold a venue (or TD) status into the pending set.

        Pending statuses stay marked; anything else — working, terminal,
        ``UNKNOWN`` — means a cancel may now be sent.
        """
        cid = order.client_order_id
        if not cid:
            return
        key = str(cid)
        if not order.status.is_pending():
            self._inflight.discard(key)
            self._done.add(key)
            return
        if key not in self._done:
            self._inflight.add(key)

    def note_gone(self, client_order_id: str | int | None) -> None:
        """The cid is no longer inflight — reject, or a cancel that failed."""
        if client_order_id is None:
            return
        key = str(client_order_id)
        self._inflight.discard(key)
        self._done.add(key)

    def note_reject(
        self,
        error_code: int | str | None,
        client_order_id: str | int | None,
    ) -> None:
        """Clear inflight only for a determined refuse.

        Transport ambiguity must not look like "gone": the submit may have
        landed, and a cancel then would have to wait for UNKNOWN / recovery.
        """
        if error_code in _TRANSPORT_AMBIGUOUS:
            return
        self.note_gone(client_order_id)

    def clear_inflight(self) -> None:
        """Drop every inflight mark. Used on rebuild / bind."""
        self._inflight.clear()
        self._done.clear()

    def _mark_inflight(self, cid: str) -> None:
        if cid not in self._done:
            self._inflight.add(cid)

    def _next_client_order_id(self) -> str:
        if self._cid_factory is None:
            session = self._require_session()
            self._cid_factory = ClientOrderIdFactory(session.session_id)
        return self._cid_factory.next()

    async def view(self, api_id: int | None = None) -> OmsView:
        """Read TD's live book for ``api_id`` over ``td.account``."""
        resolved = self._resolve(api_id)
        log = session_log(self._strategy)
        if resolved is None:
            log.record("read", "oms.view", dir="out", resolved=False, count=0)
            return OmsView()
        session = self._require_session()
        reply = await session.broker.request(
            Topics.td_account(resolved),
            Envelope[TdOmsViewRequest].wrap(
                TdOmsViewRequest(api_id=resolved),
                type=TD_OMS_VIEW,
                source=_source_name(session),
                session_id=getattr(session, "session_id", None),
            ),
            timeout=self._ack_timeout,
        )
        view = OmsView.model_validate(reply.payload or {})
        log.record(
            "read",
            "oms.view",
            dir="out",
            api_id=resolved,
            count=len(view.orders),
            payload=view.model_dump(mode="json"),
        )
        return view

    async def orders(self, api_id: int | None = None) -> dict[str, Order]:
        """Live orders keyed by ``client_order_id``."""
        return dict((await self.view(api_id)).orders)

    async def order(
        self, client_order_id: str | int, api_id: int | None = None
    ) -> Order | None:
        """One order by ``client_order_id``, or None if it is not live.

        Always a direct read of TD memory. A miss here is a money bug.
        """
        resolved = self._resolve(api_id)
        log = session_log(self._strategy)
        cid = str(client_order_id)
        if resolved is None:
            log.record("read", "oms.order", dir="out", cid=cid, resolved=False)
            return None
        session = self._require_session()
        reply = await session.broker.request(
            Topics.td_account(resolved),
            Envelope[TdOmsOrderRequest].wrap(
                TdOmsOrderRequest(api_id=resolved, client_order_id=cid),
                type=TD_OMS_ORDER,
                source=_source_name(session),
                session_id=getattr(session, "session_id", None),
            ),
            timeout=self._ack_timeout,
        )
        payload = reply.payload or {}
        found = bool(payload)
        log.record(
            "read",
            "oms.order",
            dir="out",
            api_id=resolved,
            cid=cid,
            found=found,
            payload=payload or None,
        )
        return None if not found else Order.model_validate(payload)

    def _resolve(self, api_id: int | None) -> int | None:
        """Pick the account: the one asked for, or the only one attached.

        ``None`` when the session has zero or several accounts — ``td: {}``
        is a legal MD-only run, and guessing among several would pick the
        wrong book. :meth:`~mftik.strategy.session.SessionView.td_sole`
        raises in those cases; this path must not. Same shape as
        :meth:`mftik.strategy.ledger.StrategyLedger._resolve`.
        """
        if api_id is not None:
            return api_id
        attached = self.api_ids
        return attached[0] if len(attached) == 1 else None

    def _session_broker(self):
        return self._require_session().broker

    @property
    def api_ids(self) -> list[int]:
        session = self._strategy.session if self._strategy is not None else None
        return list(session.td_api_ids) if session is not None else []

    @property
    def last_client_order_id(self) -> str | None:
        """The ``client_order_id`` minted by the most recent submit.

        Set just before :meth:`submit_order` returns, on refusals too — the id
        is what correlates a failure with the venue events that may still show
        up. Read it on the line after the submit: there is no await in between,
        so a concurrent submit cannot interleave and clobber it.
        """
        return self._last_cid

    @property
    def last_reject_reason(self) -> str:
        """Why TD refused the most recent request, or ``""`` if it took it.

        TD's refusals are standing conditions, not transient ones — no TD
        serving the account, the session not attached, the balance not there.
        A strategy that retries one on a timer will retry it forever, so read
        this and stop rather than re-submitting.
        """
        return self._last_reason

    @property
    def last_reject_code(self) -> int | str:
        """:attr:`last_reject_reason` as a code — see
        :mod:`mftik.protocol.reject_codes`.

        ``RejectCode.NONE`` when the last request was taken. Everything on
        this path is a TD refusal, so the code is always in the ``1xx`` band:
        branch on it rather than matching on the reason text, which is
        free-form and will drift.
        """
        return self._last_code

    async def submit_order(
        self,
        api_id: int,
        *,
        ticker: UniversalTicker | str,
        side: Side,
        qty: Decimal | None = None,
        quote_qty: Decimal | None = None,
        type: OrderType = OrderType.LIMIT,
        price: Decimal | None = None,
        tif: TimeInForce | None = None,
        reduce_only: bool = False,
    ) -> bool:
        """Submit an order via TD. True if TD accepted the request.

        Size is ``qty`` (base) or, on a market order, ``quote_qty`` (quote).
        Exactly one. TD refuses a pairing the venue cannot express
        (``TD_UNSUPPORTED_ORDER_SHAPE``) before anything is reserved.

        ``ticker`` names the instrument — ``Bybit_Perp_BTCUSDT``, not
        ``BTCUSDT``. ``api_id`` says which account, which on a unified venue
        does not say which book, and a strategy already holds the ticker: it
        came from the feed key it subscribed to. TD refuses one belonging to a
        venue this account does not trade, so sending a Binance ticker to a
        Bybit session is caught rather than routed somewhere plausible.

        False means the order never reached the venue — no ack, a refusal, or
        no TD holding ``api_id``. It says nothing about whether the venue
        would have filled it. The minted id is in
        :attr:`last_client_order_id` either way.

        ``tif`` left at ``None`` takes the adapter's default for this order
        type. :attr:`TimeInForce.POST_ONLY` asks the venue to refuse rather
        than cross, which arrives as a rejection, not a fill — so a caller
        using it has to handle being turned down as a normal outcome.

        ``reduce_only`` asks the venue to refuse this order rather than let it
        open or extend a position. It is the guarantee a strategy closing out
        wants: a size computed from its own tally of fills can exceed what the
        account actually holds — funding, ADL and liquidation move a position
        without ever arriving as a fill — and without this, the excess opens
        the opposite position instead of being turned away.

        Contract markets only. TD refuses a spot order carrying it
        (``TD_REDUCE_ONLY_UNSUPPORTED``) rather than dropping the flag, so a
        caller is never told True for an order it believes is protected and
        is not.
        """
        session = self._require_session()
        cid = self._next_client_order_id()
        accepted = await self._request_ack(
            api_id,
            cid,
            Envelope[OrderSubmit].wrap(
                OrderSubmit(
                    session_id=session.session_id,
                    api_id=api_id,
                    universal_ticker=str(ticker),
                    side=side,
                    type=type,
                    qty=qty,
                    quote_qty=quote_qty,
                    price=price,
                    tif=tif,
                    reduce_only=reduce_only,
                    client_order_id=cid,
                ),
                type=STS_ORDER_SUBMIT,
                source=f"strategy.{session.strategy.name}",
                session_id=session.session_id,
            ),
        )
        # After the await, not before: a submit that overlapped ours would
        # otherwise leave its cid here for our caller to read.
        self._last_cid = cid
        # The reject can be processed before this line runs: TD acks,
        # then the venue refuses, and that pub/sub lands while we are
        # still inside ``_request_ack``. Do not revive a settled cid.
        if accepted:
            self._mark_inflight(cid)
        return accepted

    async def cancel_order(self, api_id: int, client_order_id: str | int) -> bool:
        """Cancel an open order by ``client_order_id`` via TD.

        True if TD accepted the request; the venue's answer still arrives
        asynchronously as an order update or a cancel reject.

        Refuses locally when the cid is still inflight — same outcome as
        TD's ``TD_NOT_CANCELABLE``, without the round-trip or the warn.
        """
        cid = str(client_order_id)
        if cid in self._inflight:
            self._last_reason = (
                "order is inflight; it cannot be cancelled from that state"
            )
            self._last_code = RejectCode.TD_NOT_CANCELABLE
            return False
        session = self._require_session()
        accepted = await self._request_ack(
            api_id,
            cid,
            Envelope[OrderCancel].wrap(
                OrderCancel(
                    session_id=session.session_id,
                    api_id=api_id,
                    client_order_id=cid,
                ),
                type=STS_ORDER_CANCEL,
                source=f"strategy.{session.strategy.name}",
                session_id=session.session_id,
            ),
        )
        if accepted:
            self._mark_inflight(cid)
        return accepted

    async def _request_ack(
        self, api_id: int, cid: str, envelope: Envelope[Any]
    ) -> bool:
        """Round-trip an order request to TD and report whether it was taken."""
        session = self._require_session()
        log = session.event_log
        # Before the request, not after it. A submit that is never answered is
        # the case the log is most needed for, and one recorded on the way back
        # would have nothing to say about it.
        log.record(
            "order",
            envelope.type,
            dir="out",
            api_id=api_id,
            cid=cid,
            payload=envelope.payload,
        )
        # Cleared up front so a stale reason cannot outlive the refusal it
        # described and be read against a later, accepted request.
        self._last_reason = ""
        self._last_code = RejectCode.NONE
        try:
            reply = await session.broker.request(
                Topics.td_order(api_id), envelope, timeout=self._ack_timeout
            )
        except RequestTimeoutError:
            # No TD is serving this account, or it is wedged. Either way the
            # request did not land: say so rather than letting the strategy
            # wait on venue events that will never arrive.
            logger.warning(
                "TD order ack timed out api_id=%s cid=%s type=%s",
                api_id,
                cid,
                envelope.type,
            )
            self._last_reason = "no ack from TD"
            self._last_code = RejectCode.TD_NO_ACK
            log.record(
                "order",
                "order_ack",
                api_id=api_id,
                cid=cid,
                accepted=False,
                code=self._last_code,
                reason=self._last_reason,
            )
            return False

        try:
            ack = OrderAck.model_validate(reply.payload)
        except Exception:
            logger.warning(
                "TD order ack unreadable api_id=%s cid=%s reply=%s",
                api_id,
                cid,
                reply.type,
            )
            self._last_reason = "unreadable ack from TD"
            self._last_code = RejectCode.TD_UNREADABLE_ACK
            log.record(
                "order",
                "order_ack",
                api_id=api_id,
                cid=cid,
                accepted=False,
                code=self._last_code,
                reason=self._last_reason,
                sent_ts=reply.ts,
            )
            return False

        if not ack.accepted:
            logger.warning(
                "TD refused order api_id=%s cid=%s code=%s: %s",
                api_id,
                cid,
                describe(ack.error_code),
                ack.reason,
            )
            self._last_reason = ack.reason
            self._last_code = ack.error_code
        log.record(
            "order",
            "order_ack",
            api_id=api_id,
            cid=cid,
            accepted=ack.accepted,
            code=ack.error_code,
            reason=ack.reason or None,
            sent_ts=reply.ts,
        )
        return ack.accepted

    def _require_session(self):
        if self._strategy is None or self._strategy.session is None:
            raise RuntimeError("strategy OMS is not bound to a session")
        return self._strategy.session


def _source_name(session: object) -> str:
    strategy = getattr(session, "strategy", None)
    name = getattr(strategy, "name", None)
    return f"strategy.{name}" if name else "sts"

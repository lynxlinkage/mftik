"""Strategy-side OMS mirror — snapshots + order entry by client_order_id."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from mftik.broker.errors import RequestTimeoutError
from mftik.exchange.models import Order, OrderType, Side, TimeInForce
from mftik.exchange.oms import OmsView
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    Envelope,
    OrderAck,
    OrderCancel,
    OrderSubmit,
    RejectCode,
    Topics,
)
from mftik.protocol.reject_codes import describe

from mftik_sts.client_order_id import ClientOrderIdFactory
from mftik_sts.eventlog import session_log

if TYPE_CHECKING:
    from mftik_sts.strategy import Strategy

logger = logging.getLogger(__name__)

#: An ack is answered before TD touches the venue, so it should come back in
#: about one Redis round-trip. Generous enough to ride out a GC pause without
#: leaving a strategy blocked for long.
ORDER_ACK_TIMEOUT_S = 2.0


class StrategyOms:
    """Order entry, and reads of TD's book out of Redis.

    There is no local copy of the book here. TD writes ``td.oms.{api_id}``
    (client_order_id → Order) and every read below goes to it, so what a
    strategy sees is always what TD sees. ``on_order_update`` and friends are
    the cue to go and look, not a stream to fold into a private mirror —
    rebuilding state from a fan-out that other sessions also feed is how the
    two sides drift apart.

    Order entry is request-reply on ``td.order.{api_id}``: submit and cancel
    both wait for TD's ack and return whether it took the request. Venue
    outcomes are separate and still arrive asynchronously on
    ``td.{api_id}.global`` as order updates, fills or rejects — a ``True`` here
    does **not** mean the venue accepted anything.

    ``submit_order`` mints the uint64 ``client_order_id`` itself::

        [session slot 16][ts_ms_from_2026-01-01 40][seq 8]

    and leaves it in :attr:`last_client_order_id` for the caller to keep.
    """

    def __init__(self, *, ack_timeout: float = ORDER_ACK_TIMEOUT_S) -> None:
        self._strategy: Strategy | None = None
        self._cid_factory: ClientOrderIdFactory | None = None
        self._ack_timeout = ack_timeout
        self._last_cid: str | None = None
        self._last_reason: str = ""
        self._last_code: int | str = RejectCode.NONE

    def bind(self, strategy: Strategy, *, cid_slot: int) -> None:
        self._strategy = strategy
        self._cid_factory = ClientOrderIdFactory(cid_slot)

    async def view(self, api_id: int | None = None) -> OmsView:
        """Read TD's live book for ``api_id`` out of ``td.oms.{api_id}``.

        Always a read: STS keeps no copy. TD is the only writer, so whatever
        comes back here is what TD believes right now, and two strategies on
        the same account cannot drift apart by each maintaining their own.
        """
        resolved = self._resolve(api_id)
        log = session_log(self._strategy)
        if resolved is None:
            # An empty book and an unanswerable question look identical to the
            # caller, so the log has to be the thing that tells them apart.
            log.record("read", "oms.view", dir="out", resolved=False, count=0)
            return OmsView()
        rows = await self._session_broker().state_all(Topics.td_oms(resolved))
        # The rows, not the view built from them. This read is an input to
        # whatever the strategy did next, and only the answer it was actually
        # given can stand in for TD's book after the fact.
        log.record(
            "read",
            "oms.view",
            dir="out",
            api_id=resolved,
            count=len(rows),
            payload=rows,
        )
        orders = {
            cid: Order.model_validate(row) for cid, row in rows.items()
        }
        return OmsView(orders=orders)

    async def orders(self, api_id: int | None = None) -> dict[str, Order]:
        """Live orders keyed by ``client_order_id``."""
        return dict((await self.view(api_id)).orders)

    async def order(
        self, client_order_id: str | int, api_id: int | None = None
    ) -> Order | None:
        """One order by ``client_order_id``, or None if it is not live."""
        resolved = self._resolve(api_id)
        log = session_log(self._strategy)
        cid = str(client_order_id)
        if resolved is None:
            log.record(
                "read", "oms.order", dir="out", cid=cid, resolved=False
            )
            return None
        row = await self._session_broker().state_get(
            Topics.td_oms(resolved), cid
        )
        # ``found`` rather than inferring it from a missing payload: an order
        # that is no longer live is a different answer from one nobody asked
        # about, and both arrive here as None.
        log.record(
            "read",
            "oms.order",
            dir="out",
            api_id=resolved,
            cid=cid,
            found=row is not None,
            payload=row,
        )
        return None if row is None else Order.model_validate(row)

    def _resolve(self, api_id: int | None) -> int | None:
        """Pick the account: the one asked for, or the only one attached."""
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

    def _next_client_order_id(self) -> str:
        if self._cid_factory is None:
            raise RuntimeError("strategy OMS is not bound to a session")
        return self._cid_factory.next()

    async def submit_order(
        self,
        api_id: int,
        *,
        ticker: UniversalTicker | str,
        side: Side,
        qty: Decimal,
        type: OrderType = OrderType.LIMIT,
        price: Decimal | None = None,
        tif: TimeInForce | None = None,
        reduce_only: bool = False,
    ) -> bool:
        """Submit an order via TD. True if TD accepted the request.

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
        return accepted

    async def cancel_order(self, api_id: int, client_order_id: str | int) -> bool:
        """Cancel an open order by ``client_order_id`` via TD.

        True if TD accepted the request; the venue's answer still arrives
        asynchronously as an order update or a cancel reject.
        """
        session = self._require_session()
        cid = str(client_order_id)
        return await self._request_ack(
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

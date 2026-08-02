"""Strategy-side OMS mirror — snapshots + order entry by client_order_id."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from mft.exchange.models import OrderType, Side
from mft.exchange.oms import OmsView
from mft.protocol import (
    STS_ORDER_CANCEL,
    STS_ORDER_SUBMIT,
    Envelope,
    OrderCancel,
    OrderSubmit,
    Topics,
)

from mft_sts.client_order_id import ClientOrderIdFactory

if TYPE_CHECKING:
    from mft_sts.strategy import Strategy


class StrategyOms:
    """Read API for reconciled / live OMS state, keyed by TD ``api_id``.

    Order entry publishes STS → TD commands on ``sts.{session_id}``; venue
    events return on ``td.{api_id}.global``.

    ``submit_order`` always mints a uint64 ``client_order_id``::

        [session slot 16][ts_ms_from_2026-01-01 40][seq 8]
    """

    def __init__(self) -> None:
        self._views: dict[int, OmsView] = {}
        self._strategy: Strategy | None = None
        self._cid_factory: ClientOrderIdFactory | None = None

    def bind(self, strategy: Strategy, *, cid_slot: int) -> None:
        self._strategy = strategy
        self._cid_factory = ClientOrderIdFactory(cid_slot)

    def update(self, api_id: int, view: OmsView) -> None:
        self._views[api_id] = view

    def get(self, api_id: int) -> OmsView | None:
        return self._views.get(api_id)

    def __getitem__(self, api_id: int) -> OmsView:
        return self._views[api_id]

    def __contains__(self, api_id: object) -> bool:
        return isinstance(api_id, int) and api_id in self._views

    @property
    def api_ids(self) -> list[int]:
        return list(self._views)

    def view(self, api_id: int | None = None) -> OmsView | None:
        """Return OMS for ``api_id``, or the sole account if only one is present."""
        if api_id is not None:
            return self._views.get(api_id)
        if len(self._views) == 1:
            return next(iter(self._views.values()))
        return None

    async def submit_order(
        self,
        api_id: int,
        *,
        symbol: str,
        side: Side,
        qty: Decimal,
        type: OrderType = OrderType.LIMIT,
        price: Decimal | None = None,
    ) -> str:
        """Submit an order via TD; returns the minted uint64 ``client_order_id``."""
        session = self._require_session()
        if self._cid_factory is None:
            raise RuntimeError("strategy OMS is not bound to a session")
        cid = self._cid_factory.next()
        await session.broker.publish(
            Topics.sts_td_session(session.session_id),
            Envelope[OrderSubmit].wrap(
                OrderSubmit(
                    session_id=session.session_id,
                    api_id=api_id,
                    symbol=symbol,
                    side=side,
                    type=type,
                    qty=qty,
                    price=price,
                    client_order_id=cid,
                ),
                type=STS_ORDER_SUBMIT,
                source=f"strategy.{session.strategy.name}",
                session_id=session.session_id,
            ),
        )
        return cid

    async def cancel_order(self, api_id: int, client_order_id: str | int) -> None:
        """Cancel an open order by ``client_order_id`` via TD."""
        session = self._require_session()
        await session.broker.publish(
            Topics.sts_td_session(session.session_id),
            Envelope[OrderCancel].wrap(
                OrderCancel(
                    session_id=session.session_id,
                    api_id=api_id,
                    client_order_id=str(client_order_id),
                ),
                type=STS_ORDER_CANCEL,
                source=f"strategy.{session.strategy.name}",
                session_id=session.session_id,
            ),
        )

    def _require_session(self):
        if self._strategy is None or self._strategy.session is None:
            raise RuntimeError("strategy OMS is not bound to a session")
        return self._strategy.session

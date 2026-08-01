"""Order Management System — owns live orders / positions / balances."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING

from mft.exchange.models import Balance, Fill, Order, OrderStatus, Side
from mft.exchange.oms import OmsView, Position

if TYPE_CHECKING:
    from mft_td.session.session import Session

logger = logging.getLogger(__name__)

UpdateHook = Callable[[OmsView], None]


class Oms:
    """In-process book of record for one trading session.

    OMS registers callbacks on the :class:`Session`; the session forwards
    exchange events via those callbacks. Session reads :meth:`view` to publish.
    """

    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}
        self._positions: dict[str, Decimal] = {}
        self._balances: dict[str, Balance] = {}
        self._hooks: list[UpdateHook] = []

    def bind(self, session: Session) -> None:
        """Register OMS handlers on the session's exchange event bus."""
        session.on_order(self.handle_order)
        session.on_fill(self.handle_fill)
        session.on_balance(self.handle_balance)

    def on_update(self, hook: UpdateHook) -> None:
        """Notify when OMS state changes (Session uses this to publish)."""
        self._hooks.append(hook)

    def view(self) -> OmsView:
        return OmsView(
            orders=dict(self._orders),
            positions={
                symbol: Position(symbol=symbol, qty=qty)
                for symbol, qty in self._positions.items()
                if qty != 0
            },
            balances=dict(self._balances),
        )

    def apply_reconcile(
        self,
        *,
        orders: Sequence[Order],
        balances: Sequence[Balance],
        positions: Sequence[Position] | None = None,
    ) -> OmsView:
        """Replace OMS books from venue snapshots (full recon)."""
        self._orders = {
            o.order_id: o
            for o in orders
            if o.status
            not in (
                OrderStatus.FILLED,
                OrderStatus.CANCELED,
                OrderStatus.REJECTED,
            )
        }
        self._balances = {b.asset: b for b in balances}
        if positions is not None:
            self._positions = {
                p.symbol: p.qty for p in positions if p.qty != 0
            }
        logger.info(
            "OMS reconciled orders=%s balances=%s positions=%s",
            len(self._orders),
            len(self._balances),
            len(self._positions),
        )
        return self._emit()

    def handle_order(self, order: Order) -> None:
        if order.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
        ):
            self._orders.pop(order.order_id, None)
        else:
            self._orders[order.order_id] = order

        logger.debug("OMS order id=%s status=%s", order.order_id, order.status)
        self._emit()

    def handle_fill(self, fill: Fill) -> None:
        signed = fill.qty if fill.side is Side.BUY else -fill.qty
        self._positions[fill.symbol] = (
            self._positions.get(fill.symbol, Decimal("0")) + signed
        )
        logger.debug(
            "OMS fill symbol=%s qty=%s side=%s",
            fill.symbol,
            fill.qty,
            fill.side,
        )
        self._emit()

    def handle_balance(self, balance: Balance) -> None:
        self._balances[balance.asset] = balance
        logger.debug("OMS balance asset=%s free=%s", balance.asset, balance.free)
        self._emit()

    def _emit(self) -> OmsView:
        snap = self.view()
        for hook in list(self._hooks):
            hook(snap)
        return snap

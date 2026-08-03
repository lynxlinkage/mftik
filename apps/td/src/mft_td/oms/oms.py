"""Order Management System — owns live orders / positions / balances."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING

from mft.exchange.models import Balance, Fill, Order, Side, is_open, is_terminal
from mft.exchange.oms import OmsView, Position

if TYPE_CHECKING:
    from mft_td.session.session import Session

logger = logging.getLogger(__name__)


def order_key(order: Order) -> str:
    """Book key for an order: its ``client_order_id`` when it has one.

    Recon turns up orders TD never sent (placed elsewhere, or before a
    restart); those fall back to the venue id so they stay visible instead of
    collapsing onto a single entry.
    """
    return order.client_order_id or order.order_id

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
        self._orders = {order_key(o): o for o in orders if is_open(o.status)}
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

    def get_order(self, key: str) -> Order | None:
        """Look an order up by ``client_order_id``, falling back to order_id."""
        return self._orders.get(key)

    def handle_order(self, order: Order) -> None:
        # Keyed by client_order_id so an order we minted is findable from the
        # moment it is created — before the venue has given it an id at all.
        key = order_key(order)
        if is_terminal(order.status):
            self._orders.pop(key, None)
        else:
            self._orders[key] = order

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

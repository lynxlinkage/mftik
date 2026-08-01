"""Trading session — exchange connectivity + OMS pub/sub."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from mft.broker import Broker
from mft.exchange.base import PrivateClient
from mft.exchange.models import Balance, Fill, Order
from mft.exchange.oms import OmsView, Position
from mft.protocol import TD_OMS_VIEW, Envelope, Topics

from mft_td.oms import Oms

logger = logging.getLogger(__name__)

OrderCallback = Callable[[Order], None]
FillCallback = Callable[[Fill], None]
BalanceCallback = Callable[[Balance], None]


class Session:
    """Shared exchange session keyed by API id.

    Lifecycle:
    1. Created / started when first STS attaches (refcount 0→1).
    2. Further STS sessions attach via lease; OMS publishes on ``td.oms.{api_id}``.
    3. Last detach destroys the trading session.
    """

    def __init__(
        self,
        *,
        api_id: int,
        broker: Broker,
        private: PrivateClient,
        oms: Oms | None = None,
    ) -> None:
        self.api_id = api_id
        self.broker = broker
        self.private = private
        self.oms = oms or Oms()
        self._order_cbs: list[OrderCallback] = []
        self._fill_cbs: list[FillCallback] = []
        self._balance_cbs: list[BalanceCallback] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._started = False
        self._destroyed = False

        self.oms.bind(self)
        self.oms.on_update(self._on_oms_update)

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    def on_order(self, cb: OrderCallback) -> None:
        self._order_cbs.append(cb)

    def on_fill(self, cb: FillCallback) -> None:
        self._fill_cbs.append(cb)

    def on_balance(self, cb: BalanceCallback) -> None:
        self._balance_cbs.append(cb)

    async def start(self) -> None:
        """Connect private venue client, seed OMS, and begin stream pumps."""
        if self._started:
            return
        await self.private.connect()
        self._started = True

        await self.reconcile()

        order_stream = self.private.stream_orders()
        fill_stream = self.private.stream_fills()
        balance_stream = self.private.stream_balances()
        self._tasks = [
            asyncio.create_task(
                self._pump(order_stream, self._dispatch_order),
                name=f"sess-{self.api_id}-orders",
            ),
            asyncio.create_task(
                self._pump(fill_stream, self._dispatch_fill),
                name=f"sess-{self.api_id}-fills",
            ),
            asyncio.create_task(
                self._pump(balance_stream, self._dispatch_balance),
                name=f"sess-{self.api_id}-balances",
            ),
        ]
        logger.info("Session started api_id=%s", self.api_id)

    async def reconcile(self) -> OmsView:
        """Query venue open orders / positions / balances into OMS and publish."""
        if self._destroyed:
            raise RuntimeError(f"session api_id={self.api_id} is destroyed")
        orders = await self.private.fetch_open_orders()
        balances = await self.private.fetch_balances()
        positions: list[Position] | None = None
        if type(self.private).fetch_positions is not PrivateClient.fetch_positions:
            positions = list(await self.private.fetch_positions())

        view = self.oms.apply_reconcile(
            orders=orders,
            balances=balances,
            positions=positions,
        )
        await self.publish_oms(view)
        return view

    async def publish_oms(self, view: OmsView | None = None) -> None:
        """Publish OMS snapshot to ``td.oms.{api_id}``."""
        snap = view if view is not None else self.oms.view()
        await self.broker.publish(
            Topics.td_oms(self.api_id),
            Envelope[OmsView].wrap(
                snap,
                type=TD_OMS_VIEW,
                source="td",
                session_id=str(self.api_id),
            ),
        )

    async def destroy(self) -> None:
        """Tear down exchange pumps and private client."""
        if self._destroyed:
            return
        self._destroyed = True

        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self.private.connected:
            await self.private.close()
        self._started = False
        logger.info("Session destroyed api_id=%s", self.api_id)

    async def _pump(self, stream: Any, dispatch: Callable[[Any], None]) -> None:
        try:
            async for item in stream:
                dispatch(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("venue pump failed api_id=%s", self.api_id)

    def _dispatch_order(self, order: Order) -> None:
        for cb in list(self._order_cbs):
            cb(order)

    def _dispatch_fill(self, fill: Fill) -> None:
        for cb in list(self._fill_cbs):
            cb(fill)

    def _dispatch_balance(self, balance: Balance) -> None:
        for cb in list(self._balance_cbs):
            cb(balance)

    def _on_oms_update(self, view: OmsView) -> None:
        if self._destroyed or not self._started:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._safe_publish(view))

    async def _safe_publish(self, view: OmsView) -> None:
        try:
            await self.publish_oms(view)
        except Exception:
            logger.exception("failed to publish OMS view api_id=%s", self.api_id)

"""Place one limit buy, then cancel it once the exchange has it on the book.

``InitPayload.params`` is the strategy's own JSON. Price, size, account, and
instrument all come from there. This file does not name a venue or a product.

.. code-block:: json

   {"ticker": "Ex_Spot_ABC", "api_id": 7, "algo_id": 1, "qty": 1, "price": 10}

The first ``on_recon`` reads ``position`` and ``open_orders``. If that algo
already has orders (an earlier incarnation's, or the exchange's), it
``batch_cancel``s them, then ``submit``s one limit buy. ``on_order`` calls
``cancel`` when that order is accepted. ``on_fill`` logs the fill. Each of
those is one JSON line on stdout.
"""

from __future__ import annotations

import json
import math
import sys
from typing import Any

import mftik
from mftik.common.v1 import OrderType, Side, TimeInForce
from mftik.strategy import Strategy
from mftik.td.v1 import OrderStatus

__all__ = ["Maker"]

_I64_MAX = 2**63 - 1
_ALGO_MAX = 1023


class _Params:
    def __init__(self, ticker: str, api_id: int, algo_id: int, qty: float, price: float) -> None:
        self.ticker = ticker
        self.api_id = api_id
        self.algo_id = algo_id
        self.qty = qty
        self.price = price


def _as_int(value: object, low: int, high: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < low or value > high:
        raise ValueError(f"maker: {name} is not an integer in {low}..{high}")
    return value


def _as_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"maker: {name} is not a number")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"maker: {name} must be a finite number above 0")
    return number


def _parse(raw: bytes) -> _Params:
    if not raw:
        raise ValueError("maker: params are required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"maker: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("maker: params must be a JSON object")
    ticker = data.get("ticker", "")
    if not isinstance(ticker, str) or not ticker:
        raise ValueError("maker: ticker is required")
    for name in ("api_id", "algo_id", "qty", "price"):
        if name not in data:
            raise ValueError(f"maker: {name} is required")
    api_id = _as_int(data["api_id"], 1, _I64_MAX, "api_id")
    algo_id = _as_int(data["algo_id"], 1, _ALGO_MAX, "algo_id")
    return _Params(
        ticker,
        api_id,
        algo_id,
        _as_float(data["qty"], "qty"),
        _as_float(data["price"], "price"),
    )


def _log(fields: dict[str, Any]) -> None:
    payload = {key: value for key, value in fields.items() if value is not None}
    sys.stdout.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=True) + "\n")
    sys.stdout.flush()


class Maker(Strategy):
    """One resting limit buy, then a cancel, selected by ``InitPayload.params``."""

    def __init__(self) -> None:
        self._ticker = ""
        self._api_id = 0
        self._algo_id = 0
        self._uid = 0
        self._qty = 0.0
        self._price = 0.0
        self._placed = False
        self._cloid = ""
        self._cancelled = False

    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        info = ctx.resolve(params.ticker)
        self._ticker = params.ticker
        self._api_id = params.api_id
        self._algo_id = params.algo_id
        self._uid = info.uid
        self._qty = params.qty
        self._price = params.price
        _log({"hook": "OnInit", "ticker": params.ticker, "uid": info.uid})

    def on_start(self, _ctx: mftik.Context) -> None:
        _log({"hook": "OnStart", "uid": self._uid})

    def on_stop(self, _ctx: mftik.Context) -> None:
        _log({"hook": "OnStop", "cloid": self._cloid})

    def on_recon(self, ctx: mftik.Context, ev: mftik.ReconEvent) -> None:
        if ev.td.api_id != self._api_id:
            return
        position = ctx.position(self._api_id, self._uid)
        orders = ctx.open_orders(self._api_id, self._algo_id)
        ids = [order.client_order_id for order in orders]
        _log(
            {
                "hook": "OnRecon",
                "api_id": self._api_id,
                "uid": self._uid,
                "position": None if position is None else position.size,
                "open": ids,
            }
        )
        if ids and not self._placed:
            try:
                ctx.batch_cancel(self._api_id, self._algo_id)
            except mftik.Error as exc:
                _log({"hook": "batch_cancel_error", "reason": str(exc)})
                return
            _log({"hook": "batch_cancel", "open": len(ids)})
        if self._placed:
            return
        try:
            cloid = ctx.submit(
                mftik.Order(
                    api_id=self._api_id,
                    algo_id=self._algo_id,
                    uid=self._uid,
                    side=Side.SIDE_BUY,
                    order_type=OrderType.ORDER_TYPE_LIMIT,
                    tif=TimeInForce.TIME_IN_FORCE_GTC,
                    qty=self._qty,
                    price=self._price,
                )
            )
        except mftik.Error as exc:
            _log({"hook": "submit_error", "reason": str(exc)})
            return
        self._placed = True
        self._cloid = cloid
        _log({"hook": "submit", "cloid": cloid, "qty": self._qty, "price": self._price})

    def on_order(self, ctx: mftik.Context, ev: mftik.OrderEvent) -> None:
        cloid = ev.order.client_order_id
        if cloid != self._cloid or not self._cloid:
            return
        _log({"hook": "OnOrder", "cloid": cloid, "status": int(ev.order.status)})
        if ev.order.status != OrderStatus.ORDER_STATUS_ACCEPTED or self._cancelled:
            return
        self._cancelled = True
        try:
            ctx.cancel(self._api_id, cloid)
        except mftik.Error as exc:
            _log({"hook": "cancel_error", "cloid": cloid, "reason": str(exc)})
            self._cancelled = False
            return
        _log({"hook": "cancel", "cloid": cloid})

    def on_fill(self, _ctx: mftik.Context, ev: mftik.FillEvent) -> None:
        if ev.td.api_id != self._api_id or ev.fill.uid != self._uid:
            return
        _log(
            {
                "hook": "OnFill",
                "uid": ev.fill.uid,
                "fill_id": ev.fill.fill_id,
                "cloid": ev.fill.client_order_id,
                "qty": ev.fill.qty,
                "price": ev.fill.price,
            }
        )

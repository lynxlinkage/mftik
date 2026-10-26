"""Watch one instrument's trades and book. This strategy does not trade.

``InitPayload.params`` is the strategy's own JSON. The platform does not
read it. The ticker is a universal ticker (``Venue_Category_Symbol``); this
file does not name a venue or a product.

.. code-block:: json

   {"ticker": "Ex_Spot_ABC", "every_s": 30}

``OnInit`` resolves the ticker. ``on_trade`` and ``on_book`` each write one
JSON line. ``on_book`` reads the local ``BookView`` (empty until ``md.view``
has synced). ``every_s`` is the ``after`` heartbeat, in seconds. Lines go to
stdout, the same way the other examples do.
"""

from __future__ import annotations

import json
import math
import sys
from typing import Any

import mftik
from mftik.strategy import Strategy

__all__ = ["MdWatch"]


class _Params:
    def __init__(self, ticker: str, every_s: float) -> None:
        self.ticker = ticker
        self.every_s = every_s


def _as_float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"md_watch: not a number: {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("md_watch: every_s must be a finite number above 0")
    return number


def _parse(raw: bytes) -> _Params:
    if not raw:
        raise ValueError("md_watch: params are required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"md_watch: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("md_watch: params must be a JSON object")
    ticker = data.get("ticker", "")
    if not isinstance(ticker, str) or not ticker:
        raise ValueError("md_watch: ticker is required")
    if "every_s" not in data:
        raise ValueError("md_watch: every_s is required")
    every_s = _as_float(data["every_s"])
    if every_s <= 0:
        raise ValueError("md_watch: every_s must be a finite number above 0")
    return _Params(ticker, every_s)


def _log(fields: dict[str, Any]) -> None:
    payload = {key: value for key, value in fields.items() if value is not None}
    sys.stdout.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=True) + "\n")
    sys.stdout.flush()


def _top(view: mftik.BookView, bid: bool) -> dict[str, Any]:
    levels = view.bids(1) if bid else view.asks(1)
    if not levels:
        return {}
    tick, qty = levels[0]
    prefix = "bid" if bid else "ask"
    return {prefix: view.price(tick), f"{prefix}_tick": tick, f"{prefix}_qty": qty}


class MdWatch(Strategy):
    """Market-data watcher selected by ``InitPayload.params``."""

    def __init__(self) -> None:
        self._ticker = ""
        self._uid = 0
        self._every = 0.0
        self._trades = 0
        self._books = 0

    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        info = ctx.resolve(params.ticker)
        self._ticker = params.ticker
        self._uid = info.uid
        self._every = params.every_s
        _log({"hook": "OnInit", "ticker": params.ticker, "uid": info.uid})

    def on_start(self, ctx: mftik.Context) -> None:
        ctx.after(self._every, self._on_timer)
        _log({"hook": "OnStart", "uid": self._uid})

    def on_stop(self, _ctx: mftik.Context) -> None:
        _log({"hook": "OnStop", "trades": self._trades, "books": self._books})

    def on_trade(self, _ctx: mftik.Context, ev: mftik.Feed) -> None:
        if ev.md.uid != self._uid:
            return
        trade = ev.data
        self._trades += 1
        _log(
            {
                "hook": "OnTrade",
                "uid": ev.md.uid,
                "seq": ev.md.seq,
                "trade_id": trade.trade_id,
                "price": trade.price,
                "qty": trade.qty,
                "side": int(trade.side),
            }
        )

    def on_book(self, ctx: mftik.Context, ev: mftik.BookUpdate) -> None:
        if ev.md.uid != self._uid:
            return
        self._books += 1
        view = ctx.book(self._uid)
        fields: dict[str, Any] = {"hook": "OnBook", "uid": ev.md.uid, "seq": ev.md.seq}
        if view is None:
            fields["synced"] = False
        else:
            fields["synced"] = True
            fields.update(_top(view, True))
            fields.update(_top(view, False))
        _log(fields)

    def _on_timer(self, ctx: mftik.Context) -> None:
        _log({"hook": "OnTimer", "uid": self._uid, "trades": self._trades, "books": self._books})
        ctx.after(self._every, self._on_timer)

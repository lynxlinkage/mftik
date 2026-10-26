"""Keep a fill count across incarnations (PROTOCOL §9.1.4).

``InitPayload.params`` selects the instrument, the account, and how often to
save. The count itself is not a param.

.. code-block:: json

   {"ticker": "Ex_Spot_ABC", "api_id": 7, "every_s": 30}

On startup the count is the ``fills`` file in the volume directory, if that
file is there. Otherwise it is the same name under the ``state`` input
(``ctx.input_dir("state")``). If neither has it, the count is 0. Each
``on_fill`` for that account and instrument adds one. The ``after`` timer
and ``OnStop`` both replace the volume file atomically. The file is JSON,
``{"fills": N}``.
"""

from __future__ import annotations

import json
import math
import sys
from typing import Any

import mftik
from mftik.strategy import Strategy

__all__ = ["Stateful"]

_STATE = "fills"
_INPUT = "state"
_I64_MAX = 2**63 - 1


class _Params:
    def __init__(self, ticker: str, api_id: int, every_s: float) -> None:
        self.ticker = ticker
        self.api_id = api_id
        self.every_s = every_s


def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > _I64_MAX:
        raise ValueError("stateful: api_id is not a positive integer")
    return value


def _as_float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("stateful: every_s is not a number")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("stateful: every_s must be a finite number above 0")
    return number


def _parse(raw: bytes) -> _Params:
    if not raw:
        raise ValueError("stateful: params are required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"stateful: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("stateful: params must be a JSON object")
    ticker = data.get("ticker", "")
    if not isinstance(ticker, str) or not ticker:
        raise ValueError("stateful: ticker is required")
    if "api_id" not in data or "every_s" not in data:
        raise ValueError("stateful: api_id and every_s are required")
    return _Params(ticker, _as_int(data["api_id"]), _as_float(data["every_s"]))


def _count(raw: bytes | None) -> int:
    if raw is None:
        return 0
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"stateful: fills file: {exc}") from exc
    if not isinstance(data, dict) or "fills" not in data:
        raise ValueError("stateful: fills file must be a JSON object with fills")
    value = data["fills"]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("stateful: fills must be a non-negative integer")
    return value


def _dump(count: int) -> bytes:
    return json.dumps({"fills": count}, separators=(",", ":")).encode("ascii")


def _log(fields: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(fields, separators=(",", ":"), ensure_ascii=True) + "\n")
    sys.stdout.flush()


class Stateful(Strategy):
    """Fill counter. Volume wins, then the ``state`` input, then zero."""

    def __init__(self) -> None:
        self._ticker = ""
        self._api_id = 0
        self._uid = 0
        self._every = 0.0
        self._fills = 0

    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        info = ctx.resolve(params.ticker)
        self._ticker = params.ticker
        self._api_id = params.api_id
        self._uid = info.uid
        self._every = params.every_s
        raw = mftik.volume.read(ctx.volume_dir(), ctx.input_dir(_INPUT), _STATE)
        self._fills = _count(raw)
        _log({"hook": "OnInit", "ticker": params.ticker, "uid": info.uid, "fills": self._fills})

    def on_start(self, ctx: mftik.Context) -> None:
        ctx.after(self._every, self._on_timer)
        _log({"hook": "OnStart", "fills": self._fills})

    def on_fill(self, _ctx: mftik.Context, ev: mftik.FillEvent) -> None:
        if ev.td.api_id != self._api_id or ev.fill.uid != self._uid:
            return
        self._fills += 1
        _log({"hook": "OnFill", "fills": self._fills, "fill_id": ev.fill.fill_id})

    def on_stop(self, ctx: mftik.Context) -> None:
        self._save(ctx)
        _log({"hook": "OnStop", "fills": self._fills})

    def _on_timer(self, ctx: mftik.Context) -> None:
        self._save(ctx)
        _log({"hook": "OnTimer", "fills": self._fills})
        ctx.after(self._every, self._on_timer)

    def _save(self, ctx: mftik.Context) -> None:
        mftik.volume.write(ctx.volume_dir(), _STATE, _dump(self._fills))

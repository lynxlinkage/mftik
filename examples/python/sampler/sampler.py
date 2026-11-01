"""Stamp each hook with ``ctx.now`` and a draw from ``ctx.rand``.

``InitPayload.params`` is the strategy's own JSON. The platform does not
read it. This file does not name a venue or a product.

.. code-block:: json

   {"every_s": 30}

``OnInit`` keeps the session's ``random.Random``. Each hook logs ``now``
twice (the same start time) and one ``getrandbits(64)`` draw. ``every_s``
is the ``after`` heartbeat, in seconds. Lines go to stderr through
``ctx.log``.
"""

from __future__ import annotations

import json
import math
import random
from typing import Any

import mftik
from mftik.strategy import Strategy

__all__ = ["Sampler"]


class _Params:
    def __init__(self, every_s: float) -> None:
        self.every_s = every_s


def _as_float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"sampler: not a number: {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("sampler: every_s must be a finite number above 0")
    return number


def _parse(raw: bytes) -> _Params:
    if not raw:
        raise ValueError("sampler: params are required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"sampler: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("sampler: params must be a JSON object")
    if "every_s" not in data:
        raise ValueError("sampler: every_s is required")
    every_s = _as_float(data["every_s"])
    if every_s <= 0:
        raise ValueError("sampler: every_s must be a finite number above 0")
    return _Params(every_s)


def _log(ctx: mftik.Context, fields: dict[str, Any]) -> None:
    payload = {key: value for key, value in fields.items() if value is not None}
    hook = payload.get("hook", "")
    ctx.log.info(hook if isinstance(hook, str) else "", **payload)


class Sampler(Strategy):
    """Uses ``ctx.now`` and ``ctx.rand`` on every hook."""

    def __init__(self) -> None:
        self._every = 0.0
        self._rng: random.Random | None = None

    def on_init(self, ctx: mftik.Context) -> None:
        params = _parse(ctx.params())
        self._every = params.every_s
        self._rng = ctx.rand()
        self._stamp(ctx, "OnInit")

    def on_start(self, ctx: mftik.Context) -> None:
        ctx.after(self._every, self._on_timer)
        self._stamp(ctx, "OnStart")

    def on_stop(self, ctx: mftik.Context) -> None:
        self._stamp(ctx, "OnStop")

    def _on_timer(self, ctx: mftik.Context) -> None:
        self._stamp(ctx, "OnTimer")
        ctx.after(self._every, self._on_timer)

    def _stamp(self, ctx: mftik.Context, hook: str) -> None:
        rng = self._rng
        if rng is None:
            raise RuntimeError("sampler: rand is missing")
        now = ctx.now()
        _log(
            ctx,
            {
                "hook": hook,
                "now": now,
                "again": ctx.now(),
                "same": ctx.rand() is rng,
                "draw": rng.getrandbits(64),
            },
        )

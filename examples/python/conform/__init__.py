"""The Python standard conformance strategy (PROTOCOL §9.8, E08-D035, E05-01).

The same behavior as ``examples/conform`` and mftiko
``examples/strategies/conform``, started with
``python -m mftik.run conform:Conform``. ``InitPayload.params`` selects it.
Buffer limits, the prehook, the recon watermark, and ``deadline_ms`` aborts
belong to the SDK; this process only sleeps, submits, and reads its context.
For the checks that must see events only after the subscriptions are up,
``on_start`` calls ``resolve``, which the harness holds, and ``priority`` asks
for a recon first. The SDK prints ``gate`` once the subscriptions are up.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

import mftik
from mftik.common.v1 import OrderType, Side, TimeInForce
from mftik.strategy import Strategy
from mftik.topic import TRADE

# The fixture instrument (PROTOCOL §9.8). It is not a params field. Resolving
# it lets the harness hold OnStart.
_CONFORM_TICKER = "Binance_Spot_BTCUSDT"

# Scenarios whose events must wait in the SDK's buffers until OnStart returns.
# The harness holds resolve for these, and only these.
_HOLD = frozenset({"priority", "prehook", "ring", "overflow", "ctx"})

_U32_MAX = 2**32 - 1
_U64_MAX = 2**64 - 1
_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1

__all__ = ["Conform"]


class _Params:
    """``InitPayload.params``. Missing fields are zero; unknown fields are ignored."""

    def __init__(self) -> None:
        self.hook_sleep_ms = 0
        self.order_count = 0
        self.init_sleep_ms = 0
        self.start_sleep_ms = 0
        self.on_stop_sleep_ms = 0
        self.self_exit = False
        self.probe_reads = False
        self.scenario = ""
        # Read by the harness, not the strategy (PROTOCOL §9.8). Parsed so a
        # wrong type fails OnInit the same way serde_json does.
        self.until_seq = 0
        self.api_id = 0
        self.algo_id = 0
        self.uid = 0


def _as_int(value: object, low: int, high: int) -> int:
    """A JSON integer in ``[low, high]``. Bools and fractional numbers are rejected."""
    if isinstance(value, bool) or not isinstance(value, int) or value < low or value > high:
        raise ValueError(f"invalid integer: {value!r}")
    return value


def _parse_params(raw: bytes) -> _Params:
    """Decode params the way Go ``json.Unmarshal`` and the Rust strategy do."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(str(exc)) from exc
    if not isinstance(data, dict):
        raise ValueError("params must be a JSON object")
    params = _Params()
    integers = {
        "hook_sleep_ms": (0, _U64_MAX),
        "order_count": (0, _U32_MAX),
        "init_sleep_ms": (0, _U64_MAX),
        "start_sleep_ms": (0, _U64_MAX),
        "on_stop_sleep_ms": (0, _U64_MAX),
        "until_seq": (0, _U64_MAX),
        "api_id": (_I64_MIN, _I64_MAX),
        "algo_id": (0, _U32_MAX),
        "uid": (0, _U32_MAX),
    }
    for name, (low, high) in integers.items():
        if name in data:
            setattr(params, name, _as_int(data[name], low, high))
    if "self_exit" in data:
        if not isinstance(data["self_exit"], bool):
            raise ValueError("invalid boolean: self_exit")
        params.self_exit = data["self_exit"]
    if "probe_reads" in data:
        if not isinstance(data["probe_reads"], bool):
            raise ValueError("invalid boolean: probe_reads")
        params.probe_reads = data["probe_reads"]
    if "scenario" in data:
        scenario = data["scenario"]
        if not isinstance(scenario, str):
            raise ValueError("invalid string: scenario")
        params.scenario = scenario
    return params


def _write(fields: dict[str, Any]) -> None:
    """One stdout observation. Field order matches the Go and Rust strategies.

    Zero numbers and empty strings are left out. A bool that was set is
    written, ``false`` included.
    """
    payload = json.dumps(fields, separators=(",", ":"), ensure_ascii=True)
    sys.stdout.write(payload + "\n")
    sys.stdout.flush()


def _obs(
    hook: str | None = None,
    *,
    ctx: bool | None = None,
    seq: int = 0,
    incarnation: int = 0,
    view: bool | None = None,
    fetch: bool | None = None,
) -> None:
    fields: dict[str, Any] = {}
    if hook:
        fields["hook"] = hook
    if ctx is not None:
        fields["ctx"] = ctx
    if seq:
        fields["seq"] = seq
    if incarnation:
        fields["incarnation"] = incarnation
    if view is not None:
        fields["view"] = view
    if fetch is not None:
        fields["fetch"] = fetch
    _write(fields)


def _sleep_ms(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000)


def _view(_ctx: object, _page: object, _err: object) -> None:
    _obs(view=True)


def _fetch(_ctx: object, _page: object, _err: object) -> None:
    _obs(fetch=True)


class Conform(Strategy):
    """Session strategy selected by ``InitPayload.params``."""

    def __init__(self) -> None:
        self._p = _Params()

    def on_init(self, ctx: mftik.Context) -> None:
        raw = ctx.params()
        if raw:
            self._p = _parse_params(raw)
        _sleep_ms(self._p.init_sleep_ms)
        _obs("OnInit")

    def on_start(self, ctx: mftik.Context) -> None:
        beat = self._p.scenario == "beat"
        if beat:
            _obs("OnStart")
            _sleep_ms(self._p.hook_sleep_ms)
        else:
            _sleep_ms(self._p.start_sleep_ms)
        if self._p.scenario == "priority":
            ctx.request_recon(self._p.api_id)
        if self._p.scenario in _HOLD:
            ctx.resolve(_CONFORM_TICKER)
        for _ in range(self._p.order_count):
            ctx.submit(
                mftik.Order(
                    api_id=self._p.api_id,
                    algo_id=self._p.algo_id,
                    uid=self._p.uid,
                    side=Side.SIDE_BUY,
                    order_type=OrderType.ORDER_TYPE_LIMIT,
                    tif=TimeInForce.TIME_IN_FORCE_GTC,
                    qty=1.0,
                    price=1.0,
                )
            )
        if self._p.probe_reads:
            # The Rust strategy ignores a read that cannot be sent.
            try:
                ctx.tape(self._p.uid, TRADE, mftik.TapeQuery(limit=1), _view)
            except mftik.Error:
                pass
            try:
                ctx.fetch_klines(self._p.uid, "1m", 1, _fetch)
            except mftik.Error:
                pass
        if not beat:
            _obs("OnStart")
            _sleep_ms(self._p.hook_sleep_ms)
        if self._p.self_exit:
            ctx.exit()

    def on_stop(self, _ctx: mftik.Context) -> None:
        _sleep_ms(self._p.on_stop_sleep_ms)
        _obs("OnStop")

    def on_trade(self, _ctx: mftik.Context, ev: mftik.Feed) -> None:
        _obs("OnTrade", seq=ev.md.seq)

    def on_order(self, ctx: mftik.Context, ev: mftik.OrderEvent) -> None:
        ok = ctx.order(ev.td.api_id, ev.order.client_order_id) is not None
        _obs("OnOrder", ctx=ok, seq=ev.td.seq, incarnation=ev.td.incarnation)
        if self._p.scenario == "ctx" and not ok:
            ctx.fail("ctx")

    def on_position(self, ctx: mftik.Context, ev: mftik.PositionEvent) -> None:
        uid = ev.positions[0].uid if ev.positions else 0
        ok = ctx.position(ev.td.api_id, uid) is not None
        _obs("OnPosition", ctx=ok, seq=ev.td.seq, incarnation=ev.td.incarnation)
        if self._p.scenario == "ctx" and not ok:
            ctx.fail("ctx")

    def on_recon(self, _ctx: mftik.Context, _ev: mftik.ReconEvent) -> None:
        _obs("OnRecon")

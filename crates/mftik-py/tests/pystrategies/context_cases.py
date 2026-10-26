"""Context SYM, orders, local state. Not a port of the Go tests."""

import json
import sys
import threading

from mftik import NotInInit, Order, Strategy, UnknownUid
from mftik.common.v1 import OrderType, Side, TimeInForce

TICKER = "Bybit_Spot_BTCUSDT"
MISSING = "Bybit_Spot_NOPE"
API = 7
ALGO = 1
UID = 1001
NOT_IN_INIT = "sdk: only OnInit and OnStart may call this"


def line(hook, **fields):
    payload = {"hook": hook, **fields}
    sys.stdout.write(json.dumps(payload) + "\n")


def limit(uid):
    return Order(
        api_id=API,
        algo_id=ALGO,
        uid=uid,
        side=Side.SIDE_BUY,
        order_type=OrderType.ORDER_TYPE_LIMIT,
        tif=TimeInForce.TIME_IN_FORCE_GTC,
        qty=1.0,
        price=2.0,
    )


class Trade(Strategy):
    def __init__(self):
        self.first = ""

    def on_init(self, ctx):
        try:
            ctx.submit(1)
        except TypeError as exc:
            line("submit-type", name=type(exc).__name__)
        else:
            raise AssertionError("submit accepted a non-order")

        try:
            ctx.submit(api_id=API)
        except TypeError:
            line("submit-kwargs")
        else:
            raise AssertionError("submit accepted kwargs")

        try:
            ctx.submit(Order(side=99))
        except ValueError as exc:
            line("submit-enum", text=str(exc))
        else:
            raise AssertionError("submit accepted an unknown side")

        info = ctx.resolve(TICKER)
        again = ctx.info(info.uid)
        line(
            "resolved",
            uid=info.uid,
            tick=info.price_tick(),
            again=again.uid,
            tradable=info.is_tradable(),
        )
        try:
            ctx.info(9)
        except UnknownUid as exc:
            line("unknown-uid", text=str(exc))
        else:
            raise AssertionError("unknown uid")

        opens = ctx.open_orders(API, 0)
        if not isinstance(opens, tuple):
            raise AssertionError(type(opens).__name__)
        line(
            "init-state",
            orders=len(opens),
            order=ctx.order(API, "absent") is None,
            pos=ctx.position(API, UID) is None,
            bal=ctx.balance(API, "USDT") is None,
        )

    def on_start(self, ctx):
        self.first = ctx.submit(limit(UID))
        second = ctx.submit(limit(UID + 1))
        opens = ctx.open_orders(API, ALGO)
        line(
            "submitted",
            n=len(opens),
            ids=[item.client_order_id for item in opens],
            status=[int(item.status) for item in opens],
            first=self.first,
            second=second,
            other=len(ctx.open_orders(8, 0)),
        )
        ctx.cancel(API, self.first)
        ctx.after(0.5, self.later)

    def later(self, ctx):
        order = ctx.order(API, self.first)
        pos = ctx.position(API, UID)
        bal = ctx.balance(API, "USDT")
        ctx.batch_cancel(API, ALGO, UID, 9)
        ctx.request_recon(API)
        line(
            "later",
            oid=None if order is None else order.order_id,
            status=None if order is None else int(order.status),
            size=None if pos is None else pos.size,
            free=None if bal is None else bal.free,
            missing_order=ctx.order(API, "absent") is None,
            missing_pos=ctx.position(API, 9) is None,
            missing_bal=ctx.balance(API, "BTC") is None,
            other_algo=len(ctx.open_orders(API, 2)),
        )
        ctx.exit()

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-trade")


class ResolveLate(Strategy):
    def on_init(self, ctx):
        info = ctx.resolve(TICKER)
        line("init-resolve", uid=info.uid)

    def on_start(self, ctx):
        ctx.after(0.15, self.late)

    def late(self, ctx):
        try:
            ctx.resolve(TICKER)
        except NotInInit as exc:
            line("late", name=type(exc).__name__, text=str(exc))
        else:
            raise AssertionError("resolve ran outside init")
        ctx.exit()

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-late")


class ResolveMissing(Strategy):
    def on_init(self, ctx):
        ctx.resolve(MISSING)

    def on_start(self, ctx):
        raise AssertionError("start")

    def on_stop(self, ctx):
        raise AssertionError("stop")


class ResolveGil(Strategy):
    def on_init(self, ctx):
        state = {"n": 0, "stop": False}

        def loop():
            while not state["stop"]:
                state["n"] += 1

        threading.Thread(target=loop, daemon=True).start()
        before = state["n"]
        info = ctx.resolve(TICKER)
        state["stop"] = True
        if state["n"] <= before:
            raise AssertionError(f"gil held during resolve: {state['n']}")
        line("gil", uid=info.uid, n=state["n"])

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-gil")

"""Optional hook delivery through a session (E04-03). Not a port of the Go tests."""

import json

from mftik.strategy import Strategy

_NEED = {
    "agg_trade",
    "balance",
    "fill",
    "funding_rate",
    "kline",
    "liquidation",
    "mark_price",
    "open_interest",
    "position",
    "recon",
    "ticker",
}


def emit(hook, **fields):
    print(json.dumps({"hook": hook, **fields}, sort_keys=True), flush=True)


def frozen(obj, name):
    try:
        setattr(obj, name, 0)
    except Exception:
        return "frozen"
    return "mutable"


class Deliver(Strategy):
    """Records every wired hook. ``on_greeks`` is left to the base."""

    def __init__(self):
        self.held = []
        self.quotes = 0
        self.trades = 0
        self.orders = 0
        self.got = set()

    def on_init(self, ctx):
        emit("init")
        return 1

    def on_start(self, ctx):
        emit("start")
        return 1

    def on_stop(self, ctx):
        emit("stop")
        return 1

    def _done(self, ctx):
        if self.quotes >= 2 and self.trades >= 2 and self.orders >= 2 and _NEED <= self.got:
            ctx.exit()

    def _hold(self, ev):
        self.held.append(ev)
        data = getattr(ev, "data", None)
        if data is not None:
            self.held.append(data)

    def on_best_quote(self, ctx, ev):
        self._hold(ev)
        self.quotes += 1
        emit(
            "best_quote",
            bid=ev.data.bid,
            data=id(ev.data),
            obj=id(ev),
            seq=ev.md.seq,
            uid=ev.md.uid,
        )
        self._done(ctx)
        return 1

    def on_trade(self, ctx, ev):
        self._hold(ev)
        self.trades += 1
        emit(
            "trade",
            data=id(ev.data),
            frozen=frozen(ev.data, "trade_id"),
            obj=id(ev),
            price=ev.data.price,
            seq=ev.md.seq,
            trade_id=ev.data.trade_id,
            uid=ev.md.uid,
        )
        self._done(ctx)
        return 1

    def on_agg_trade(self, ctx, ev):
        emit(
            "agg_trade",
            count=ev.data.match_count,
            price=ev.data.price,
            trade_id=ev.data.trade_id,
        )
        self.got.add("agg_trade")
        self._done(ctx)
        return 1

    def on_kline(self, ctx, ev):
        emit(
            "kline",
            close=ev.data.close,
            closed=ev.data.closed,
            interval=ev.data.interval,
            quote=ev.data.quote_volume,
        )
        self.got.add("kline")
        self._done(ctx)
        return 1

    def on_ticker(self, ctx, ev):
        emit("ticker", last=ev.data.last)
        self.got.add("ticker")
        self._done(ctx)
        return 1

    def on_liquidation(self, ctx, ev):
        emit(
            "liquidation",
            price=ev.data.price,
            qty=ev.data.qty,
            side=int(ev.data.side),
        )
        self.got.add("liquidation")
        self._done(ctx)
        return 1

    def on_funding_rate(self, ctx, ev):
        emit("funding_rate", rate=ev.data.rate)
        self.got.add("funding_rate")
        self._done(ctx)
        return 1

    def on_open_interest(self, ctx, ev):
        emit("open_interest", oi=ev.data.open_interest)
        self.got.add("open_interest")
        self._done(ctx)
        return 1

    def on_mark_price(self, ctx, ev):
        emit("mark_price", mark=ev.data.mark_price)
        self.got.add("mark_price")
        self._done(ctx)
        return 1

    def on_book(self, ctx, ev):
        emit("book", exch=ev.exch_ts_ns, uid=ev.md.uid)
        return 1

    def on_order(self, ctx, ev):
        self._hold(ev)
        self.orders += 1
        emit(
            "order",
            cause=int(ev.cause),
            cloid=ev.order.client_order_id,
            obj=id(ev),
            origin=int(ev.origin),
            seq=ev.td.seq,
            status=int(ev.order.status),
            uid=ev.order.uid,
        )
        self._done(ctx)
        return 1

    def on_fill(self, ctx, ev):
        emit("fill", fill_id=ev.fill.fill_id, qty=ev.fill.qty, seq=ev.td.seq)
        self.got.add("fill")
        self._done(ctx)
        return 1

    def on_position(self, ctx, ev):
        emit("position", seq=ev.td.seq, size=ev.positions[0].size, uid=ev.positions[0].uid)
        self.got.add("position")
        self._done(ctx)
        return 1

    def on_balance(self, ctx, ev):
        emit(
            "balance",
            asset=ev.balances[0].asset,
            free=ev.balances[0].free,
            seq=ev.td.seq,
        )
        self.got.add("balance")
        self._done(ctx)
        return 1

    def on_recon(self, ctx, ev):
        emit(
            "recon",
            asset=ev.recon.balances[0].asset,
            free=ev.recon.balances[0].free,
            inc=ev.td.incarnation,
            reason=int(ev.recon.reason),
            seq=ev.td.seq,
            size=ev.recon.positions[0].size,
        )
        self.got.add("recon")
        self._done(ctx)
        return 1

    def on_reject(self, ctx, ev):
        emit("reject", reason=ev.reason)
        return 1


class AsyncTrade(Strategy):
    def on_init(self, ctx):
        print("TAIL-init", flush=True)

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        print("TAIL-stop", flush=True)

    async def on_trade(self, ctx, ev):
        print("TAIL-trade", flush=True)


class AsyncInit(Strategy):
    async def on_init(self, ctx):
        print("TAIL-init", flush=True)

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        print("TAIL-stop", flush=True)


class AsyncStop(Strategy):
    def on_init(self, ctx):
        print("TAIL-init", flush=True)

    def on_start(self, ctx):
        return None

    async def on_stop(self, ctx):
        print("TAIL-stop", flush=True)

    def on_trade(self, ctx, ev):
        print("TAIL-trade", flush=True)


class TradeBoom(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        print("TAIL-stop", flush=True)

    def on_trade(self, ctx, ev):
        raise RuntimeError("trade broke")


class TradeExit(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        print("TAIL-stop", flush=True)

    def on_trade(self, ctx, ev):
        raise SystemExit("stop-via-sys")

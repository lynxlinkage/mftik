"""MD reads, callbacks, and BookView. Driven by the reads harness."""

import sys
import threading

from mftik import (
    ContextError,
    NoFeed,
    ReadError,
    Strategy,
    TapeQuery,
    UnknownUid,
    topic,
)

TICKER = "Bybit_Spot_BTCUSDT"
UNKNOWN = "sdk: the instrument is not known to this session"
NO_FEED = "sdk: the session has no such feed"
NO_RESPONDER = "mdpath: md.view: nats: no responders available for request"
TIMEOUT = "mdpath: md.view: context deadline exceeded"
FETCH_ERR = "mdpath: md.fetch: invalid: limit"
BOOK_ONLY = "mftik.BookView is only valid inside the hook that received it"
CALLABLE = "expected a callable"


def _raises(call, exc_type, text):
    try:
        call()
    except exc_type as exc:
        if str(exc) != text:
            raise AssertionError(str(exc)) from exc
    else:
        raise AssertionError(exc_type.__name__)


class Reads(Strategy):
    def __init__(self):
        self.uid = 0
        self.ident = 0
        self.got = {}

    def on_init(self, ctx):
        info = ctx.resolve(TICKER)
        self.uid = info.uid
        if info.uid != 7 or info.price_tick() != 0.5:
            raise AssertionError((info.uid, info.price_tick()))
        query = TapeQuery()
        _raises(
            lambda: ctx.tape(99, topic.TRADE, query, self._never),
            UnknownUid,
            UNKNOWN,
        )
        _raises(
            lambda: ctx.tape(info.uid, topic.TRADE, query, self._never),
            NoFeed,
            NO_FEED,
        )

    def on_start(self, ctx):
        if threading.get_ident() != threading.main_thread().ident:
            raise AssertionError("dispatcher")
        self.ident = threading.get_ident()
        if ctx.book(self.uid) is not None or ctx.book(99) is not None:
            raise AssertionError("book")
        query = TapeQuery()
        _raises(lambda: ctx.tape(self.uid, "nope", query, self._never), NoFeed, NO_FEED)

        def take(kind):
            def callback(hook, value, err):
                self._got(hook, kind, value, err)

            return callback

        ctx.tape(self.uid, topic.TRADE, query, take("tape"))
        ctx.book_snapshot(self.uid, take("book"))
        ctx.fetch_klines(self.uid, "1m", 2, take("klines"))
        ctx.fetch_open_interest(self.uid, take("oi"))
        ctx.tape(self.uid, topic.LIQUIDATION, query, take("liq"))
        ctx.tape(self.uid, topic.TICKER, query, take("ticker"))
        ctx.fetch_klines(self.uid, "err", 2, take("bad"))

    def _got(self, ctx, kind, value, err):
        if threading.get_ident() != self.ident:
            raise AssertionError(kind)
        if (value is None) == (err is None):
            raise AssertionError(kind)
        self.got[kind] = (value, err)
        if len(self.got) == 7:
            self._finish(ctx)

    def _finish(self, ctx):
        page, err = self.got["tape"]
        if err is not None or page.head is None or page.head.seq != 9 or page.more:
            raise AssertionError("tape")
        book, err = self.got["book"]
        if err is not None or book.uid != 7 or book.seq != 1:
            raise AssertionError("snapshot")
        if book.bids[0].price != 100.0 or book.bids[0].qty != 2.0:
            raise AssertionError("bid")
        if book.asks[0].price != 101.0 or book.asks[0].qty != 3.0:
            raise AssertionError("ask")
        klines, err = self.got["klines"]
        bar = klines.klines[0]
        if err is not None or bar.interval != "1m" or bar.close != 5.0 or not bar.closed:
            raise AssertionError("klines")
        oi, err = self.got["oi"]
        if err is not None or oi.open_interest != 4.0:
            raise AssertionError("oi")
        self._err("liq", NO_RESPONDER)
        self._err("ticker", TIMEOUT)
        self._err("bad", FETCH_ERR)
        sys.stdout.write("TAIL-reads\n")
        ctx.exit()

    def _err(self, kind, text):
        value, err = self.got[kind]
        if value is not None or not isinstance(err, ReadError) or str(err) != text:
            raise AssertionError((kind, value, type(err).__name__, str(err)))

    def _never(self, ctx, value, err):
        raise AssertionError((value, err))

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-stop\n")


class Book(Strategy):
    def __init__(self):
        self.uid = 0
        self.tries = 0

    def on_init(self, ctx):
        info = ctx.resolve(TICKER)
        self.uid = info.uid
        if info.uid != 7 or info.price_tick() != 0.5:
            raise AssertionError((info.uid, info.price_tick()))

    def on_start(self, ctx):
        if ctx.book(self.uid) is not None:
            raise AssertionError("early")
        ctx.after(0.05, self._look)

    def _look(self, ctx):
        view = ctx.book(self.uid)
        if view is None:
            self.tries += 1
            if self.tries > 40:
                ctx.fail("book never synced")
                return
            ctx.after(0.05, self._look)
            return
        if view.tick_size() != 0.5 or view.window() != 2048:
            raise AssertionError((view.tick_size(), view.window()))
        if view.price(200) != 100.0:
            raise AssertionError(view.price(200))
        if view.best_bid_tick() != 200 or view.best_ask_tick() != 202:
            raise AssertionError((view.best_bid_tick(), view.best_ask_tick()))
        if view.qty(True, 100.0) != 2.0 or view.qty(False, 101.0) != 3.0:
            raise AssertionError("qty")
        if view.qty(True, 50.0) is not None or view.qty(False, 50.0) is not None:
            raise AssertionError("missing")
        if view.bids(0) != [] or view.bids(-1) != []:
            raise AssertionError("empty")
        levels = view.bids(4)
        asks = view.asks(1)
        if not isinstance(levels, list) or levels != [(200, 2.0)]:
            raise AssertionError(levels)
        if not isinstance(asks, list) or not isinstance(asks[0], tuple) or asks != [(202, 3.0)]:
            raise AssertionError(asks)
        caught = []

        def other():
            try:
                view.tick_size()
            except ContextError as exc:
                caught.append(str(exc))

        worker = threading.Thread(target=other)
        worker.start()
        worker.join()
        if caught != [BOOK_ONLY]:
            raise AssertionError(caught)
        if view.best_bid_tick() != 200:
            raise AssertionError("inside")
        copied = list(levels)

        def later(ctx2):
            _raises(view.best_bid_tick, ContextError, BOOK_ONLY)
            if copied != [(200, 2.0)]:
                raise AssertionError(copied)
            sys.stdout.write("TAIL-book\n")
            ctx2.exit()

        ctx.after(0, later)

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-stop\n")


class NotCallable(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        query = TapeQuery()
        calls = (
            lambda: ctx.tape(7, topic.TRADE, query, None),
            lambda: ctx.book_snapshot(7, None),
            lambda: ctx.fetch_klines(7, "1m", 1, None),
            lambda: ctx.fetch_open_interest(7, None),
        )
        for call in calls:
            _raises(call, TypeError, CALLABLE)

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-callable\n")


class Arity(Strategy):
    def on_init(self, ctx):
        info = ctx.resolve(TICKER)
        if info.uid != 7:
            raise AssertionError(info.uid)

    def on_start(self, ctx):
        def bad(hook):
            return hook

        ctx.tape(7, topic.TRADE, TapeQuery(), bad)

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-arity\n")


class Boom(Strategy):
    def on_init(self, ctx):
        info = ctx.resolve(TICKER)
        if info.uid != 7:
            raise AssertionError(info.uid)

    def on_start(self, ctx):
        def bad(hook, value, err):
            raise RuntimeError("read broke")

        ctx.tape(7, topic.TRADE, TapeQuery(), bad)

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-boom\n")

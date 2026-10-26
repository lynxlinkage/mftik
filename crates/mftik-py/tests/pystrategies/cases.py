"""Strategies the session tests drive through ``python -m mftik.run``."""

import sys
import threading

from mftik import ContextError, Strategy


class Ok(Strategy):
    def on_init(self, ctx):
        if ctx.session_id() != "c0ffee":
            raise AssertionError(ctx.session_id())
        if not ctx.volume_dir():
            raise AssertionError("volume")
        if ctx.input_dir("model") != "" or ctx.input_dir("..") != "":
            raise AssertionError("input")

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-stop")


class InitBoom(Strategy):
    def on_init(self, ctx):
        sys.stdout.write("TAIL-init")
        raise RuntimeError("init broke")

    def on_start(self, ctx):
        raise AssertionError("start")

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-stop")


class StartBoom(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        raise ValueError("start broke")

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-start")


class AfterBoom(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        def boom(_ctx):
            raise RuntimeError("after broke")

        ctx.after(0.05, boom)

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-after")


class Hung(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        def stuck(_ctx):
            sys.stdout.write("TAIL-hung")
            while True:
                pass

        ctx.post(stuck)

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-stop")


class SysExitInit(Strategy):
    def on_init(self, ctx):
        sys.stdout.write("TAIL-sysexit")
        sys.exit("stop-via-sys")

    def on_start(self, ctx):
        raise AssertionError("start")

    def on_stop(self, ctx):
        raise AssertionError("stop")


class KeyInt(Strategy):
    def on_init(self, ctx):
        sys.stdout.write("TAIL-keyint")
        raise KeyboardInterrupt("tick")

    def on_start(self, ctx):
        raise AssertionError("start")

    def on_stop(self, ctx):
        raise AssertionError("stop")


class StopBoom(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        sys.stdout.write("TAIL-onstop")
        raise RuntimeError("stop broke")


class NanDelay(Strategy):
    def on_init(self, ctx):
        sys.stdout.write("TAIL-nan")
        ctx.after(float("nan"), lambda _ctx: None)

    def on_start(self, ctx):
        raise AssertionError("start")

    def on_stop(self, ctx):
        raise AssertionError("stop")


class CtxLife(Strategy):
    def __init__(self):
        self.held = None
        self.poster = None

    def on_init(self, ctx):
        if ctx.params() != b"ctx":
            raise AssertionError(repr(ctx.params()))
        self.held = ctx
        self.poster = ctx.poster()

    def on_start(self, ctx):
        self._dead(self.held)
        seen = {}

        def via_post(live):
            seen["id"] = live.session_id()

        ctx.post(via_post)

        def worker():
            try:
                ctx.session_id()
            except ContextError:
                seen["err"] = "ContextError"
            else:
                seen["err"] = "none"

            def later(live):
                try:
                    ctx.fail("stale")
                except ContextError:
                    pass
                else:
                    raise AssertionError("ctx still valid")
                if seen.get("id") != "c0ffee":
                    raise AssertionError(repr(seen.get("id")))
                if live.session_id() != "c0ffee":
                    raise AssertionError(live.session_id())
                sys.stdout.write("TAIL-ctx")
                live.exit()

            self.poster.post(later)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        if seen.get("err") != "ContextError":
            raise AssertionError(seen.get("err"))

    def on_stop(self, ctx):
        self._dead(self.held)

    def _dead(self, ctx):
        try:
            ctx.volume_dir()
        except ContextError:
            return
        raise AssertionError("ctx still valid")


class GilGap(Strategy):
    def __init__(self):
        self.n = 0
        self.stop = False
        self.at_init = 0

    def on_init(self, ctx):
        def loop():
            while not self.stop:
                self.n += 1

        threading.Thread(target=loop, daemon=True).start()
        self.at_init = self.n
        ctx.after(0.2, self.between)

    def between(self, ctx):
        # The runtime releases the GIL between hooks, so this thread runs
        # while the dispatcher is outside Python.
        if self.n <= self.at_init:
            raise AssertionError(f"no progress between hooks: {self.n}")
        self.stop = True
        sys.stdout.write("TAIL-gil")
        ctx.exit()

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        self.stop = True


class Plain:
    pass

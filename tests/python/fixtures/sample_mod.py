"""A strategy module the loader tests import."""

from mftik import Strategy


class Sample(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        return None

    def on_trade(self, ctx, ev):
        return None


class Child(Sample):
    def on_kline(self, ctx, ev):
        return None


class Plain:
    def __init__(self):
        return None


class Boom:
    def __init__(self):
        raise RuntimeError("construct broke")

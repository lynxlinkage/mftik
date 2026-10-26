"""Class-identity registration and async rejection (E04-D031, E04-D032)."""

import io
import unittest
from contextlib import redirect_stderr

from mftik._mftik import _registered_hooks
from mftik.strategy import Strategy


class Bare(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        return None


class Alias(Bare):
    on_trade = Strategy.on_trade


class Both(Bare):
    on_trade = Strategy.on_trade

    def on_kline(self, ctx, ev):
        return None


class AsyncInit(Bare):
    async def on_init(self, ctx):
        return None

    async def on_trade(self, ctx, ev):
        return None


class AsyncStop(Bare):
    async def on_stop(self, ctx):
        return None

    def on_trade(self, ctx, ev):
        return None


class AsyncTrade(Bare):
    async def on_trade(self, ctx, ev):
        return None


class Parent(Bare):
    async def on_funding_rate(self, ctx, ev):
        return None


class Child(Parent):
    def on_kline(self, ctx, ev):
        return None


class AsyncKline(Bare):
    async def on_kline(self, ctx, ev):
        yield None


class HookRegistrationTests(unittest.TestCase):
    def registered(self, strategy):
        buf = io.StringIO()
        with redirect_stderr(buf):
            hooks = _registered_hooks(strategy)
        return hooks, buf.getvalue()

    def test_assigning_the_base_function_is_not_an_override(self):
        hooks, text = self.registered(Alias())
        self.assertEqual(hooks, [])
        self.assertNotIn("Traceback", text)

        hooks, _text = self.registered(Both())
        self.assertEqual(hooks, ["on_kline"])

    def test_an_instance_attribute_is_not_an_override(self):
        strategy = Bare()

        async def on_trade(self, ctx, ev):
            return None

        strategy.on_trade = on_trade
        hooks, text = self.registered(strategy)
        self.assertEqual(hooks, [])
        self.assertNotIn("is async", text)

    def test_async_hooks_are_rejected_in_name_order(self):
        cases = [
            (AsyncInit(), "TypeError: on_init is async"),
            (AsyncStop(), "TypeError: on_stop is async"),
            (AsyncTrade(), "TypeError: on_trade is async"),
            (Child(), "TypeError: on_funding_rate is async"),
            (AsyncKline(), "TypeError: on_kline is async"),
        ]
        for strategy, message in cases:
            with self.subTest(message=message):
                hooks, text = self.registered(strategy)
                self.assertEqual(hooks, [])
                self.assertIn("Traceback", text)
                self.assertIn(message, text)


if __name__ == "__main__":
    unittest.main()

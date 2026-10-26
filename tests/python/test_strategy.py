"""Which optional hooks a subclass overrides (E04-D003)."""

import sys
import unittest
from pathlib import Path

import mftik
from mftik._mftik import _registered_hooks
from mftik.strategy import Strategy

FIXTURES = Path(__file__).resolve().parent / "fixtures"

OPTIONAL = [
    "on_best_quote",
    "on_trade",
    "on_agg_trade",
    "on_kline",
    "on_ticker",
    "on_liquidation",
    "on_funding_rate",
    "on_open_interest",
    "on_greeks",
    "on_mark_price",
    "on_book",
    "on_order",
    "on_fill",
    "on_position",
    "on_balance",
    "on_recon",
    "on_reject",
]


class Bare(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        return None


class AllHooks(Bare):
    def on_best_quote(self, ctx, ev):
        return None

    def on_trade(self, ctx, ev):
        return None

    def on_agg_trade(self, ctx, ev):
        return None

    def on_kline(self, ctx, ev):
        return None

    def on_ticker(self, ctx, ev):
        return None

    def on_liquidation(self, ctx, ev):
        return None

    def on_funding_rate(self, ctx, ev):
        return None

    def on_open_interest(self, ctx, ev):
        return None

    def on_greeks(self, ctx, ev):
        return None

    def on_mark_price(self, ctx, ev):
        return None

    def on_book(self, ctx, ev):
        return None

    def on_order(self, ctx, ev):
        return None

    def on_fill(self, ctx, ev):
        return None

    def on_position(self, ctx, ev):
        return None

    def on_balance(self, ctx, ev):
        return None

    def on_recon(self, ctx, ev):
        return None

    def on_reject(self, ctx, ev):
        return None


class HookTests(unittest.TestCase):
    def test_required_hooks_raise_until_implemented(self):
        strategy = Strategy()
        with self.assertRaises(NotImplementedError):
            strategy.on_init(None)
        with self.assertRaises(NotImplementedError):
            strategy.on_start(None)
        with self.assertRaises(NotImplementedError):
            strategy.on_stop(None)

    def test_base_hooks_are_not_registered(self):
        self.assertEqual(_registered_hooks(Bare()), [])

    def test_only_overrides_are_registered_including_inherited_ones(self):
        sys.path.insert(0, str(FIXTURES))
        try:
            from sample_mod import Child, Sample

            self.assertEqual(_registered_hooks(Sample()), ["on_trade"])
            self.assertEqual(_registered_hooks(Child()), ["on_trade", "on_kline"])
        finally:
            sys.path.remove(str(FIXTURES))
            sys.modules.pop("sample_mod", None)

    def test_every_optional_hook(self):
        self.assertEqual(_registered_hooks(AllHooks()), OPTIONAL)

    def test_context_error_is_an_exception(self):
        self.assertTrue(issubclass(mftik.ContextError, Exception))


if __name__ == "__main__":
    unittest.main()

"""The strategy base class (E04-D003).

``on_init``, ``on_start``, and ``on_stop`` are required. The other hooks are
optional: the runtime registers a hook only when a subclass defines it, and
it decides that once before ``on_init``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mftik._mftik import Context


class Strategy:
    """Base class of a session strategy. Constructed with no arguments."""

    def on_init(self, ctx: Context) -> None:
        raise NotImplementedError

    def on_start(self, ctx: Context) -> None:
        raise NotImplementedError

    def on_stop(self, ctx: Context) -> None:
        raise NotImplementedError

    def on_best_quote(self, ctx: Context, ev: object) -> None:
        return

    def on_trade(self, ctx: Context, ev: object) -> None:
        return

    def on_agg_trade(self, ctx: Context, ev: object) -> None:
        return

    def on_kline(self, ctx: Context, ev: object) -> None:
        return

    def on_ticker(self, ctx: Context, ev: object) -> None:
        return

    def on_liquidation(self, ctx: Context, ev: object) -> None:
        return

    def on_funding_rate(self, ctx: Context, ev: object) -> None:
        return

    def on_open_interest(self, ctx: Context, ev: object) -> None:
        return

    def on_greeks(self, ctx: Context, ev: object) -> None:
        return

    def on_mark_price(self, ctx: Context, ev: object) -> None:
        return

    def on_book(self, ctx: Context, ev: object) -> None:
        return

    def on_order(self, ctx: Context, ev: object) -> None:
        return

    def on_fill(self, ctx: Context, ev: object) -> None:
        return

    def on_position(self, ctx: Context, ev: object) -> None:
        return

    def on_balance(self, ctx: Context, ev: object) -> None:
        return

    def on_recon(self, ctx: Context, ev: object) -> None:
        return

    def on_reject(self, ctx: Context, ev: object) -> None:
        return

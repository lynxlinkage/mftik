"""The strategy base class (E04-D003, E04-D031, E04-D032).

``on_init``, ``on_start``, and ``on_stop`` are required. An optional hook is
registered only when ``type(strategy).on_x is not Strategy.on_x``. That
decision is made once, before ``on_init``. An attribute set on the instance
does not count. An ``async def`` hook is rejected then with ``TypeError``
and the session ends ``INIT_FAILED``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mftik._mftik import (
        BalanceEvent,
        BookUpdate,
        Context,
        Feed,
        FillEvent,
        OrderEvent,
        PositionEvent,
        ReconEvent,
        RejectEvent,
    )


class Strategy:
    """Base class of a session strategy. Constructed with no arguments."""

    def on_init(self, ctx: Context) -> None:
        raise NotImplementedError

    def on_start(self, ctx: Context) -> None:
        raise NotImplementedError

    def on_stop(self, ctx: Context) -> None:
        raise NotImplementedError

    def on_best_quote(self, ctx: Context, ev: Feed) -> None:
        return

    def on_trade(self, ctx: Context, ev: Feed) -> None:
        return

    def on_agg_trade(self, ctx: Context, ev: Feed) -> None:
        return

    def on_kline(self, ctx: Context, ev: Feed) -> None:
        return

    def on_ticker(self, ctx: Context, ev: Feed) -> None:
        return

    def on_liquidation(self, ctx: Context, ev: Feed) -> None:
        return

    def on_funding_rate(self, ctx: Context, ev: Feed) -> None:
        return

    def on_open_interest(self, ctx: Context, ev: Feed) -> None:
        return

    def on_greeks(self, ctx: Context, ev: Feed) -> None:
        return

    def on_mark_price(self, ctx: Context, ev: Feed) -> None:
        return

    def on_book(self, ctx: Context, ev: BookUpdate) -> None:
        return

    def on_order(self, ctx: Context, ev: OrderEvent) -> None:
        return

    def on_fill(self, ctx: Context, ev: FillEvent) -> None:
        return

    def on_position(self, ctx: Context, ev: PositionEvent) -> None:
        return

    def on_balance(self, ctx: Context, ev: BalanceEvent) -> None:
        return

    def on_recon(self, ctx: Context, ev: ReconEvent) -> None:
        return

    def on_reject(self, ctx: Context, ev: RejectEvent) -> None:
        return

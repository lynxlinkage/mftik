"""Rounding an order to a venue's filters.

Every strategy rounds through :class:`SymbolInfo` before it submits, so what
comes out of here is what reaches the venue. These tests assert on the
**written form**, not on ``Decimal`` equality: ``Decimal("0.00780000") ==
Decimal("0.0078")`` is true, and a test that compares that way cannot see the
difference between a size Binance accepts and one it answers ``-1111`` to.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.protocol.messages import SymbolFilterInfo, SymbolInfo


def _info(**filters: str | None) -> SymbolInfo:
    return SymbolInfo(
        universal_ticker="Binance_Spot_ETHUSDT",
        base="ETH",
        quote="USDT",
        exch_ticker="ETHUSDT",
        filters=[
            SymbolFilterInfo(
                name=name, value=None if raw is None else Decimal(raw)
            )
            for name, raw in filters.items()
        ],
    )


def test_a_rounded_size_is_written_at_the_steps_own_precision() -> None:
    """The live ``-1111``: Binance publishes ETHUSDT's step as ``0.00010000``.

    A ``Decimal`` product inherits the operands' scale, so flooring against
    that step used to yield ``0.00780000`` — eight decimals where the venue
    allows four. Binance checks the written precision, not the value, and
    refuses it.
    """
    info = _info(qty_step="0.00010000")

    rounded = info.round_qty(Decimal("15") / Decimal("1918.67"))

    assert str(rounded) == "0.0078"
    assert rounded == Decimal("0.0078")


def test_the_step_being_written_exactly_gives_the_same_answer() -> None:
    """A clean step and a padded one must not produce different orders.

    Gate's steps arrive exact and Binance's arrive padded; the same size on
    the same instrument should reach either venue written the same way.
    """
    padded = _info(qty_step="0.00010000").round_qty(Decimal("0.0078179"))
    exact = _info(qty_step="0.0001").round_qty(Decimal("0.0078179"))

    assert str(padded) == str(exact) == "0.0078"


@pytest.mark.parametrize(
    ("step", "value", "expected"),
    [
        ("0.0001", "0.0078179", "0.0078"),
        ("0.00010000", "0.0078179", "0.0078"),
        ("0.01", "1918.679", "1918.67"),
        ("0.01000000", "1918.679", "1918.67"),
        # A whole-number step must not come back exponential: ``normalize``
        # alone turns 30 into 3E+1, which no venue parses as a size.
        ("10", "37", "30"),
        ("10.00000000", "37", "30"),
        ("1", "37.9", "37"),
        # Already a multiple, and already exact.
        ("0.5", "2.5", "2.5"),
    ],
)
def test_rounding_never_widens_the_written_precision(
    step: str, value: str, expected: str
) -> None:
    info = _info(qty_step=step)
    assert str(info.round_qty(Decimal(value))) == expected


def test_price_rounding_goes_through_the_same_arithmetic() -> None:
    info = _info(price_tick="0.01000000")
    assert str(info.round_price(Decimal("1918.6794"))) == "1918.67"


def test_qty_for_notional_rounds_down_to_a_writable_size() -> None:
    """The path ``qty_quote`` strategies actually take."""
    info = _info(qty_step="0.00010000")

    qty = info.qty_for_notional(Decimal("15"), Decimal("1918.67"))

    assert str(qty) == "0.0078"
    # Down, never up: rounding a size up can breach a balance.
    assert qty * Decimal("1918.67") <= Decimal("15")


def test_a_venue_that_publishes_no_step_leaves_the_size_alone() -> None:
    info = _info(qty_step=None)
    assert info.round_qty(Decimal("0.0078179")) == Decimal("0.0078179")


def test_a_zero_step_is_not_divided_by() -> None:
    """Binance writes ``0`` for a filter it publishes but does not enforce."""
    info = _info(qty_step="0")
    assert info.round_qty(Decimal("0.0078179")) == Decimal("0.0078179")

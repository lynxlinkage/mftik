"""Rendering amounts for the wire.

Every case here is a value that must survive the trip unchanged. The only
transformation allowed is removing scale padding the column added, so each test
asserts the rendered text parses back to the same number — a rendering that
merely *looks* right is the failure mode this guards, since a shortened amount
reads as a plausible one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft_api.decimals import wire_decimal


@pytest.mark.parametrize(
    ("stored", "shown"),
    [
        # The case that prompted this: a price the venue reported as 63863.6.
        ("63863.600000000000000000", "63863.6"),
        ("0.250000000000000000", "0.25"),
        ("-0.500000000000000000", "-0.5"),
        # Nothing to strip.
        ("0.000000000000000001", "0.000000000000000001"),
        # An integer has no padding, and stripping zeros off one would turn
        # 100 into 1.
        ("100.000000000000000000", "100"),
        ("1000000", "1000000"),
        ("100", "100"),
        # Zero, in the several shapes a NUMERIC column produces.
        ("0", "0"),
        ("0E-18", "0"),
        ("-0E-18", "0"),
    ],
)
def test_padding_is_removed_and_nothing_else(stored: str, shown: str) -> None:
    rendered = wire_decimal(Decimal(stored))
    assert rendered == shown
    assert Decimal(rendered) == Decimal(stored), "the value must not change"


def test_a_full_precision_amount_survives() -> None:
    """The reason this does not use ``Decimal.normalize``.

    ``normalize`` rounds to the active context precision — 28 significant
    digits by default — where these columns hold up to 38. It would return a
    shortened number that still looks like a price, which is exactly the silent
    loss ``NUMERIC(38,18)`` exists to prevent.
    """
    stored = Decimal("123456789012345678.123456789012345678")

    rendered = wire_decimal(stored)

    assert Decimal(rendered) == stored
    assert rendered == "123456789012345678.123456789012345678"


def test_never_scientific_notation() -> None:
    """A price is read by a person; ``1E-18`` is not a price to them."""
    assert wire_decimal(Decimal("1E-18")) == "0.000000000000000001"
    assert wire_decimal(Decimal("1E+2")) == "100"


def test_a_missing_amount_is_not_a_zero() -> None:
    """On an execution the two are different facts."""
    assert wire_decimal(None) is None


def test_a_value_already_a_string_is_handled() -> None:
    """The live path gets these pre-serialized by pydantic, not as Decimals."""
    assert wire_decimal("63863.600000000000000000") == "63863.6"


def test_something_unreadable_is_passed_through_not_swallowed() -> None:
    """If a venue ever sends this, the screen should show the thing to
    complain about rather than a plausible substitute."""
    assert wire_decimal("not-a-number") == "not-a-number"

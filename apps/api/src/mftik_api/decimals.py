"""Rendering stored amounts for the wire, without rounding any of them.

``NUMERIC(38,18)`` pads every value out to its full scale, so a price the venue
reported as ``63863.6`` comes back as ``63863.600000000000000000``. The scale is
an artifact of the column, not of anything a venue said, and it makes a table of
prices unreadable.

Trailing zeros are the only thing removed. That is exact by definition — the
value is unchanged, only its scale — and it is done on the *text* rather than
through :meth:`decimal.Decimal.normalize`, which is the obvious approach and
quietly wrong: ``normalize`` rounds to the active context precision, 28
significant digits by default, where these columns hold up to 38. A 38-digit
amount would come back shortened, which is precisely the silent loss the column
type exists to prevent.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def wire_decimal(value: Any) -> str | None:
    """One stored amount as a string, padding removed and nothing else.

    None passes through: on an execution a missing figure and a zero are
    different facts, and rendering the first as ``"0"`` would invent the
    second. Anything unparseable is passed along as-is rather than swallowed —
    if a venue ever sends something this cannot read, the value on screen
    should be the thing to complain about.
    """
    if value is None:
        return None
    try:
        text = format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError, TypeError):
        return str(value)
    if "." in text:
        # Guarded, because an integer has no padding to strip and ``100``
        # would otherwise become ``1``.
        text = text.rstrip("0").rstrip(".")
    if text in ("", "-", "-0"):
        # ``-0E-18`` is a real thing to read out of a NUMERIC column, and a fee
        # of "-0" reads as an error rather than as nothing.
        return "0"
    return text


__all__ = ["wire_decimal"]

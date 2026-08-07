"""Canonical candle intervals — one spelling above the adapter layer."""

from __future__ import annotations

import pytest
from mft.exchange.intervals import (
    InvalidIntervalError,
    interval_seconds,
    normalize_interval,
)


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("1s", "1s"),
        ("10s", "10s"),
        ("1m", "1m"),
        ("15m", "15m"),
        ("1h", "1h"),
        ("1d", "1d"),
        ("1w", "1w"),
        ("1mo", "1mo"),
        # Whitespace and casing are typos, not ambiguities.
        (" 1H ", "1h"),
        ("4H", "4h"),
        ("1MO", "1mo"),
    ],
)
def test_normalize_accepts_well_formed_intervals(given: str, expected: str) -> None:
    assert normalize_interval(given) == expected


def test_month_must_be_mo_not_capital_m() -> None:
    """``1M`` is rejected rather than aliased — see the module docstring.

    It differs from ``1m`` by case alone while meaning 43200 times more, so
    accepting it would leave one stray ``.lower()`` between a month of candles
    and a minute of them, with a plausible-looking answer either way.
    """
    with pytest.raises(InvalidIntervalError) as excinfo:
        normalize_interval("1M")

    # The error has to name the fix; "invalid interval" alone would send the
    # caller looking at their count, not their unit.
    assert "1mo" in str(excinfo.value)


@pytest.mark.parametrize(
    "given",
    [
        "",
        "m",  # no count
        "0m",  # a zero-length window is not a window
        "1",  # no unit
        "1y",  # unit we do not define
        "99x",
        "1 m",  # inner space, not surrounding
        "-5m",
        "1.5h",
    ],
)
def test_normalize_rejects_malformed_intervals(given: str) -> None:
    with pytest.raises(InvalidIntervalError):
        normalize_interval(given)


def test_normalize_rejects_non_strings() -> None:
    with pytest.raises(InvalidIntervalError):
        normalize_interval(60)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("interval", "seconds"),
    [
        ("1s", 1),
        ("10s", 10),
        ("1m", 60),
        ("5m", 300),
        ("1h", 3600),
        ("4h", 14400),
        ("1d", 86400),
        ("1w", 604800),
        ("1mo", 2592000),
        ("3d", 259200),
    ],
)
def test_interval_seconds(interval: str, seconds: int) -> None:
    assert interval_seconds(interval) == seconds


def test_interval_seconds_normalizes_first() -> None:
    assert interval_seconds(" 1H ") == 3600


def test_intervals_order_by_seconds() -> None:
    """The point of ``interval_seconds``: comparing windows across units."""
    windows = ["1d", "10s", "1h", "1mo", "5m", "1w"]
    assert sorted(windows, key=interval_seconds) == [
        "10s",
        "5m",
        "1h",
        "1d",
        "1w",
        "1mo",
    ]

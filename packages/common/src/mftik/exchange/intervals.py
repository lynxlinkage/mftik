"""Canonical candle intervals, spelled one way above the adapter layer.

Every venue invents its own vocabulary for a candle window. Gate takes ``1w``
and ``7d`` alike but rejects ``1M`` outright; the next venue will accept
exactly one of those and spell the month differently again. A strategy asking
for weekly candles should not have to know which.

The canonical spelling is ``<count><unit>`` with the unit drawn from ``s``,
``m``, ``h``, ``d``, ``w`` or ``mo``, and it is what crosses every boundary
above the adapter: the strategy API, the MD protocol, and the ``interval``
stamped on a returned :class:`~mftik.exchange.models.Kline`. Adapters translate
on the way out, and a venue's own spelling appears nowhere else.

Month is ``mo``, never ``M``. ``1M`` is the more widespread convention, but it
differs from ``1m`` by case alone while meaning something 43200 times larger —
one ``.lower()`` anywhere in the pipeline would quietly turn a month of
candles into a minute of them, and the result is plausible enough to survive
review. So ``1M`` is rejected with a message naming ``1mo`` rather than
accepted as an alias: a loud error beats a believable wrong answer.

Which intervals a venue actually serves is the adapter's business, not this
module's — :func:`normalize` says an interval is well-formed, never that
anyone will answer it.
"""

from __future__ import annotations

import re

from mftik.exchange.errors import ExchangeError

#: Seconds in one unit. ``mo`` is a nominal 30 days: months are not a fixed
#: length, and this value exists to order and compare intervals, not to do
#: calendar arithmetic.
UNIT_SECONDS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "mo": 2592000,
}

_PATTERN = re.compile(r"^([1-9][0-9]*)(s|m|h|d|w|mo)$")


class InvalidIntervalError(ExchangeError):
    """Raised when an interval is not well-formed canonical spelling."""


def _parse(interval: str) -> tuple[int, str]:
    if not isinstance(interval, str):
        raise InvalidIntervalError(
            f"interval must be a string, got {type(interval).__name__}"
        )
    text = interval.strip()
    if text.endswith("M"):
        # Caught before the lowercase below, which would fold it onto minutes.
        raise InvalidIntervalError(
            f"interval {interval!r}: month is spelled 'mo' here, not 'M' "
            f"(did you mean {text[:-1]}mo?)"
        )
    match = _PATTERN.match(text.lower())
    if match is None:
        raise InvalidIntervalError(
            f"interval {interval!r} is not <count><unit> with unit in "
            f"{sorted(UNIT_SECONDS)}"
        )
    count, unit = match.groups()
    return int(count), unit


def normalize_interval(interval: str) -> str:
    """Return ``interval`` in canonical spelling, or raise.

    Surrounding whitespace and casing are forgiven — ``" 1H "`` is ``1h`` —
    because those are typos, not ambiguities. A trailing uppercase ``M`` is
    the one thing that is not forgiven; see the module docstring.
    """
    count, unit = _parse(interval)
    return f"{count}{unit}"


def interval_seconds(interval: str) -> int:
    """Length of one ``interval`` window in seconds (``mo`` is 30 days)."""
    count, unit = _parse(interval)
    return count * UNIT_SECONDS[unit]

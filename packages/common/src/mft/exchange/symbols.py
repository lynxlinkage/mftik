"""Canonical instrument symbols.

The platform speaks one spelling — ``BTCUSDT``, uppercase, no separator — and
each adapter translates to its venue's form at the boundary.

Translation is a **lookup, not a transform**. Deriving a venue ticker from a
canonical symbol needs the base/quote split, which ``BTCUSDT`` does not carry,
and guessing it is not merely imprecise — on real Gate data ``USDTUSD`` splits
to ``(USD, TUSD)`` by longest-suffix matching, which is a different instrument
and would route an order to ``USD_TUSD``. Venues also publish tickers that are
not ``base + separator + quote`` at all.

So the symbol plane (``apps/sym``) owns both directions, and adapters take a
:class:`SymbolResolver` rather than doing string surgery. :func:`canonical` is
kept only for normalizing user input before a lookup.
"""

from __future__ import annotations

import re
from typing import Protocol

_SEPARATORS = re.compile(r"[/\-_\s]")


class SymbolResolver(Protocol):
    """Two-way symbol translation, backed by the symbol plane.

    Adapters depend on this rather than on the plane's transport, so they stay
    usable with a stub and do not drag a broker into the exchange layer.
    """

    async def exch_ticker(
        self, venue: str, symbol: str, *, category: str = "spot"
    ) -> str:
        """Canonical → the venue's spelling."""

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str = "spot"
    ) -> str:
        """The venue's spelling → canonical."""


def canonical(symbol: str) -> str:
    """Normalize a spelling for lookup.

    ``BTC_USDT``, ``BTC-USDT``, ``btc/usdt`` and ``BTCUSDT`` all collapse to
    ``BTCUSDT``. This makes user input uniform; it does not tell you the
    venue's ticker — ask the resolver for that.
    """
    return _SEPARATORS.sub("", (symbol or "").strip()).upper()


def join(base: str, quote: str, separator: str = "") -> str:
    """Render a pair in a venue's spelling from known base/quote."""
    return f"{base.upper()}{separator}{quote.upper()}"


#: Unambiguous alias for re-export at the ``mft.exchange`` level.
canonical_symbol = canonical

__all__ = [
    "SymbolResolver",
    "canonical",
    "canonical_symbol",
    "join",
]

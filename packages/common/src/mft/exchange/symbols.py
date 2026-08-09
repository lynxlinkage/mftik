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
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:  # Only for the annotation; tickers is built on this module.
    from mft.exchange.tickers import UniversalTicker

_SEPARATORS = re.compile(r"[/\-_\s]")


class SymbolResolver(Protocol):
    """Two-way symbol translation, backed by the symbol plane.

    Adapters depend on this rather than on the plane's transport, so they stay
    usable with a stub and do not drag a broker into the exchange layer.

    Both directions are keyed by a :class:`~mft.exchange.tickers.\
UniversalTicker` rather than a bare symbol: on a unified-account venue the
    same symbol names two different instruments, and only the ticker says
    which. The inbound direction needs the venue and category but not the
    symbol — that is the answer — so it takes them as the two parts.
    """

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        """Canonical → the venue's spelling."""

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        """The venue's spelling → the universal ticker."""


def check_venue(ticker: UniversalTicker, venue: str, categories=None) -> None:
    """Refuse an order for an instrument this connector cannot trade.

    TD checks the same thing at its own boundary, where a strategy's mistake
    should be caught. This is the connector saying it for itself: it is also
    reachable directly, and an order routed to the wrong venue is the one
    mistake that cannot be undone by noticing later.
    """
    from mft.exchange.errors import OrderError

    if ticker.venue != venue:
        raise OrderError(
            f"{venue} client was handed a {ticker.venue} order: {ticker}"
        )
    if categories is not None and ticker.category not in categories:
        traded = ", ".join(sorted(c.value for c in categories))
        raise OrderError(
            f"{venue} client trades {traded}; {ticker} is "
            f"{ticker.category.value}"
        )


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
    "check_venue",
    "canonical",
    "canonical_symbol",
    "join",
]

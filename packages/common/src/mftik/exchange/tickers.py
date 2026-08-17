"""Universal tickers — one string that names an instrument on any venue.

A venue's API is organised one of two ways. **Classic** accounts give each
market its own credentials and its own endpoints, so Gate's spot plane and
Gate's futures plane are, to us, two venues that happen to share a brand.
**Unified** accounts give one credential and one endpoint and pass the market
as a parameter, so Bybit's spot and perp books are two categories inside a
single venue.

Folding the category into the venue name — ``gate_spot`` — only serves the
first kind. It cannot spell Bybit's two books without inventing venues that do
not exist, and it leaves the category invisible to everything downstream: one
connector, one credential, two markets, and no way to say which one an order
or a feed meant.

So identity is three parts, rendered as one string::

    <Venue>_<Category>_<SYMBOL>     Gate_Spot_BTCUSDT
                                    GateFutures_Perp_BTCUSDT
                                    Bybit_Spot_BTCUSDT
                                    Bybit_Perp_BTCUSDT

``_`` separates, and therefore cannot appear inside a part. Venues are
CamelCase (``GateFutures``, never ``gate_futures``) and symbols are canonical
(:func:`mftik.exchange.symbols.canonical`, so ``BTCUSDT``, never ``BTC_USDT``).
:func:`mftik.exchange.venues.check_registry` enforces the venue half of that at
import time, so a venue named with an underscore fails loudly at startup
rather than silently breaking every parse.

Two entry points, deliberately different:

* :meth:`UniversalTicker.parse` is strict and knows no venues. Anything on the
  wire or in the database is canonical already, and a parser that quietly
  accepted ``gate_spot_btcusdt`` would let two spellings of one instrument
  become two rows.
* :meth:`UniversalTicker.resolve` is lenient and registry-checked, for user
  input — an API query string, a line of strategy YAML.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from mftik.exchange.errors import ExchangeError
from mftik.exchange.symbols import canonical

#: What splits the three parts, and so what none of them may contain.
SEPARATOR = "_"

#: Venues are CamelCase: an uppercase initial, then letters and digits. These
#: are our own names, out of the registry, so ASCII is a fair constraint.
_VENUE_RE = re.compile(r"[A-Z][A-Za-z0-9]*\Z")


class InvalidTickerError(ExchangeError):
    """The string is not a well-formed universal ticker."""


class Category(StrEnum):
    """The market an instrument trades on, within a venue.

    Rendered into the ticker as spelled here, so the casing is load-bearing:
    ``Gate_Spot_BTCUSDT``, not ``Gate_spot_BTCUSDT``.
    """

    SPOT = "Spot"
    PERP = "Perp"
    FUTURE = "Future"
    OPTION = "Option"


_CATEGORY_BY_CASEFOLD = {c.value.casefold(): c for c in Category}


def category(name: str) -> Category:
    """Resolve a category name, ignoring case. Raises on an unknown one."""
    found = _CATEGORY_BY_CASEFOLD.get((name or "").strip().casefold())
    if found is None:
        known = ", ".join(c.value for c in Category)
        raise InvalidTickerError(
            f"unknown category {name!r}; known categories: {known}"
        )
    return found


@dataclass(frozen=True, order=True)
class UniversalTicker:
    """One instrument, anywhere — the platform's instrument identity.

    Frozen and ordered so it can key a dict, sit in a set, and sort without a
    key function. ``str(ticker)`` is the canonical rendering and the only form
    that belongs on the wire or in a column.
    """

    venue: str
    category: Category
    symbol: str

    def __str__(self) -> str:
        return SEPARATOR.join((self.venue, self.category.value, self.symbol))

    @property
    def value(self) -> str:
        """The rendered ticker. Same as ``str(self)``, named for readability."""
        return str(self)

    @classmethod
    def of(cls, venue: str, category_: str, symbol: str) -> UniversalTicker:
        """Build from parts, normalizing each. Raises on a malformed part."""
        return cls(
            venue=_check_venue(venue),
            category=category(category_),
            symbol=_check_symbol(canonical(symbol)),
        )

    @classmethod
    def parse(cls, text: str) -> UniversalTicker:
        """Strict: split a canonical ticker, no venue registry consulted.

        Use this for anything already inside the system — a wire payload, a
        database column, a feed key. It rejects a spelling it would have had
        to change, because a ticker that needed changing did not come from
        here and two spellings of one instrument is the bug this type exists
        to prevent.
        """
        parts = (text or "").split(SEPARATOR)
        if len(parts) != 3 or not all(parts):
            raise InvalidTickerError(
                f"invalid universal ticker {text!r}; expected "
                f"Venue{SEPARATOR}Category{SEPARATOR}SYMBOL, e.g. "
                f"Gate{SEPARATOR}Spot{SEPARATOR}BTCUSDT"
            )
        venue, category_, symbol = parts
        ticker = cls(
            venue=_check_venue(venue),
            category=category(category_),
            symbol=_check_symbol(symbol),
        )
        if ticker.value != text:
            raise InvalidTickerError(
                f"universal ticker {text!r} is not canonical; "
                f"expected {ticker.value!r}"
            )
        return ticker

    @classmethod
    def resolve(cls, text: str) -> UniversalTicker:
        """Lenient: accept a human's spelling and normalize it.

        ``gate_spot_btcusdt``, ``Gate_SPOT_BTC/USDT`` and ``Gate_Spot_BTCUSDT``
        all land on the same ticker. The venue is matched against the registry
        and the category against what that venue actually trades, so a typo is
        refused here rather than at deploy time.

        Only for user input. Everything downstream of the boundary that took
        it should be passing the parsed :class:`UniversalTicker` around.
        """
        # Deferred: the registry is built on this module, so importing it at
        # module scope would close a cycle. Only the lenient path needs it.
        from mftik.exchange import venues

        parts = (text or "").strip().split(SEPARATOR)
        if len(parts) != 3 or not all(parts):
            raise InvalidTickerError(
                f"invalid universal ticker {text!r}; expected "
                f"Venue{SEPARATOR}Category{SEPARATOR}SYMBOL, e.g. "
                f"Gate{SEPARATOR}Spot{SEPARATOR}BTCUSDT"
            )
        venue = venues.require(parts[0])
        return venue.ticker(parts[1], parts[2])


def _check_venue(name: str) -> str:
    if not _VENUE_RE.match(name or ""):
        raise InvalidTickerError(
            f"invalid venue {name!r} in a universal ticker; venues are "
            f"CamelCase with no {SEPARATOR!r} (e.g. 'Gate', 'GateFutures')"
        )
    return name


def _check_symbol(symbol: str) -> str:
    """Accept a symbol that is already canonical, whatever alphabet it is in.

    The test is ``canonical(s) == s`` rather than a character class, because
    the only two things this has to guarantee are that the symbol carries no
    separator (``canonical`` strips them, so one cannot survive the round
    trip) and that it is spelled the one way the platform spells it.

    Anything narrower is wrong about real listings. Gate lists pairs whose
    base currency is a CJK meme token — ``龙虾_USDT``, ``我踏马来了_USDT`` —
    and an ASCII-only rule rejects them at parse time. That does not cost
    four instruments: :meth:`SymbolClient._table` keys a whole venue's table
    by ``SymbolInfo.symbol``, so one unparseable row breaks every read on
    that venue in TD and MD.
    """
    if not symbol or canonical(symbol) != symbol:
        raise InvalidTickerError(
            f"invalid symbol {symbol!r} in a universal ticker; symbols are "
            f"canonical — uppercase, no separator (e.g. 'BTCUSDT')"
        )
    return symbol


__all__ = [
    "SEPARATOR",
    "Category",
    "InvalidTickerError",
    "UniversalTicker",
    "category",
]

"""Canonical instrument symbols.

Pair spellings collapse to one form — ``BTCUSDT``, uppercase, no separator —
and each adapter translates to its venue's form at the boundary. Dated
futures and options keep ``-`` between fields (``BTCUSDT-250926``,
``BTCUSDT-260905-100000-C``); :func:`normalize_symbol` is the function
that knows that grammar. :func:`canonical` only folds pair punctuation
so ``BTC-USDT`` and ``btc/usdt`` still look up as one instrument.

Translation is a **lookup, not a transform**. Deriving a venue ticker from a
canonical symbol needs the base/quote split, which ``BTCUSDT`` does not carry,
and guessing it is not merely imprecise — on real Gate data ``USDTUSD`` splits
to ``(USD, TUSD)`` by longest-suffix matching, which is a different instrument
and would route an order to ``USD_TUSD``. Venues also publish tickers that are
not ``base + separator + quote`` at all.

So the symbol plane (``apps/sym``) owns both directions, and adapters take a
:class:`SymbolResolver` rather than doing string surgery.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:  # Only for the annotation; tickers is built on this module.
    from mftik.exchange.tickers import UniversalTicker

_SEPARATORS = re.compile(r"[/\-_\s]")

#: ``YYMMDD`` as the venues write it onto a dated ticker. Six digits, not a
#: calendar check — the venue already named the contract; we only need to
#: see the field so we do not glue it to the pair.
_DATE_CODE = re.compile(r"\d{6}\Z")

#: Categories whose symbol may carry ``-`` as a field separator.
_STRUCTURED = frozenset({"future", "option"})

#: How a fractional strike writes its decimal point: ``6.4`` is ``6D4``.
#:
#: A strike has to be spelled somehow and ``.`` is the obvious candidate,
#: but ``.`` is also what a feed key uses to separate its topic from its
#: ticker (``bestquote.Gate_Spot_BTCUSDT``). Every reader of one splits
#: leftmost today, so a ``.`` inside a ticker would survive by
#: implementation rather than by grammar — and the first time somebody
#: split from the right it would break somewhere no test looks. A letter
#: cannot collide with the fields either side, which are a six-digit date
#: and a ``C``/``P``, and a strike is a number so no digit is displaced.
#:
#: Uppercase because every other character in a symbol is. That is not
#: only tidiness: :func:`normalize_symbol` uppercases before it splits,
#: so a strike typed ``6d4`` and one typed ``6.4`` land on the same
#: spelling for free. A lowercase marker would need that case folded back
#: by hand, and the spelling that got missed would be a second identity
#: for one strike.
STRIKE_DECIMAL = "D"

#: The punctuation :func:`canonical` folds out of a pair, minus ``-`` —
#: the one character the structured grammar promotes to a field separator.
#: Refused inside a symbol rather than folded away, because
#: :func:`normalize_symbol` does not fold every field it keeps: an option's
#: strike is passed through verbatim, so a ``_`` there would otherwise ride
#: into a ticker that :meth:`UniversalTicker.parse` cannot split back.
#:
#: ``.`` is refused because a fractional strike spells its decimal point
#: :data:`STRIKE_DECIMAL`, not because it is meaningless — which is why
#: :func:`normalize_symbol` folds it rather than rejecting the input.
#: Refusing it *here* is what keeps the folded form the only one that can
#: reach a column, exactly as ``BTC/USDT`` folds while
#: ``Gate_Spot_BTC/USDT`` does not parse.
_FORBIDDEN_IN_SYMBOL = re.compile(r"[/_.\s]")


class SymbolResolver(Protocol):
    """Two-way symbol translation, backed by the symbol plane.

    Adapters depend on this rather than on the plane's transport, so they stay
    usable with a stub and do not drag a broker into the exchange layer.

    Both directions are keyed by a :class:`~mftik.exchange.tickers.\
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

    async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
        """How much base one venue-native size unit is.

        ``None`` on books that already size in the asset (spot, Binance
        USD-M, Bybit linear). Gate futures and OKX SWAP size in contracts;
        the adapter converts at the wire using this, and refuses rather
        than guessing ``1``.
        """


def check_venue(ticker: UniversalTicker, venue: str, categories=None) -> None:
    """Refuse an order for an instrument this connector cannot trade.

    TD checks the same thing at its own boundary, where a strategy's mistake
    should be caught. This is the connector saying it for itself: it is also
    reachable directly, and an order routed to the wrong venue is the one
    mistake that cannot be undone by noticing later.
    """
    from mftik.exchange.errors import OrderError

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
    """Normalize a pair spelling for lookup.

    ``BTC_USDT``, ``BTC-USDT``, ``btc/usdt`` and ``BTCUSDT`` all collapse to
    ``BTCUSDT``. This makes user input uniform; it does not tell you the
    venue's ticker — ask the resolver for that. Dated and option symbols
    go through :func:`normalize_symbol` instead, which keeps their ``-``.
    """
    return _SEPARATORS.sub("", (symbol or "").strip()).upper()


def normalize_symbol(symbol: str, *, category: str | None = None) -> str:
    """The platform spelling of a symbol, given the book it trades on.

    Spot, perp and inverse stay a pair: separators fall out, so
    ``BTC-USDT`` and ``btc/usdt`` become ``BTCUSDT``. Future and option
    keep ``-`` between fields, and a hyphenated pair plus a date still
    lands on one identity (``BTC-USDT-250926`` → ``BTCUSDT-250926``).
    The glued form ``BTCUSDT250926`` is accepted on those books so
    older YAML and typed input do not fork a second instrument.

    A fractional strike is spelled with :data:`STRIKE_DECIMAL`, and a
    decimal point is folded onto it here — ``AVAXUSDC-260905-6.4-C``
    becomes ``AVAXUSDC-260905-6D4-C``. Same shape as ``BTC/USDT`` folding
    to ``BTCUSDT``: the spelling a venue or a person writes is taken, and
    the one the platform stores comes back.
    """
    raw = (symbol or "").strip().upper()
    if not raw:
        return ""
    kind = str(category).strip().casefold() if category is not None else ""
    if kind not in _STRUCTURED:
        return canonical(raw)
    parts = raw.split("-")
    if _is_option_parts(parts):
        pair = canonical("-".join(parts[:-3]))
        if pair:
            return f"{pair}-{parts[-3]}-{_strike(parts[-2])}-{parts[-1]}"
    if len(parts) >= 2 and _DATE_CODE.match(parts[-1]):
        pair = canonical("-".join(parts[:-1]))
        if pair:
            return f"{pair}-{parts[-1]}"
    compact = canonical(raw)
    if len(compact) > 6 and compact[-6:].isdigit():
        pair, date = compact[:-6], compact[-6:]
        if pair:
            return f"{pair}-{date}"
    return compact


def forbidden_in_symbol(symbol: str) -> str | None:
    """The first character a platform symbol may not contain, or ``None``.

    A symbol is checked for this rather than trusted to
    :func:`normalize_symbol`, because normalizing is not the same as
    validating: the structured grammar keeps some fields verbatim, so a
    spelling can be its own normal form and still be unspellable as a
    ticker. ``_`` separates the ticker's three parts and whitespace and
    ``/`` are pair punctuation that only ever reach here through a field
    ``normalize_symbol`` passes through.
    """
    found = _FORBIDDEN_IN_SYMBOL.search(symbol or "")
    return found.group(0) if found else None


def _strike(field: str) -> str:
    """A strike in platform form: ``6.4``, ``6d4`` and ``6D4`` all land here.

    Only the decimal point needs folding. The case is already folded by
    the time this runs — :func:`normalize_symbol` uppercases before it
    splits — which is the whole reason :data:`STRIKE_DECIMAL` is
    uppercase. A lowercase marker would make this two folds, and two
    spellings of one strike the first time one of them was forgotten.
    """
    return field.replace(".", STRIKE_DECIMAL)


def _is_option_parts(parts: list[str]) -> bool:
    return (
        len(parts) >= 4
        and parts[-1] in {"C", "P"}
        and _DATE_CODE.match(parts[-3]) is not None
        and parts[-2] != ""
    )


def join(base: str, quote: str, separator: str = "") -> str:
    """Render a pair in a venue's spelling from known base/quote."""
    return f"{base.upper()}{separator}{quote.upper()}"


#: Unambiguous alias for re-export at the ``mftik.exchange`` level.
canonical_symbol = canonical

__all__ = [
    "STRIKE_DECIMAL",
    "SymbolResolver",
    "check_venue",
    "canonical",
    "canonical_symbol",
    "forbidden_in_symbol",
    "join",
    "normalize_symbol",
]

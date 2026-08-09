"""Venue registry — the canonical list of venues credentials can belong to.

``apis.venue`` is a free-form column, but the value has to match a venue the
platform can actually connect to: TD picks a private client off it and MD picks
a public one. A typo like ``Gate-Spot`` would otherwise store happily and only
fail much later, at deploy time. This module is the single place that says
which names are real and what each one needs.

A venue here is **one connection with one credential**, which is the axis the
API is actually organised on. Gate's spot and futures planes sign separately
and speak to separate endpoints, so they are two venues (``Gate``,
``GateFutures``) that happen to share a brand. Bybit's unified account signs
once for both books, so it is one venue with two categories. What a venue
trades is :attr:`Venue.categories`; which one an instrument is on is the middle
part of its :class:`~mft.exchange.tickers.UniversalTicker`.

Adding a venue is a one-entry change here plus the client wiring in TD/MD.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mft.exchange.errors import ExchangeError
from mft.exchange.tickers import (
    SEPARATOR,
    Category,
    UniversalTicker,
)
from mft.exchange.tickers import category as resolve_category

HMAC = "HMAC"
ED25519 = "ED25519"


class UnknownVenueError(ExchangeError):
    """The venue name is not in the registry."""


class UnsupportedApiTypeError(ExchangeError):
    """The venue does not accept that credential algorithm."""


class UnsupportedCategoryError(ExchangeError):
    """The venue does not trade that instrument category."""


@dataclass(frozen=True)
class Venue:
    """One tradeable venue and what a credential for it must look like."""

    #: Canonical id, stored in ``apis.venue`` and rendered as the first part of
    #: a universal ticker. CamelCase, no separator — see :mod:`.tickers`.
    name: str
    #: Human-readable name for the UI.
    label: str
    #: Markets this venue trades. A classic-account venue has exactly one; a
    #: unified-account venue has several behind the same credential.
    categories: frozenset[Category] = field(
        default_factory=lambda: frozenset({Category.SPOT})
    )
    #: Credential algorithms this venue's adapter can sign with.
    api_types: frozenset[str] = field(default_factory=lambda: frozenset({HMAC}))
    #: Whether a passphrase is part of the credential (OKX-style venues).
    requires_passphrase: bool = False
    #: Simulated venue — no real money, credentials are arbitrary.
    simulated: bool = False

    @property
    def default_category(self) -> Category:
        """The category to assume when a caller names none.

        Only meaningful for a single-market venue; a unified venue has no such
        thing and callers must say which book they mean.
        """
        if len(self.categories) != 1:
            raise UnsupportedCategoryError(
                f"venue {self.name!r} trades {_categories(self)} — a category "
                f"must be named explicitly"
            )
        return next(iter(self.categories))

    @property
    def ticker_example(self) -> str:
        """A well-formed ticker on this venue, for UI hints."""
        example = sorted(self.categories)[0]
        return str(UniversalTicker(self.name, example, "BTCUSDT"))

    def ticker(self, category: str | None, symbol: str) -> UniversalTicker:
        """Build a ticker on this venue, refusing a category it does not trade.

        ``category`` may be ``None`` on a single-market venue, and is matched
        ignoring case — this is the lenient boundary user input comes through.
        """
        resolved = (
            self.default_category
            if category is None
            else resolve_category(category)
        )
        if resolved not in self.categories:
            raise UnsupportedCategoryError(
                f"venue {self.name!r} does not trade {resolved.value}; "
                f"it trades {_categories(self)}"
            )
        return UniversalTicker.of(self.name, resolved.value, symbol)


PAPER = Venue(
    name="Paper",
    label="Paper",
    simulated=True,
)

GATE = Venue(
    name="Gate",
    label="Gate Spot",
    # Gate's WebSocket v4 signs with HMAC-SHA512, both for private channel
    # subscribes and for trading calls.
    api_types=frozenset({HMAC}),
    requires_passphrase=False,
)

BINANCE = Venue(
    name="Binance",
    label="Binance Spot",
    # Binance's WebSocket API authenticates a connection with ``session.logon``
    # and accepts only Ed25519 keys for it. HMAC keys still work against its
    # REST API, but this adapter does not use REST at all — so an HMAC
    # credential could not place an order and is refused when it is stored.
    api_types=frozenset({ED25519}),
    requires_passphrase=False,
)

BYBIT = Venue(
    name="Bybit",
    label="Bybit",
    # The unified account this module's docstring is about: one credential and
    # one connection for both books, so the category is the instrument's
    # property rather than part of the venue's name. Options are left out until
    # something trades them — a category listed here is one the ticker parser
    # will accept, and accepting a market no adapter serves only moves the
    # failure later.
    categories=frozenset({Category.SPOT, Category.PERP}),
    # Bybit signs both its WebSocket auth and its REST calls with HMAC-SHA256.
    api_types=frozenset({HMAC}),
    requires_passphrase=False,
)

#: Every venue the platform knows, keyed by canonical name. The single source
#: of truth — :func:`get` scans it rather than keeping a second index, so a
#: test or a plugin that adds an entry here is immediately visible to lookups.
VENUES: dict[str, Venue] = {v.name: v for v in (PAPER, GATE, BINANCE, BYBIT)}


def check_registry() -> None:
    """Assert every registered name can survive a ticker round trip.

    A venue named ``gate_futures`` would parse as venue ``gate`` / category
    ``futures``, quietly aliasing a different instrument. Enforced at import
    so that mistake is a startup failure, not a data corruption.
    """
    for name, venue in VENUES.items():
        if SEPARATOR in name:
            raise ValueError(
                f"venue name {name!r} contains {SEPARATOR!r}, which separates "
                f"the parts of a universal ticker; use CamelCase instead"
            )
        if not venue.categories:
            raise ValueError(f"venue {name!r} trades no categories")
        # Round-trips through the strict parser, so a name the parser would
        # reject cannot be registered in the first place.
        UniversalTicker.parse(str(UniversalTicker(name, Category.SPOT, "BTCUSDT")))


check_registry()


def names() -> list[str]:
    """Canonical venue names, sorted."""
    return sorted(VENUES)


def all_venues() -> list[Venue]:
    """Every registered venue, ordered by name."""
    return [VENUES[name] for name in names()]


def get(name: str) -> Venue | None:
    """Look up a venue, normalizing case and surrounding whitespace.

    Case-insensitive because venue names arrive from user input and from rows
    written by older code; what comes back is always the canonical spelling.
    """
    wanted = (name or "").strip()
    if not wanted:
        return None
    found = VENUES.get(wanted)
    if found is not None:
        return found
    folded = wanted.casefold()
    return next((v for k, v in VENUES.items() if k.casefold() == folded), None)


def normalize(name: str) -> str:
    """A venue's canonical spelling, or the input trimmed if it is unknown."""
    venue = get(name)
    return venue.name if venue is not None else (name or "").strip()


def require(name: str) -> Venue:
    """Look up a venue or raise :class:`UnknownVenueError`."""
    venue = get(name)
    if venue is None:
        raise UnknownVenueError(
            f"unknown venue {name!r}; known venues: {', '.join(names())}"
        )
    return venue


def ticker(venue: str, symbol: str, category: str | None = None) -> UniversalTicker:
    """Build a ticker from loose parts — the lenient, registry-checked path."""
    return require(venue).ticker(category, symbol)


def validate_credential(name: str, api_type: str) -> Venue:
    """Check a venue/algorithm pair before a credential is stored.

    Returns the resolved :class:`Venue` so callers can persist its canonical
    ``name`` rather than whatever spelling arrived.
    """
    venue = require(name)
    normalized_type = (api_type or "").strip().upper()
    if normalized_type not in venue.api_types:
        raise UnsupportedApiTypeError(
            f"venue {venue.name!r} does not support type {normalized_type!r}; "
            f"supported: {', '.join(sorted(venue.api_types))}"
        )
    return venue


def _categories(venue: Venue) -> str:
    return ", ".join(sorted(c.value for c in venue.categories))


__all__ = [
    "BINANCE",
    "BYBIT",
    "ED25519",
    "GATE",
    "HMAC",
    "PAPER",
    "VENUES",
    "UnknownVenueError",
    "UnsupportedApiTypeError",
    "UnsupportedCategoryError",
    "Venue",
    "all_venues",
    "check_registry",
    "get",
    "names",
    "normalize",
    "require",
    "ticker",
    "validate_credential",
]

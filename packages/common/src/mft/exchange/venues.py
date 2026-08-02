"""Venue registry — the canonical list of venues credentials can belong to.

``apis.venue`` is a free-form column, but the value has to match a venue the
platform can actually connect to: TD picks a private client off it and MD picks
a public one. A typo like ``gate-spot`` would otherwise store happily and only
fail much later, at deploy time. This module is the single place that says
which names are real and what each one needs.

Adding a venue is a one-entry change here plus the client wiring in TD/MD.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mft.exchange.errors import ExchangeError

HMAC = "HMAC"
ED25519 = "ED25519"


class UnknownVenueError(ExchangeError):
    """The venue name is not in the registry."""


class UnsupportedApiTypeError(ExchangeError):
    """The venue does not accept that credential algorithm."""


@dataclass(frozen=True)
class Venue:
    """One tradeable venue and what a credential for it must look like."""

    #: Canonical id, stored in ``apis.venue``. Lowercase, underscore-separated.
    name: str
    #: Human-readable name for the UI.
    label: str
    #: Credential algorithms this venue's adapter can sign with.
    api_types: frozenset[str] = field(default_factory=lambda: frozenset({HMAC}))
    #: Whether a passphrase is part of the credential (OKX-style venues).
    requires_passphrase: bool = False
    #: Simulated venue — no real money, credentials are arbitrary.
    simulated: bool = False
    #: Canonical spelling of an instrument on this venue, for UI hints.
    #: Adapters render the venue's own form; this is what users write.
    symbol_example: str = ""


PAPER = Venue(
    name="paper",
    label="Paper",
    simulated=True,
    symbol_example="BTCUSDT",
)

GATE_SPOT = Venue(
    name="gate_spot",
    label="Gate Spot",
    # Gate's WebSocket v4 signs with HMAC-SHA512, both for private channel
    # subscribes and for trading calls.
    api_types=frozenset({HMAC}),
    requires_passphrase=False,
    symbol_example="BTCUSDT",
)

#: Every venue the platform knows, keyed by canonical name.
VENUES: dict[str, Venue] = {v.name: v for v in (PAPER, GATE_SPOT)}


def names() -> list[str]:
    """Canonical venue names, sorted."""
    return sorted(VENUES)


def all_venues() -> list[Venue]:
    """Every registered venue, ordered by name."""
    return [VENUES[name] for name in names()]


def get(name: str) -> Venue | None:
    """Look up a venue, normalizing case and surrounding whitespace."""
    return VENUES.get(normalize(name))


def normalize(name: str) -> str:
    return (name or "").strip().lower()


def require(name: str) -> Venue:
    """Look up a venue or raise :class:`UnknownVenueError`."""
    venue = get(name)
    if venue is None:
        raise UnknownVenueError(
            f"unknown venue {name!r}; known venues: {', '.join(names())}"
        )
    return venue


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


__all__ = [
    "ED25519",
    "GATE_SPOT",
    "HMAC",
    "PAPER",
    "VENUES",
    "UnknownVenueError",
    "UnsupportedApiTypeError",
    "Venue",
    "all_venues",
    "get",
    "names",
    "normalize",
    "require",
    "validate_credential",
]

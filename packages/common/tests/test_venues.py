"""Venue registry — what ``apis.venue`` is allowed to hold."""

from __future__ import annotations

import pytest
from mft.exchange import venues


def test_gate_spot_is_registered_as_its_own_venue() -> None:
    gate = venues.require("gate_spot")
    assert gate is venues.GATE_SPOT
    assert gate.name == "gate_spot"
    assert gate.label == "Gate Spot"
    assert not gate.simulated
    assert gate.symbol_example == "BTCUSDT"  # canonical, not BTC_USDT
    # Gate's WS v4 signs with HMAC-SHA512, for subscribes and trading calls.
    assert gate.api_types == frozenset({venues.HMAC})
    assert not gate.requires_passphrase


def test_registry_lists_every_venue() -> None:
    assert venues.names() == ["gate_spot", "paper"]
    assert [v.name for v in venues.all_venues()] == venues.names()
    assert venues.PAPER.simulated
    assert not venues.GATE_SPOT.simulated


@pytest.mark.parametrize(
    "spelling",
    ["gate_spot", "GATE_SPOT", " Gate_Spot ", "Gate_spot"],
)
def test_lookup_normalizes_case_and_whitespace(spelling: str) -> None:
    assert venues.get(spelling) is venues.GATE_SPOT


@pytest.mark.parametrize("bad", ["gate-spot", "gateio", "gate", "gate_futures", ""])
def test_near_miss_spellings_are_rejected(bad: str) -> None:
    """The whole point: a typo must fail now, not at deploy time."""
    assert venues.get(bad) is None
    with pytest.raises(venues.UnknownVenueError, match="unknown venue"):
        venues.require(bad)


def test_unknown_venue_error_lists_the_known_ones() -> None:
    with pytest.raises(venues.UnknownVenueError) as exc:
        venues.require("gate-spot")
    assert "gate_spot" in str(exc.value)
    assert "paper" in str(exc.value)


def test_validate_credential_returns_the_canonical_venue() -> None:
    """Callers persist the resolved name, not whatever spelling arrived."""
    resolved = venues.validate_credential("  GATE_SPOT ", "hmac")
    assert resolved.name == "gate_spot"


def test_validate_credential_rejects_unsupported_algorithm() -> None:
    with pytest.raises(venues.UnsupportedApiTypeError, match="ED25519"):
        venues.validate_credential("gate_spot", venues.ED25519)


def test_validate_credential_rejects_unknown_venue() -> None:
    with pytest.raises(venues.UnknownVenueError):
        venues.validate_credential("binance_spot", venues.HMAC)


def test_registry_errors_are_exchange_errors() -> None:
    """So callers can catch one type at the boundary."""
    from mft.exchange.errors import ExchangeError

    assert issubclass(venues.UnknownVenueError, ExchangeError)
    assert issubclass(venues.UnsupportedApiTypeError, ExchangeError)


def test_venue_is_immutable() -> None:
    with pytest.raises(Exception):
        venues.GATE_SPOT.name = "something-else"  # type: ignore[misc]

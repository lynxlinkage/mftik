"""Canonical symbol normalization.

Translation itself lives in the symbol plane — see ``apps/sym``. What is left
here is only making user input uniform before a lookup.
"""

from __future__ import annotations

import pytest
from decimal import Decimal

from mftik.exchange import symbols
from mftik.exchange.symbols import canonical, join
from mftik.exchange.tickers import UniversalTicker


@pytest.mark.parametrize(
    "spelling",
    ["BTCUSDT", "BTC_USDT", "BTC-USDT", "BTC/USDT", "btc_usdt", " BTC USDT "],
)
def test_every_spelling_collapses_to_one_canonical(spelling: str) -> None:
    """Spellings of the same pair carry identical information."""
    assert canonical(spelling) == "BTCUSDT"


def test_canonical_tolerates_empty_input() -> None:
    assert canonical("") == ""
    assert canonical("   ") == ""


def test_join_renders_a_venue_spelling_from_known_parts() -> None:
    assert join("btc", "usdt") == "BTCUSDT"
    assert join("BTC", "USDT", "_") == "BTC_USDT"


def test_the_suffix_heuristic_is_gone() -> None:
    """Guessing base/quote from a concatenated symbol is not a supported path.

    On real Gate data it produced silent errors — ``USDTUSD`` split to
    ``(USD, TUSD)``, a different instrument — so the plane owns this now.
    """
    assert not hasattr(symbols, "split_base_quote")
    assert not hasattr(symbols, "QUOTE_CURRENCIES")


def test_resolver_protocol_shape() -> None:
    """Adapters depend on this, not on the plane's transport."""

    class Stub:
        async def exch_ticker(self, ticker: UniversalTicker) -> str:
            return "BTC_USDT"

        async def symbol_for(
            self, venue: str, exch_ticker: str, *, category: str
        ) -> UniversalTicker:
            return UniversalTicker.of(venue, category, "BTCUSDT")

        async def contract_size(self, ticker: UniversalTicker) -> Decimal | None:
            return None

    resolver: symbols.SymbolResolver = Stub()
    assert resolver is not None

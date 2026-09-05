"""Canonical symbol normalization.

Translation itself lives in the symbol plane — see ``apps/sym``. What is left
here is only making user input uniform before a lookup.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange import symbols
from mftik.exchange.symbols import canonical, join, normalize_symbol
from mftik.exchange.tickers import Category, UniversalTicker


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


@pytest.mark.parametrize(
    ("symbol", "category", "want"),
    [
        ("BTC-USDT", "spot", "BTCUSDT"),
        ("BTCUSDT-250926", "spot", "BTCUSDT250926"),
        ("BTCUSDT250926", "future", "BTCUSDT-250926"),
        ("btc-usdt-250926", "future", "BTCUSDT-250926"),
        ("BTCUSDT-250926", "future", "BTCUSDT-250926"),
        ("BTC-USDT-260905-100000-C", "option", "BTCUSDT-260905-100000-C"),
        ("AVAXUSDC-260905-6.4-c", "option", "AVAXUSDC-260905-6.4-C"),
        ("BTCUSDT", Category.PERP, "BTCUSDT"),
    ],
)
def test_normalize_symbol_keeps_structured_hyphens(
    symbol: str, category: str | Category, want: str
) -> None:
    assert normalize_symbol(symbol, category=category) == want


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

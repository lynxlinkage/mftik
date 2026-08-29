"""Universal tickers — the platform's one instrument identity.

The parser is deliberately two-faced: strict for anything already inside the
system, lenient at the boundaries people type into. Most of what is here is
about keeping those two apart, because collapsing them is how one instrument
becomes two rows.
"""

from __future__ import annotations

import pytest
from mftik.exchange.tickers import (
    Category,
    InvalidTickerError,
    UniversalTicker,
    category,
)


def test_a_ticker_renders_its_three_parts() -> None:
    ticker = UniversalTicker(venue="Gate", category=Category.SPOT, symbol="BTCUSDT")
    assert str(ticker) == "Gate_Spot_BTCUSDT"
    assert ticker.value == "Gate_Spot_BTCUSDT"


def test_parse_round_trips() -> None:
    for text in (
        "Gate_Spot_BTCUSDT",
        "GateFutures_Perp_BTCUSDT",
        "Bybit_Spot_ETHUSDT",
        "Bybit_Perp_ETHUSDT",
        "Paper_Spot_BTCUSDT",
        "BinanceDelivery_Inverse_BTCUSD",
    ):
        assert str(UniversalTicker.parse(text)) == text


def test_the_two_sides_of_a_unified_venue_are_different_tickers() -> None:
    """The whole reason the category is in the identity at all."""
    spot = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    assert spot != perp
    assert spot.venue == perp.venue == "Bybit"
    assert spot.symbol == perp.symbol == "BTCUSDT"
    assert len({spot, perp}) == 2  # hashable and distinct — they key feeds


def test_a_classic_venue_puts_the_market_in_the_venue_name() -> None:
    """Gate spot and Gate futures are separate credentials, so separate venues."""
    spot = UniversalTicker.parse("Gate_Spot_BTCUSDT")
    perp = UniversalTicker.parse("GateFutures_Perp_BTCUSDT")
    assert spot.venue != perp.venue


def test_tickers_sort_and_key_dictionaries() -> None:
    a = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    b = UniversalTicker.parse("Gate_Spot_BTCUSDT")
    assert sorted([b, a]) == [a, b]
    assert {a: 1, b: 2}[a] == 1


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "Gate_Spot",  # missing a part
        "Gate_Spot_BTC_USDT",  # symbol carrying a separator
        "Gate__BTCUSDT",  # empty part
        "Gate_Bogus_BTCUSDT",  # not a category
        "gate_Spot_BTCUSDT",  # venue not CamelCase
        "Gate_spot_BTCUSDT",  # category not canonically spelled
        "Gate_Spot_btcusdt",  # symbol not canonical
        "Gate_Spot_BTC-USDT",  # separator inside the symbol
    ],
)
def test_parse_is_strict(bad: str) -> None:
    """Strict on purpose: a spelling it would have had to change is a bug.

    Everything that reaches ``parse`` — a wire payload, a column, a feed key —
    was written by this system in canonical form. Accepting anything else means
    two spellings of one instrument, which refcount as two feeds and store as
    two rows.
    """
    with pytest.raises(InvalidTickerError):
        UniversalTicker.parse(bad)


@pytest.mark.parametrize(
    "symbol", ["龙虾USDT", "老子USDT", "我踏马来了USDT", "小股东USDT"]
)
def test_a_symbol_need_not_be_ascii(symbol: str) -> None:
    """Gate really lists these — CJK meme tokens against USDT.

    An ASCII-only rule does not cost four instruments, it costs the venue:
    ``SymbolClient._table`` keys a whole venue's table by
    ``SymbolInfo.symbol``, so one unparseable row raises on every Gate read
    in TD and MD. What a symbol must be is canonical and separator-free,
    which is a different claim from being ``[A-Z0-9]+``.
    """
    text = f"Gate_Spot_{symbol}"
    assert str(UniversalTicker.parse(text)) == text
    assert UniversalTicker.parse(text).symbol == symbol


def test_a_non_ascii_symbol_still_has_to_be_canonical() -> None:
    """Widening the alphabet must not weaken the separator rule."""
    with pytest.raises(InvalidTickerError):
        UniversalTicker.parse("Gate_Spot_龙虾_USDT")
    assert str(UniversalTicker.of("Gate", "Spot", "龙虾/usdt")) == "Gate_Spot_龙虾USDT"


@pytest.mark.parametrize(
    "loose",
    [
        "Gate_Spot_BTCUSDT",
        "gate_spot_btcusdt",
        "GATE_SPOT_BTC/USDT",
        " Gate_spot_btc-usdt ",
    ],
)
def test_resolve_is_lenient_and_lands_on_one_spelling(loose: str) -> None:
    assert str(UniversalTicker.resolve(loose)) == "Gate_Spot_BTCUSDT"


def test_resolve_checks_the_venue_registry() -> None:
    with pytest.raises(Exception, match="unknown venue"):
        UniversalTicker.resolve("Kraken_Spot_BTCUSDT")


def test_resolve_checks_the_venue_actually_trades_the_category() -> None:
    with pytest.raises(Exception, match="does not trade"):
        UniversalTicker.resolve("Gate_Perp_BTCUSDT")


def test_of_normalizes_its_parts() -> None:
    assert str(UniversalTicker.of("Gate", "spot", "btc/usdt")) == "Gate_Spot_BTCUSDT"


def test_category_lookup_ignores_case() -> None:
    assert category("perp") is Category.PERP
    assert category("inverse") is Category.INVERSE
    assert category(" SPOT ") is Category.SPOT
    with pytest.raises(InvalidTickerError, match="unknown category"):
        category("futures")


def test_invalid_ticker_is_an_exchange_error() -> None:
    """So a boundary can catch one type for every malformed instrument id."""
    from mftik.exchange.errors import ExchangeError

    assert issubclass(InvalidTickerError, ExchangeError)

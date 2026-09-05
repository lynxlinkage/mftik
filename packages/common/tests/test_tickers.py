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
        "BinanceCM_Inverse_BTCUSD",
        "BinanceCM_Future_BTCUSD-260925",
        "BinanceUM_Future_BTCUSDT-250926",
        "BinanceUM_Option_BTCUSDT-260905-100000-C",
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
        "Gate_Spot_BTC-USDT",  # pair hyphen is not a dated suffix
        "BinanceUM_Future_BTCUSDT250926",  # glued dated form is no longer stored
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
    in TD and MD. What a symbol must be is in platform form — separator-free
    on a pair, hyphenated fields on a dated or option contract — which is
    a different claim from being ``[A-Z0-9]+``.
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
    "strike", ["10_000", "10 000", "10/000"]
)
def test_of_refuses_a_field_it_would_render_unparseable(strike: str) -> None:
    """``of`` must not build what ``parse`` will not read back.

    The structured grammar keeps an option's strike verbatim rather than
    folding it, so a strike carrying pair punctuation is its own normal
    form. Left to the ``normalize_symbol(s) == s`` test alone it would pass
    — and then render ``Bybit_Option_BTCUSDT-260905-10_000-C``, which
    splits into four parts and is refused on the way home. A value the
    lenient boundary can build but the strict one cannot read is the exact
    failure this type exists to prevent, so it is refused where it is made.
    """
    with pytest.raises(InvalidTickerError, match="cannot"):
        UniversalTicker.of("Bybit", "option", f"BTCUSDT-260905-{strike}-C")


def test_a_fractional_strike_is_refused_until_an_encoding_is_chosen() -> None:
    """Not a parse failure — a decision nobody has made yet.

    ``.`` would survive a ticker today, but only because every reader of a
    feed key (``bestquote.Gate_Spot_BTCUSDT``) happens to split leftmost.
    That is an implementation, not a grammar, and no venue here lists an
    option yet — so the cheap moment to refuse it is now, while refusing
    costs nothing. The epic that lists the first option with a fractional
    strike picks the encoding on purpose and lifts this.
    """
    with pytest.raises(InvalidTickerError, match=r"'\.' cannot"):
        UniversalTicker.of("Bybit", "option", "AVAXUSDC-260905-6.4-C")


def test_every_symbol_of_builds_survives_a_round_trip() -> None:
    """The invariant the check above defends, stated as the round trip."""
    for symbol in (
        "BTCUSDT",
        "BTCUSDT-250926",
        "BTCUSDT-260905-100000-C",
        "BTCUSDT-260905-100000-P",
        "龙虾USDT",
    ):
        category = "option" if symbol.count("-") == 3 else (
            "future" if "-" in symbol else "spot"
        )
        built = UniversalTicker.of("Bybit", category, symbol)
        assert UniversalTicker.parse(str(built)) == built


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


def test_resolve_accepts_a_binance_future_dated_contract() -> None:
    assert str(
        UniversalTicker.resolve("binanceum_future_btcusdt250926")
    ) == "BinanceUM_Future_BTCUSDT-250926"
    assert str(
        UniversalTicker.resolve("binanceum_future_btcusdt-250926")
    ) == "BinanceUM_Future_BTCUSDT-250926"


def test_of_normalizes_its_parts() -> None:
    assert str(UniversalTicker.of("Gate", "spot", "btc/usdt")) == "Gate_Spot_BTCUSDT"
    assert str(
        UniversalTicker.of("BinanceUM", "future", "btc-usdt-250926")
    ) == "BinanceUM_Future_BTCUSDT-250926"
    assert str(
        UniversalTicker.of("BinanceUM", "option", "btc-usdt-260905-100000-c")
    ) == "BinanceUM_Option_BTCUSDT-260905-100000-C"


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

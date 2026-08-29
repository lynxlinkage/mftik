"""Venue registry — what ``apis.venue`` is allowed to hold."""

from __future__ import annotations

import pytest
from mftik.exchange import venues
from mftik.exchange.tickers import SEPARATOR, Category, UniversalTicker


def test_gate_is_registered_as_its_own_venue() -> None:
    gate = venues.require("Gate")
    assert gate is venues.GATE
    assert gate.name == "Gate"
    assert gate.label == "Gate Spot"
    assert not gate.simulated
    # A classic account: Gate's futures plane signs separately and will be its
    # own venue, so this one trades spot and nothing else.
    assert gate.categories == frozenset({Category.SPOT})
    assert gate.ticker_example == "Gate_Spot_BTCUSDT"
    # Gate's WS v4 signs with HMAC-SHA512, for subscribes and trading calls.
    assert gate.api_types == frozenset({venues.HMAC})
    assert not gate.requires_passphrase


def test_registry_lists_every_venue() -> None:
    assert venues.names() == [
        "Binance",
        "BinanceDelivery",
        "BinanceFuture",
        "Bybit",
        "Gate",
        "GateFutures",
        "Okx",
        "Paper",
    ]
    assert [v.name for v in venues.all_venues()] == venues.names()
    assert venues.PAPER.simulated
    assert not venues.GATE.simulated
    assert not venues.BINANCE.simulated
    assert not venues.BINANCE_FUTURE.simulated
    assert not venues.BINANCE_DELIVERY.simulated
    assert not venues.BYBIT.simulated
    assert not venues.OKX.simulated


def test_bybit_is_one_venue_trading_two_books() -> None:
    """The unified account the ticker's category part exists for.

    Gate's futures plane will be a second venue because it signs separately;
    Bybit's perp book is the *same* credential and the same connection, so
    folding it into the name would invent a venue that does not exist.
    """
    bybit = venues.require("Bybit")
    assert bybit.categories == frozenset({Category.SPOT, Category.PERP})
    assert bybit.api_types == frozenset({venues.HMAC})
    assert not bybit.requires_passphrase
    assert str(bybit.ticker("spot", "btc/usdt")) == "Bybit_Spot_BTCUSDT"
    assert str(bybit.ticker("perp", "BTCUSDT")) == "Bybit_Perp_BTCUSDT"


def test_okx_is_one_venue_trading_two_books_and_needs_a_passphrase() -> None:
    """Same unified-account shape as Bybit, plus the passphrase the API key
    is signed with."""
    okx = venues.require("Okx")
    assert okx.categories == frozenset({Category.SPOT, Category.PERP})
    assert okx.api_types == frozenset({venues.HMAC})
    assert okx.requires_passphrase
    assert str(okx.ticker("spot", "btc/usdt")) == "Okx_Spot_BTCUSDT"
    assert str(okx.ticker("perp", "BTCUSDT")) == "Okx_Perp_BTCUSDT"


def test_a_unified_venue_refuses_to_guess_a_category() -> None:
    """Two books, so a caller naming none has not said what it meant."""
    with pytest.raises(venues.UnsupportedCategoryError, match="explicitly"):
        venues.BYBIT.default_category
    with pytest.raises(venues.UnsupportedCategoryError):
        venues.ticker("bybit", "BTCUSDT")


def test_binance_signs_with_ed25519_and_nothing_else() -> None:
    """Its WebSocket API session logon takes no other algorithm.

    HMAC keys work against Binance's REST API, but this adapter never touches
    REST — so an HMAC credential stored here could not place an order, and the
    registry refuses it rather than letting that surface at deploy time.
    """
    assert venues.BINANCE.api_types == frozenset({venues.ED25519})
    assert not venues.BINANCE.requires_passphrase
    assert venues.BINANCE.categories == frozenset({Category.SPOT})
    assert venues.BINANCE.ticker_example == "Binance_Spot_BTCUSDT"

    with pytest.raises(venues.UnsupportedApiTypeError, match="ED25519"):
        venues.validate_credential("Binance", venues.HMAC)


@pytest.mark.parametrize("spelling", ["Gate", "GATE", " gate ", "gATe"])
def test_lookup_normalizes_case_and_whitespace(spelling: str) -> None:
    assert venues.get(spelling) is venues.GATE


@pytest.mark.parametrize("bad", ["gate-spot", "gateio", "gate_spot", ""])
def test_near_miss_spellings_are_rejected(bad: str) -> None:
    """The whole point: a typo must fail now, not at deploy time.

    ``gate_spot`` is in here deliberately. It was the venue's name before
    categories became part of an instrument's identity, and it must not
    quietly keep resolving — a stored credential still spelled that way has to
    surface as an error rather than as a venue nobody meant.
    """
    assert venues.get(bad) is None
    with pytest.raises(venues.UnknownVenueError, match="unknown venue"):
        venues.require(bad)


def test_unknown_venue_error_lists_the_known_ones() -> None:
    with pytest.raises(venues.UnknownVenueError) as exc:
        venues.require("gate-spot")
    assert "Gate" in str(exc.value)
    assert "Paper" in str(exc.value)


def test_validate_credential_returns_the_canonical_venue() -> None:
    """Callers persist the resolved name, not whatever spelling arrived."""
    resolved = venues.validate_credential("  gate ", "hmac")
    assert resolved.name == "Gate"


def test_validate_credential_rejects_unsupported_algorithm() -> None:
    with pytest.raises(venues.UnsupportedApiTypeError, match="ED25519"):
        venues.validate_credential("Gate", venues.ED25519)


def test_validate_credential_rejects_unknown_venue() -> None:
    with pytest.raises(venues.UnknownVenueError):
        venues.validate_credential("binance_spot", venues.HMAC)


def test_registry_errors_are_exchange_errors() -> None:
    """So callers can catch one type at the boundary."""
    from mftik.exchange.errors import ExchangeError

    assert issubclass(venues.UnknownVenueError, ExchangeError)
    assert issubclass(venues.UnsupportedApiTypeError, ExchangeError)
    assert issubclass(venues.UnsupportedCategoryError, ExchangeError)


def test_venue_is_immutable() -> None:
    with pytest.raises(Exception):
        venues.GATE.name = "something-else"  # type: ignore[misc]


# --- tickers off the registry ---------------------------------------------


def test_a_venue_builds_tickers_on_its_own_markets() -> None:
    assert str(venues.GATE.ticker("spot", "btc/usdt")) == "Gate_Spot_BTCUSDT"
    assert str(venues.ticker("gate", "BTCUSDT")) == "Gate_Spot_BTCUSDT"


def test_gate_futures_is_its_own_perp_venue() -> None:
    """Separate credential, separate host — not a category of ``Gate``."""
    fut = venues.require("GateFutures")
    assert fut is venues.GATE_FUTURES
    assert fut.categories == frozenset({Category.PERP})
    assert fut.api_types == frozenset({venues.HMAC})
    assert fut.ticker_example == "GateFutures_Perp_BTCUSDT"
    assert str(fut.ticker(None, "BTCUSDT")) == "GateFutures_Perp_BTCUSDT"
    with pytest.raises(venues.UnsupportedCategoryError, match="does not trade"):
        fut.ticker("spot", "BTCUSDT")


def test_binance_delivery_is_its_own_perp_venue() -> None:
    """dapi — separate credential and host from spot and from USD-M."""
    coin = venues.require("BinanceDelivery")
    assert coin is venues.BINANCE_DELIVERY
    assert coin.label == "Binance COIN-M Futures"
    assert coin.categories == frozenset({Category.INVERSE})
    assert coin.api_types == frozenset({venues.ED25519})
    assert coin.ticker_example == "BinanceDelivery_Inverse_BTCUSD"
    assert str(coin.ticker(None, "BTCUSD")) == "BinanceDelivery_Inverse_BTCUSD"
    with pytest.raises(venues.UnsupportedCategoryError, match="does not trade"):
        coin.ticker("spot", "BTCUSD")
    with pytest.raises(venues.UnsupportedCategoryError, match="does not trade"):
        coin.ticker("perp", "BTCUSD")
    with pytest.raises(venues.UnsupportedApiTypeError, match="ED25519"):
        venues.validate_credential("BinanceDelivery", venues.HMAC)


def test_every_venue_hint_names_an_instrument_that_venue_could_list() -> None:
    """A hint is copied verbatim out of the UI, so it has to be well formed.

    ``example_symbol`` defaults to ``BTCUSDT`` because almost every venue
    quotes in USDT — a venue that does not has to say so, and this is what
    notices when a new one forgets.
    """
    for venue in venues.VENUES.values():
        parsed = UniversalTicker.parse(venue.ticker_example)
        assert parsed.venue == venue.name
        assert parsed.category in venue.categories
        assert str(venue.ticker(parsed.category.value, parsed.symbol)) == (
            venue.ticker_example
        )


def test_a_category_the_venue_does_not_trade_is_refused() -> None:
    with pytest.raises(venues.UnsupportedCategoryError, match="does not trade"):
        venues.GATE.ticker("perp", "BTCUSDT")


def test_a_single_market_venue_infers_its_category() -> None:
    """So a caller naming only a symbol still gets a well-formed ticker."""
    assert venues.PAPER.default_category is Category.SPOT
    assert str(venues.PAPER.ticker(None, "BTCUSDT")) == "Paper_Spot_BTCUSDT"


def test_no_registered_name_can_break_a_ticker_parse() -> None:
    """A venue named ``gate_futures`` would alias a different instrument.

    ``check_registry`` runs at import, so this restates the invariant rather
    than discovering it — but it is the one that makes ``UniversalTicker.parse``
    a plain split, and it should fail loudly if anyone relaxes it.
    """
    venues.check_registry()
    assert all(SEPARATOR not in name for name in venues.names())

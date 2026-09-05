"""Deribit JSON-RPC framing, signing, listing filter (V1 / V2 / V3 / V12)."""

from __future__ import annotations

from mftik.exchange.deribit import protocol as p
from mftik.exchange.deribit.listing import to_listed
from mftik.exchange.deribit.models import DeribitSummary
from mftik.exchange.tickers import Category, UniversalTicker

SPOT_NATIVE = {
    "instrument_name": "BTC_USDC",
    "kind": "spot",
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "tick_size": "0.01",
    "min_trade_amount": "0.0001",
    "is_active": True,
}

SPOT_CBE = {
    "instrument_name": "SOL_USDC",
    "kind": "spot",
    "base_currency": "SOL",
    "quote_currency": "USDC",
    "tick_size": "0.01",
    "min_trade_amount": "0.01",
    "is_active": True,
    "is_cbe_routed": True,
    "is_csr": True,
}

PERP_LINEAR = {
    "instrument_name": "BTC_USDC-PERPETUAL",
    "kind": "future",
    "instrument_type": "linear",
    "future_type": "linear",
    "settlement_period": "perpetual",
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "settlement_currency": "USDC",
    "tick_size": "0.1",
    "min_trade_amount": "0.0001",
    "contract_size": "1",
    "is_active": True,
}

PERP_INVERSE = {
    "instrument_name": "BTC-PERPETUAL",
    "kind": "future",
    "instrument_type": "reversed",
    "future_type": "reversed",
    "settlement_period": "perpetual",
    "base_currency": "BTC",
    "quote_currency": "USD",
    "settlement_currency": "BTC",
    "tick_size": "0.5",
    "min_trade_amount": "10",
    "is_active": True,
}

DATED = {
    "instrument_name": "BTC-6SEP26",
    "kind": "future",
    "instrument_type": "linear",
    "settlement_period": "week",
    "base_currency": "BTC",
    "quote_currency": "USD",
    "is_active": True,
}


def test_v1_ws_sign_matches_the_published_vector() -> None:
    """timestamp + newline + nonce + newline + data; milliseconds."""
    assert (
        p.sign_ws("AMANDASECRECT", 1576074319000, "1iqt2wls", "")
        == "56590594f97921b09b18f166befe0d1319b198bbcdad7ca73382de2f88fe9aa1"
    )


def test_v1_http_sign_is_a_different_formula() -> None:
    ws = p.sign_ws("secret", 1576074319000, "nonce", "")
    rest = p.sign_rest(
        "secret",
        timestamp=1576074319000,
        nonce="nonce",
        method="GET",
        uri="/api/v2/private/get_account_summaries",
        body="",
    )
    assert rest != ws
    assert len(rest) == 64


def test_auth_params_use_client_id_and_milliseconds() -> None:
    params = p.auth_params(
        api_key="cid",
        api_secret="secret",
        timestamp=1576074319000,
        nonce="1iqt2wls",
    )
    assert params["grant_type"] == "client_signature"
    assert params["client_id"] == "cid"
    assert params["timestamp"] == 1576074319000
    assert len(str(params["timestamp"])) == 13
    assert params["signature"] == p.sign_ws("secret", 1576074319000, "1iqt2wls", "")


def test_v2_spot_identity_is_base_plus_quote() -> None:
    listed = to_listed(SPOT_NATIVE, category=Category.SPOT)
    assert listed is not None
    assert str(listed.ticker) == "Deribit_Spot_BTCUSDC"
    assert listed.exch_ticker == "BTC_USDC"
    assert listed.base == "BTC"
    assert listed.quote == "USDC"
    assert listed.settlement_asset is None


def test_v2_linear_perp_shares_the_spot_symbol() -> None:
    listed = to_listed(PERP_LINEAR, category=Category.PERP)
    assert listed is not None
    assert str(listed.ticker) == "Deribit_Perp_BTCUSDC"
    assert listed.exch_ticker == "BTC_USDC-PERPETUAL"
    assert listed.settlement_asset == "USDC"


def test_v3_inverse_and_dated_are_dropped() -> None:
    assert to_listed(PERP_INVERSE, category=Category.PERP) is None
    assert to_listed(DATED, category=Category.PERP) is None
    assert to_listed(PERP_LINEAR, category=Category.SPOT) is None
    assert to_listed(SPOT_NATIVE, category=Category.PERP) is None


def test_v12_cbe_is_listed_and_detected_by_presence() -> None:
    listed = to_listed(SPOT_CBE, category=Category.SPOT)
    assert listed is not None
    assert str(listed.ticker) == "Deribit_Spot_SOLUSDC"
    assert p.is_cbe_routed(SPOT_CBE)
    assert not p.is_cbe_routed(SPOT_NATIVE)
    assert "is_cbe_routed" not in SPOT_NATIVE
    assert "is_csr" not in SPOT_NATIVE


def test_kind_and_instrument_name_round_trip() -> None:
    spot = UniversalTicker.parse("Deribit_Spot_BTCUSDC")
    perp = UniversalTicker.parse("Deribit_Perp_BTCUSDC")
    assert p.kind_of(spot) == p.KIND_SPOT
    assert p.kind_of(perp) == p.KIND_FUTURE
    assert p.category_from_instrument("BTC_USDC") is Category.SPOT
    assert p.category_from_instrument("BTC_USDC-PERPETUAL") is Category.PERP
    assert p.is_linear_perp_name("BTC_USDC-PERPETUAL")
    assert not p.is_linear_perp_name("BTC-PERPETUAL")


def test_v9_balance_maps_available_funds_and_equity() -> None:
    row = DeribitSummary.model_validate(
        {
            "currency": "btc",
            "balance": "1",
            "equity": "1.2",
            "available_funds": "0.8",
        }
    )
    balance = row.to_balance()
    assert balance is not None
    assert balance.asset == "BTC"
    assert balance.free == balance.free.__class__("0.8")
    assert balance.locked == balance.locked.__class__("0.4")


def test_hosts_are_production_not_testnet() -> None:
    assert p.DERIBIT_REST_URL == "https://www.deribit.com/api/v2"
    assert p.DERIBIT_WS_URL == "wss://www.deribit.com/ws/api/v2"

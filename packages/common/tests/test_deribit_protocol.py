"""Deribit JSON-RPC framing, signing, listing filter (V1 / V2 / V3 / V12 / V13)."""

from __future__ import annotations

from decimal import Decimal

from mftik.exchange.deribit import protocol as p
from mftik.exchange.deribit.listing import to_listed
from mftik.exchange.deribit.models import DeribitSummary
from mftik.exchange.tickers import Category, UniversalTicker
from mftik.symbols.listed import MIN_QTY, PRICE_TICK, QTY_STEP

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

DATED_INVERSE = {
    "instrument_name": "BTC-6SEP26",
    "kind": "future",
    "instrument_type": "reversed",
    "future_type": "reversed",
    "settlement_period": "day",
    "base_currency": "BTC",
    "quote_currency": "USD",
    "settlement_currency": "BTC",
    "tick_size": "2.5",
    "min_trade_amount": "10",
    "contract_size": "10",
    "expiration_timestamp": 1_788_681_600_000,
    "is_active": True,
}

DATED_LINEAR = {
    "instrument_name": "BTC_USDC-6SEP26",
    "kind": "future",
    "instrument_type": "linear",
    "future_type": "linear",
    "settlement_period": "day",
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "settlement_currency": "USDC",
    "tick_size": "0.1",
    "min_trade_amount": "0.0001",
    "contract_size": "0.0001",
    "expiration_timestamp": 1_788_681_600_000,
    "is_active": True,
}

OPTION_INVERSE = {
    "instrument_name": "BTC-13SEP26-70000-C",
    "kind": "option",
    "instrument_type": "reversed",
    "option_type": "call",
    "strike": 70000,
    "base_currency": "BTC",
    "quote_currency": "BTC",
    "counter_currency": "USD",
    "settlement_currency": "BTC",
    "tick_size": "0.0001",
    "min_trade_amount": "0.1",
    "contract_size": "1",
    "expiration_timestamp": 1_789_286_400_000,
    "is_active": True,
}

OPTION_LINEAR = {
    "instrument_name": "BTC_USDC-13SEP26-70000-C",
    "kind": "option",
    "instrument_type": "linear",
    "option_type": "call",
    "strike": 70000,
    "base_currency": "BTC",
    "quote_currency": "USDC",
    "counter_currency": "USDC",
    "settlement_currency": "USDC",
    "tick_size": "5",
    "min_trade_amount": "0.01",
    "contract_size": "1",
    "expiration_timestamp": 1_789_286_400_000,
    "is_active": True,
}

OPTION_AVAX = {
    "instrument_name": "AVAX_USDC-13SEP26-6d4-C",
    "kind": "option",
    "instrument_type": "linear",
    "option_type": "call",
    "strike": 6.4,
    "base_currency": "AVAX",
    "quote_currency": "USDC",
    "counter_currency": "USDC",
    "settlement_currency": "USDC",
    "tick_size": "0.0005",
    "min_trade_amount": "100",
    "contract_size": "100",
    "expiration_timestamp": 1_789_286_400_000,
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


def test_v3_each_future_book_keeps_only_its_own_rows() -> None:
    assert to_listed(PERP_INVERSE, category=Category.PERP) is None
    assert to_listed(DATED_INVERSE, category=Category.PERP) is None
    assert to_listed(DATED_LINEAR, category=Category.PERP) is None
    assert to_listed(PERP_LINEAR, category=Category.SPOT) is None
    assert to_listed(SPOT_NATIVE, category=Category.PERP) is None
    assert to_listed(PERP_LINEAR, category=Category.INVERSE) is None
    assert to_listed(DATED_INVERSE, category=Category.INVERSE) is None
    assert to_listed(PERP_INVERSE, category=Category.FUTURE) is None
    assert to_listed(OPTION_INVERSE, category=Category.SPOT) is None
    assert to_listed(OPTION_INVERSE, category=Category.PERP) is None
    assert to_listed(OPTION_INVERSE, category=Category.INVERSE) is None
    assert to_listed(OPTION_INVERSE, category=Category.FUTURE) is None
    assert to_listed(PERP_LINEAR, category=Category.OPTION) is None
    assert to_listed(DATED_INVERSE, category=Category.OPTION) is None


def test_v3_inverse_perp_identity_is_btc_usd() -> None:
    listed = to_listed(PERP_INVERSE, category=Category.INVERSE)
    assert listed is not None
    assert str(listed.ticker) == "Deribit_Inverse_BTCUSD"
    assert listed.exch_ticker == "BTC-PERPETUAL"
    assert listed.settlement_asset == "BTC"
    assert listed.expiry_code is None


def test_v3_dated_identity_hyphenates_yymmdd() -> None:
    inverse = to_listed(DATED_INVERSE, category=Category.FUTURE)
    linear = to_listed(DATED_LINEAR, category=Category.FUTURE)
    assert inverse is not None
    assert linear is not None
    assert str(inverse.ticker) == "Deribit_Future_BTCUSD-260906"
    assert inverse.exch_ticker == "BTC-6SEP26"
    assert inverse.settlement_asset == "BTC"
    assert inverse.expiry_code == "260906"
    assert inverse.expiry is not None
    assert inverse.expiry.year == 2026
    assert inverse.expiry.month == 9
    assert inverse.expiry.day == 6
    assert str(linear.ticker) == "Deribit_Future_BTCUSDC-260906"
    assert linear.exch_ticker == "BTC_USDC-6SEP26"
    assert linear.settlement_asset == "USDC"


def test_v13_option_identity_uses_counter_currency() -> None:
    inverse = to_listed(OPTION_INVERSE, category=Category.OPTION)
    linear = to_listed(OPTION_LINEAR, category=Category.OPTION)
    avax = to_listed(OPTION_AVAX, category=Category.OPTION)
    assert inverse is not None
    assert linear is not None
    assert avax is not None
    assert str(inverse.ticker) == "Deribit_Option_BTCUSD-260913-70000-C"
    assert inverse.exch_ticker == "BTC-13SEP26-70000-C"
    assert inverse.quote == "USD"
    assert inverse.settlement_asset == "BTC"
    assert inverse.expiry_code == "260913"
    assert inverse.strike == Decimal("70000")
    assert inverse.option_type == "C"
    assert inverse.filters[PRICE_TICK] == Decimal("0.0001")
    assert inverse.filters[QTY_STEP] == Decimal("0.1")
    assert inverse.filters[MIN_QTY] == Decimal("0.1")
    assert str(linear.ticker) == "Deribit_Option_BTCUSDC-260913-70000-C"
    assert linear.exch_ticker == "BTC_USDC-13SEP26-70000-C"
    assert linear.quote == "USDC"
    assert linear.settlement_asset == "USDC"
    assert str(avax.ticker) == "Deribit_Option_AVAXUSDC-260913-6D4-C"
    assert avax.exch_ticker == "AVAX_USDC-13SEP26-6d4-C"
    assert avax.strike == Decimal("6.4")
    assert avax.filters[QTY_STEP] == Decimal("100")
    assert avax.filters[MIN_QTY] == Decimal("100")


def test_v13_option_skips_a_row_missing_identity() -> None:
    missing_strike = dict(OPTION_INVERSE)
    missing_strike["strike"] = None
    missing_flag = dict(OPTION_INVERSE)
    missing_flag["option_type"] = ""
    missing_counter = dict(OPTION_INVERSE)
    missing_counter["counter_currency"] = ""
    combo = dict(OPTION_INVERSE)
    combo["kind"] = "option_combo"
    assert to_listed(missing_strike, category=Category.OPTION) is None
    assert to_listed(missing_flag, category=Category.OPTION) is None
    assert to_listed(missing_counter, category=Category.OPTION) is None
    assert to_listed(combo, category=Category.OPTION) is None


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
    inverse = UniversalTicker.parse("Deribit_Inverse_BTCUSD")
    dated = UniversalTicker.parse("Deribit_Future_BTCUSD-260906")
    option = UniversalTicker.parse("Deribit_Option_BTCUSD-260913-70000-C")
    assert p.kind_of(spot) == p.KIND_SPOT
    assert p.kind_of(perp) == p.KIND_FUTURE
    assert p.kind_of(inverse) == p.KIND_FUTURE
    assert p.kind_of(dated) == p.KIND_FUTURE
    assert p.kind_of(option) == p.KIND_OPTION
    assert p.kind_of(Category.OPTION) == p.KIND_OPTION
    assert p.category_of("option") is Category.OPTION
    assert p.category_from_instrument("BTC_USDC") is Category.SPOT
    assert p.category_from_instrument("BTC_USDC-PERPETUAL") is Category.PERP
    assert p.category_from_instrument("BTC-PERPETUAL") is Category.INVERSE
    assert p.category_from_instrument("BTC-6SEP26") is Category.FUTURE
    assert p.category_from_instrument("BTC_USDC-6SEP26") is Category.FUTURE
    assert p.category_from_instrument("BTC-6SEP26-100000-C") is Category.OPTION
    assert p.category_from_instrument("BTC-13SEP26-70000-C") is Category.OPTION
    assert p.is_option_name("BTC-13SEP26-70000-C")
    assert p.is_option_name("BTC_USDC-13SEP26-70000-P")
    assert not p.is_option_name("BTC-6SEP26")
    assert p.expiry_code_from_option_name("BTC-13SEP26-70000-C") == "260913"
    assert p.expiry_code_from_name("BTC-13SEP26-70000-C") is None
    assert Category.OPTION not in p.TRADED_CATEGORIES
    assert p.is_linear_perp_name("BTC_USDC-PERPETUAL")
    assert not p.is_linear_perp_name("BTC-PERPETUAL")
    assert p.is_inverse_perp_name("BTC-PERPETUAL")
    assert p.expiry_code_from_name("BTC-6SEP26") == "260906"
    assert p.expiry_suffix_from_code("260906") == "6SEP26"
    # A day the calendar does not have is skipped, not turned into a
    # code that raises out of expiry_from_code and fails the refresh.
    assert p.expiry_code_from_name("BTC-31FEB26") is None
    assert p.expiry_code_from_name("BTC-31APR26") is None
    assert p.expiry_code_from_name("BTC-29FEB27") is None
    assert p.expiry_code_from_name("BTC-29FEB28") == "280229"


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


def test_raise_for_error_uses_the_requested_method_when_the_reply_omits_it() -> None:
    """Deribit error replies have no ``method``; the caller still knows it."""
    resp = p.DeribitResponse(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "error": {"code": -32602, "message": "Invalid params"},
        }
    )
    assert resp.method == ""
    try:
        resp.raise_for_error(op="public/auth")
    except p.DeribitWsError as exc:
        assert exc.op == "public/auth"
        assert exc.code == -32602
        assert exc.msg == "Invalid params"
        assert str(exc) == "public/auth: [-32602] Invalid params"
    else:
        raise AssertionError("expected DeribitWsError")


def test_raise_for_error_without_op_stays_bare_when_the_reply_has_no_method() -> None:
    resp = p.DeribitResponse(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "error": {"code": -32602, "message": "Invalid params"},
        }
    )
    try:
        resp.raise_for_error()
    except p.DeribitWsError as exc:
        assert exc.op == ""
        assert str(exc) == "[-32602] Invalid params"
    else:
        raise AssertionError("expected DeribitWsError")

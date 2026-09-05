"""Bitget UTA v3 framing, signing, and product_of (I1 / I3 / V1 / V10)."""

from __future__ import annotations

import base64
import hashlib
import hmac
from decimal import Decimal

import pytest
from mftik.exchange.bitget import protocol as p
from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import Category, UniversalTicker


def test_i3_product_of_takes_a_ticker_not_a_category_alone() -> None:
    spot = UniversalTicker.parse("Bitget_Spot_BTCUSDT")
    usdt = UniversalTicker.parse("Bitget_Perp_BTCUSDT")
    usdc = UniversalTicker.parse("Bitget_Perp_BTCUSDC")
    assert p.product_of(spot) == "SPOT"
    assert p.product_of(usdt) == "USDT-FUTURES"
    assert p.product_of(usdc) == "USDC-FUTURES"
    assert p.category_of("USDC-FUTURES") is Category.PERP
    assert p.category_of("USDT-FUTURES") is Category.PERP
    assert p.category_of("SPOT") is Category.SPOT
    assert p.inst_type_of("USDC-FUTURES") == "usdc-futures"
    assert p.inst_type_of("USDT-FUTURES") == "usdt-futures"
    assert p.inst_type_of("SPOT") == "spot"


def test_a_perp_that_is_neither_usdt_nor_usdc_is_refused() -> None:
    with pytest.raises(ExchangeError, match="linear settle"):
        p.product_of(UniversalTicker.parse("Bitget_Perp_BTCUSD"))


def test_an_unknown_product_is_refused() -> None:
    with pytest.raises(ExchangeError):
        p.product_of("COIN-FUTURES")
    with pytest.raises(ExchangeError):
        p.product_of(Category.INVERSE)


def test_rest_sign_is_base64_hmac_over_the_prehash() -> None:
    """Published GET fee-rate vector: timestamp + METHOD + path + ?query."""
    timestamp = "1627366780545"
    path = "/api/v3/account/fee-rate?category=SPOT&symbol=BTCUSDT"
    expected = base64.b64encode(
        hmac.new(
            b"secret",
            f"{timestamp}GET{path}".encode(),
            hashlib.sha256,
        ).digest()
    ).decode("ascii")
    assert (
        p.sign_rest(
            "secret",
            timestamp=timestamp,
            method="GET",
            request_path=path,
        )
        == expected
    )


def test_post_sign_covers_the_body() -> None:
    """Published POST place-order vector: timestamp + METHOD + path + body."""
    timestamp = "1627366780545"
    path = "/api/v3/trade/place-order"
    body = '{"category":"SPOT","symbol":"BTCUSDT","qty":"0.001","side":"buy"}'
    expected = base64.b64encode(
        hmac.new(
            b"secret",
            f"{timestamp}POST{path}{body}".encode(),
            hashlib.sha256,
        ).digest()
    ).decode("ascii")
    assert (
        p.sign_rest(
            "secret",
            timestamp=timestamp,
            method="POST",
            request_path=path,
            body=body,
        )
        == expected
    )


def test_v10_query_keys_are_sorted_regardless_of_insertion_order() -> None:
    first = p.query_string({"symbol": "BTCUSDT", "category": "SPOT"})
    second = p.query_string({"category": "SPOT", "symbol": "BTCUSDT"})
    assert first == second == "category=SPOT&symbol=BTCUSDT"


def test_v1_ws_login_signs_unix_seconds_and_never_milliseconds() -> None:
    """Docs say milliseconds; the published example and Java sample use seconds."""
    seconds = "1538054050"
    expected = base64.b64encode(
        hmac.new(
            b"secret", b"1538054050GET/user/verify", hashlib.sha256
        ).digest()
    ).decode("ascii")
    assert p.sign_ws("secret", seconds) == expected
    frame, _ = p.login_frame(
        api_key="key",
        api_secret="secret",
        passphrase="phrase",
        timestamp=seconds,
    )
    assert frame["args"][0]["timestamp"] == seconds
    assert len(frame["args"][0]["timestamp"]) == 10
    # Milliseconds would be 13 digits. We do not send that unit.
    millis = "1538054050000"
    other = p.sign_ws("secret", millis)
    assert other != expected
    assert len(millis) == 13


def test_ping_is_the_literal_string_not_json() -> None:
    assert p.PING == "ping"
    assert p.PING != '{"op":"ping"}'


def test_rest_headers_sign_what_they_say_they_signed() -> None:
    headers = p.rest_headers(
        api_key="key",
        api_secret="secret",
        passphrase="phrase",
        method="POST",
        request_path="/api/v3/trade/place-order",
        body='{"category":"SPOT"}',
        timestamp="1627366780545",
    )
    assert headers["ACCESS-KEY"] == "key"
    assert headers["ACCESS-PASSPHRASE"] == "phrase"
    assert headers["ACCESS-SIGN"] == p.sign_rest(
        "secret",
        timestamp="1627366780545",
        method="POST",
        request_path="/api/v3/trade/place-order",
        body='{"category":"SPOT"}',
    )


def test_json_body_drops_none_and_stringifies_decimals() -> None:
    assert p.json_body({"qty": Decimal("0.10"), "posSide": None}) == '{"qty":"0.1"}'


def test_hosts_are_production_not_demo() -> None:
    assert p.BITGET_REST_URL == "https://api.bitget.com"
    assert p.public_url().startswith("wss://ws.bitget.com/v3/ws/public")
    assert p.private_url().endswith("/v3/ws/private")
    assert "wspap" not in p.public_url()

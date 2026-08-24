"""OKX v5 framing, signing, and the response envelope."""

from __future__ import annotations

import base64
import hashlib
import hmac
from decimal import Decimal

import pytest
from mftik.exchange.okx import protocol as p
from mftik.exchange.tickers import Category


def test_our_categories_map_onto_okx_books() -> None:
    assert p.product_of(Category.SPOT) == "SPOT"
    assert p.product_of(Category.PERP) == "SWAP"
    assert p.product_of("SWAP") == "SWAP"


def test_an_unknown_product_is_refused() -> None:
    with pytest.raises(Exception):
        p.product_of("not-a-book")


def test_demo_urls_are_a_different_host() -> None:
    assert p.public_url().startswith("wss://ws.okx.com")
    assert p.public_url(demo=True).startswith("wss://wspap.okx.com")
    assert p.private_url().endswith("/ws/v5/private")
    assert p.business_url().endswith("/ws/v5/business")


def test_rest_sign_is_base64_hmac_over_the_prehash() -> None:
    """``timestamp + METHOD + path + body`` — the exact bytes OKX re-reads."""
    expected = base64.b64encode(
        hmac.new(
            b"secret",
            b"2020-12-08T09:08:57.715ZGET/api/v5/account/balance?ccy=BTC",
            hashlib.sha256,
        ).digest()
    ).decode("ascii")
    assert (
        p.sign_rest(
            "secret",
            timestamp="2020-12-08T09:08:57.715Z",
            method="GET",
            request_path="/api/v5/account/balance?ccy=BTC",
        )
        == expected
    )


def test_ws_sign_covers_the_login_path_and_unix_seconds() -> None:
    expected = base64.b64encode(
        hmac.new(
            b"secret", b"1538054050GET/users/self/verify", hashlib.sha256
        ).digest()
    ).decode("ascii")
    assert p.sign_ws("secret", "1538054050") == expected


def test_rest_headers_sign_what_they_say_they_signed() -> None:
    headers = p.rest_headers(
        api_key="key",
        api_secret="secret",
        passphrase="phrase",
        method="POST",
        request_path="/api/v5/trade/order",
        body='{"instId":"BTC-USDT"}',
        timestamp="2020-12-08T09:08:57.715Z",
    )
    assert headers["OK-ACCESS-KEY"] == "key"
    assert headers["OK-ACCESS-PASSPHRASE"] == "phrase"
    assert headers["OK-ACCESS-SIGN"] == p.sign_rest(
        "secret",
        timestamp="2020-12-08T09:08:57.715Z",
        method="POST",
        request_path="/api/v5/trade/order",
        body='{"instId":"BTC-USDT"}',
    )


def test_demo_trading_is_a_header_not_a_host() -> None:
    headers = p.rest_headers(
        api_key="k",
        api_secret="s",
        passphrase="p",
        method="GET",
        request_path="/api/v5/account/balance",
        demo=True,
    )
    assert headers[p.DEMO_HEADER] == "1"


def test_login_frame_carries_key_passphrase_timestamp_and_sign() -> None:
    frame, req_id = p.login_frame(
        api_key="key",
        api_secret="secret",
        passphrase="phrase",
        timestamp="1538054050",
    )
    assert frame["op"] == "login"
    assert frame["id"] == req_id
    arg = frame["args"][0]
    assert arg["apiKey"] == "key"
    assert arg["passphrase"] == "phrase"
    assert arg["timestamp"] == "1538054050"
    assert arg["sign"] == p.sign_ws("secret", "1538054050")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("0.00780000"), "0.0078"),
        (Decimal("1E-8"), "0.00000001"),
        (Decimal("10"), "10"),
        (Decimal("1E+1"), "10"),
    ],
)
def test_decimals_are_written_positionally_and_unpadded(
    value: Decimal, expected: str
) -> None:
    assert p.decimal_text(value) == expected


def test_a_push_is_a_frame_with_arg_and_data_and_no_event() -> None:
    push = p.OkxResponse(
        {
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{"last": "60000"}],
        }
    )
    assert push.is_push
    assert not push.is_reply
    assert push.channel == "tickers"
    assert push.inst_id == "BTC-USDT"
    assert push.rows()[0]["last"] == "60000"


def test_a_subscribe_reply_is_correlated_by_id() -> None:
    reply = p.OkxResponse(
        {
            "id": "req-1",
            "event": "subscribe",
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "code": "0",
        }
    )
    assert reply.is_reply
    assert reply.success
    assert reply.req_id == "req-1"


def test_an_error_event_is_a_failed_reply() -> None:
    reply = p.OkxResponse(
        {"event": "error", "code": "60012", "msg": "Invalid request", "id": "r"}
    )
    assert reply.is_reply
    assert not reply.success
    assert reply.code == 60012
    assert reply.error is not None


def test_a_pong_is_the_heartbeat_coming_back() -> None:
    assert p.OkxResponse({"event": "pong"}).is_pong

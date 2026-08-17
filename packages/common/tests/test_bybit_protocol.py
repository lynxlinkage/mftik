"""Bybit v5 framing, signing, and the response envelope."""

from __future__ import annotations

import hashlib
import hmac
import json
from decimal import Decimal

import pytest
from mftik.exchange.bybit import protocol as p
from mftik.exchange.errors import ExchangeError
from mftik.exchange.tickers import Category

# --- endpoints and categories ----------------------------------------------


def test_public_url_names_the_category_and_nothing_else_does() -> None:
    """The one place a category reaches a URL on this venue."""
    assert p.public_url("spot") == "wss://stream.bybit.com/v5/public/spot"
    assert p.public_url("linear") == "wss://stream.bybit.com/v5/public/linear"
    assert p.public_url("inverse") == "wss://stream.bybit.com/v5/public/inverse"
    assert p.public_url("spot", testnet=True).startswith(
        "wss://stream-testnet.bybit.com/"
    )
    # The private and trade sockets carry every category, so neither is
    # parameterised.
    assert p.BYBIT_WS_PRIVATE_URL.endswith("/v5/private")
    assert p.BYBIT_WS_TRADE_URL.endswith("/v5/trade")


def test_an_unknown_product_is_refused_before_a_connection() -> None:
    with pytest.raises(ExchangeError, match="unknown Bybit product"):
        p.public_url("futures")


def test_our_categories_map_onto_bybit_books() -> None:
    assert p.product_of(Category.SPOT) == "spot"
    # Perp means the USDT-margined book; the inverse one is a different symbol
    # and stays reachable by passing the product directly.
    assert p.product_of(Category.PERP) == "linear"
    assert p.product_of("inverse") == "inverse"


# --- signing ---------------------------------------------------------------


def test_ws_auth_signs_the_deadline_and_nothing_else() -> None:
    """``GET/realtime<expires>`` is the whole payload; the key is not in it."""
    expected = hmac.new(
        b"secret", b"GET/realtime1700000000000", hashlib.sha256
    ).hexdigest()
    assert p.sign_ws("secret", 1700000000000) == expected


def test_auth_frame_carries_key_deadline_signature_in_that_order() -> None:
    frame, req_id = p.auth_frame(
        api_key="key", api_secret="secret", expires=1700000000000
    )
    assert frame["op"] == "auth"
    assert frame["req_id"] == req_id
    assert frame["args"] == [
        "key",
        1700000000000,
        p.sign_ws("secret", 1700000000000),
    ]


def test_auth_deadline_is_in_the_future_by_the_window() -> None:
    """It is a deadline, not a timestamp: a past one is refused by Bybit."""
    frame, _ = p.auth_frame(api_key="k", api_secret="s", window_ms=5_000)
    expires = frame["args"][1]
    now = p.now_ms()
    assert now < expires <= now + 5_000


def test_rest_sign_concatenates_timestamp_key_window_payload() -> None:
    expected = hmac.new(
        b"secret", b"1700000000000key5000category=spot", hashlib.sha256
    ).hexdigest()
    assert (
        p.sign_rest(
            "secret",
            api_key="key",
            timestamp=1700000000000,
            recv_window=5000,
            payload="category=spot",
        )
        == expected
    )


def test_rest_headers_sign_what_they_say_they_signed() -> None:
    headers = p.rest_headers(
        api_key="key", api_secret="secret", payload="category=spot"
    )
    assert headers["X-BAPI-API-KEY"] == "key"
    assert headers["X-BAPI-SIGN"] == p.sign_rest(
        "secret",
        api_key="key",
        timestamp=int(headers["X-BAPI-TIMESTAMP"]),
        recv_window=int(headers["X-BAPI-RECV-WINDOW"]),
        payload="category=spot",
    )


# --- wire values -----------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("0.00780000"), "0.0078"),
        (Decimal("1E-8"), "0.00000001"),
        (Decimal("10"), "10"),
        (Decimal("1E+1"), "10"),
        (Decimal("60000.50"), "60000.5"),
    ],
)
def test_decimals_are_written_positionally_and_unpadded(
    value: Decimal, expected: str
) -> None:
    """Bybit checks written precision against the instrument's step.

    ``0.00780000`` is refused where ``0.0078`` is taken, though they are the
    same quantity — a Decimal's scale propagates through arithmetic, so a size
    floored against a ``qtyStep`` of ``0.00010000`` carries eight decimals
    whatever the number is.
    """
    assert p.decimal_text(value) == expected


def test_only_the_money_fields_are_rewritten_as_strings() -> None:
    """Bybit's prices and sizes are strings; its flags and enums are not.

    ``positionIdx`` and ``isLeverage`` are integers and ``reduceOnly`` is a
    boolean, so a blanket "stringify every number" would turn a hedge-mode or
    margin order into a ``10001``.
    """
    assert p.wire(Decimal("0.001")) == "0.001"
    assert p.wire(1) == 1
    assert p.wire(True) is True
    # An optional argument left unset should not arrive as a null.
    assert p.wire({"a": Decimal("1.50"), "b": None}) == {"a": "1.5"}


def test_query_string_keeps_insertion_order_and_drops_unset() -> None:
    """The signature covers this literal string, so the order is load-bearing."""
    assert (
        p.query_string({"category": "spot", "symbol": "BTCUSDT", "cursor": None})
        == "category=spot&symbol=BTCUSDT"
    )
    assert p.query_string(None) == ""


def test_json_body_is_serialized_once_so_the_signature_matches() -> None:
    body = p.json_body({"category": "spot", "qty": Decimal("0.100")})
    assert body == '{"category":"spot","qty":"0.1"}'
    assert json.loads(body)["qty"] == "0.1"


# --- frames ----------------------------------------------------------------


def test_trade_frame_carries_a_header_but_no_credential() -> None:
    """The connection's auth replaces the key; the clock it does not."""
    frame, req_id = p.trade_frame(
        "order.create", [{"symbol": "BTCUSDT", "qty": Decimal("0.001")}]
    )
    assert frame["reqId"] == req_id
    assert frame["op"] == "order.create"
    assert frame["args"] == [{"symbol": "BTCUSDT", "qty": "0.001"}]
    assert set(frame["header"]) == {"X-BAPI-TIMESTAMP", "X-BAPI-RECV-WINDOW"}
    assert "X-BAPI-API-KEY" not in frame["header"]
    assert "X-BAPI-SIGN" not in frame["header"]


def test_subscribe_and_ping_frames_are_the_shared_envelope() -> None:
    frame, req_id = p.subscribe_frame(["order", "execution"])
    assert frame == {
        "req_id": req_id,
        "op": "subscribe",
        "args": ["order", "execution"],
    }
    frame, req_id = p.subscribe_frame(["order"], op=p.UNSUBSCRIBE)
    assert frame["op"] == "unsubscribe"
    frame, req_id = p.ping_frame()
    assert frame == {"req_id": req_id, "op": "ping"}


# --- responses -------------------------------------------------------------


def test_a_frame_is_a_push_if_and_only_if_it_has_a_topic() -> None:
    push = p.BybitResponse(
        {"topic": "order", "type": "snapshot", "data": [{"orderId": "1"}]}
    )
    assert push.is_push and not push.is_reply
    assert push.rows() == [{"orderId": "1"}]

    reply = p.BybitResponse(
        {"op": "subscribe", "req_id": "r1", "success": True, "ret_msg": ""}
    )
    assert reply.is_reply and not reply.is_push
    assert reply.req_id == "r1"


def test_the_trade_socket_spells_the_same_things_differently() -> None:
    """``reqId``/``retCode`` there, ``req_id``/``success`` on the streams."""
    resp = p.BybitResponse(
        {
            "reqId": "r1",
            "retCode": 0,
            "retMsg": "OK",
            "op": "order.create",
            "data": {"orderId": "ord-1"},
        }
    )
    assert resp.req_id == "r1"
    assert resp.success is True
    assert resp.error is None
    assert resp.rows() == [{"orderId": "ord-1"}]


def test_a_refusal_becomes_an_error_carrying_the_code() -> None:
    """TD normalizes on the code, so it must not have to parse ``str(exc)``."""
    resp = p.BybitResponse(
        {
            "reqId": "r1",
            "retCode": 110007,
            "retMsg": "ab not enough for new order",
            "op": "order.create",
        }
    )
    assert resp.success is False
    error = resp.error
    assert isinstance(error, p.BybitWsError)
    assert error.code == 110007
    assert "110007" in str(error)
    with pytest.raises(p.BybitWsError):
        resp.raise_for_error()


def test_not_found_is_a_property_of_the_code_not_the_message() -> None:
    """Bybit answers a different code per book for the same fact."""
    assert p.BybitError(110001, "order not exists").not_found
    assert p.BybitError(170213, "Order does not exist").not_found
    assert not p.BybitError(110007, "insufficient balance").not_found


def test_both_spellings_of_pong_are_recognised() -> None:
    """The public socket echoes ``op: ping``; the private answers ``op: pong``.

    Only one of them carries the ``req_id`` we sent, which is why the heartbeat
    does not wait for a correlated reply — and why the read loop has to know a
    pong when it sees one rather than logging an unmatched reply every 20s.
    """
    public = p.BybitResponse(
        {"op": "ping", "req_id": "r1", "success": True, "ret_msg": "pong"}
    )
    private = p.BybitResponse(
        {"op": "pong", "args": ["1700000000000"], "conn_id": "c1"}
    )
    assert public.is_pong and private.is_pong
    assert not p.BybitResponse({"op": "subscribe", "success": True}).is_pong


def test_an_empty_req_id_is_not_an_id() -> None:
    """Bybit echoes ``""`` when we sent none; correlating on that would match
    every uncorrelated frame to one waiting request."""
    assert p.BybitResponse({"op": "ping", "req_id": ""}).req_id is None

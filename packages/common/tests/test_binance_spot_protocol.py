"""Binance spot framing and Ed25519 signing.

The signing rules have no partial credit — a payload built in the wrong order,
or a value rendered the way Python prints it rather than the way it goes on the
wire, produces a signature the venue rejects with no clue as to why. So these
tests check the rules directly and then check that a real Ed25519 verifier
accepts what they produce.
"""

from __future__ import annotations

import base64
from decimal import Decimal

import pytest
from binance_stub import keypair
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from mftik.exchange.binance.spot import methods as m
from mftik.exchange.binance.spot.protocol import (
    BinanceAuthError,
    BinanceResponse,
    BinanceWsError,
    decimal_text,
    load_private_key,
    logon_frame,
    payload_for,
    render,
    request_frame,
    sign,
    signed_frame,
    subscribe_frame,
    wire,
)


def test_payload_sorts_by_name_and_drops_the_signature() -> None:
    payload = payload_for(
        {
            "symbol": "BTCUSDT",
            "apiKey": "abc",
            "timestamp": 1645423376532,
            "signature": "should-not-appear",
        }
    )
    assert payload == "apiKey=abc&symbol=BTCUSDT&timestamp=1645423376532"


def test_payload_matches_binances_documented_example() -> None:
    """The exact string from Binance's SIGNED-request docs, rebuilt."""
    payload = payload_for(
        {
            "symbol": "BTCUSDT",
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": "0.01000000",
            "price": "52000.00",
            "recvWindow": 100,
            "timestamp": 1645423376532,
            "apiKey": "value",
        }
    )
    assert payload == (
        "apiKey=value&price=52000.00&quantity=0.01000000&recvWindow=100"
        "&side=SELL&symbol=BTCUSDT&timeInForce=GTC&timestamp=1645423376532"
        "&type=LIMIT"
    )


def test_small_decimals_never_reach_the_wire_in_exponent_form() -> None:
    """``1E-8`` is a real tick size and not a price Binance accepts."""
    assert render(Decimal("1E-8")) == "0.00000001"
    assert wire(Decimal("1E-8")) == "0.00000001"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # The live failure: a size floored against a lot step written
        # "0.00010000" keeps that scale, and Binance answers -1111 because the
        # written precision exceeds what the step allows.
        (Decimal("0.00780000"), "0.0078"),
        (Decimal("1.000"), "1"),
        (Decimal("10.00"), "10"),
        (Decimal("0.0078"), "0.0078"),
        (Decimal("1E-8"), "0.00000001"),
        (Decimal("1918.67"), "1918.67"),
        (Decimal("0"), "0"),
    ],
)
def test_a_quantity_is_written_at_its_own_precision_not_its_scale(
    value: Decimal, expected: str
) -> None:
    """Trailing zeros are a Decimal's scale, and the scale propagates."""
    assert decimal_text(value) == expected
    assert wire(value) == expected
    assert render(value) == expected


def test_a_whole_number_never_goes_out_exponential() -> None:
    """``normalize`` alone turns 10 into 1E+1, the other unparseable spelling."""
    assert decimal_text(Decimal("100.00")) == "100"
    assert decimal_text(Decimal("1E+2")) == "100"


def test_render_and_wire_agree_on_every_value_kind() -> None:
    """What is signed has to describe what is sent, type by type.

    The server rebuilds the signed string from the frame it received, so a
    rendering the two disagree on verifies against something we never sent.
    """
    for value in (True, False, Decimal("0.5"), 7, "x", ["A", "B"]):
        assert render(wire(value)) == render(value)


def test_booleans_render_json_style_not_python_style() -> None:
    assert render(True) == "true"
    assert payload_for({"omitZeroBalances": True}) == "omitZeroBalances=true"


def test_arrays_stay_arrays_on_the_wire_and_json_in_the_signature() -> None:
    assert wire(["BTCUSDT", "ETHUSDT"]) == ["BTCUSDT", "ETHUSDT"]
    assert payload_for({"symbols": ["BTCUSDT", "ETHUSDT"]}) == (
        'symbols=["BTCUSDT","ETHUSDT"]'
    )


def test_signature_verifies_against_a_real_ed25519_public_key() -> None:
    key, _pem = keypair()
    params = {"apiKey": "k", "timestamp": 1700000000000}
    signature = sign(key, params)
    key.public_key().verify(
        base64.b64decode(signature), payload_for(params).encode("utf-8")
    )


def test_logon_frame_signs_only_the_params_it_sends() -> None:
    key, _pem = keypair()
    frame, req_id = logon_frame(api_key="k", private_key=key, ts=1700000000000)

    assert frame["method"] == m.SESSION_LOGON
    assert frame["id"] == req_id
    params = frame["params"]
    assert set(params) == {"apiKey", "timestamp", "signature"}
    key.public_key().verify(
        base64.b64decode(params["signature"]),
        payload_for(params).encode("utf-8"),
    )


def test_signed_frame_carries_key_timestamp_and_signature() -> None:
    key, _pem = keypair()
    frame, _ = signed_frame(
        m.ORDER_PLACE,
        {"symbol": "BTCUSDT", "quantity": Decimal("0.001")},
        api_key="k",
        private_key=key,
        recv_window=5000,
    )
    params = frame["params"]
    assert params["quantity"] == "0.001"
    assert params["recvWindow"] == 5000
    assert params["apiKey"] == "k"
    key.public_key().verify(
        base64.b64decode(params["signature"]),
        payload_for(params).encode("utf-8"),
    )


def test_request_frame_drops_unset_params_entirely() -> None:
    """A null param is malformed to Binance; an unset one should not be there."""
    frame, _ = request_frame(m.DEPTH, {"symbol": "BTCUSDT", "limit": None})
    assert frame["params"] == {"symbol": "BTCUSDT"}

    bare, _ = request_frame(m.USER_DATA_STREAM_SUBSCRIBE, {"nothing": None})
    assert "params" not in bare


def test_subscribe_frame_takes_a_bare_list() -> None:
    frame, req_id = subscribe_frame("SUBSCRIBE", ["btcusdt@aggTrade"])
    assert frame == {
        "id": req_id,
        "method": "SUBSCRIBE",
        "params": ["btcusdt@aggTrade"],
    }


# --- key loading -----------------------------------------------------------


def test_loads_a_pkcs8_pem_key() -> None:
    key, pem = keypair()
    loaded = load_private_key(pem)
    assert loaded.private_bytes_raw() == key.private_bytes_raw()


def test_loads_a_pem_whose_newlines_survived_as_backslash_n() -> None:
    """What an env var or a JSON round trip does to a PEM."""
    _key, pem = keypair()
    mangled = pem.replace("\n", "\\n")
    assert load_private_key(mangled).private_bytes_raw() == (
        load_private_key(pem).private_bytes_raw()
    )


def test_loads_a_bare_base64_seed() -> None:
    key, _pem = keypair()
    seed = base64.b64encode(key.private_bytes_raw()).decode("ascii")
    assert load_private_key(seed).private_bytes_raw() == key.private_bytes_raw()


def test_loads_a_pem_body_with_the_armour_stripped() -> None:
    key, pem = keypair()
    body = "".join(
        line for line in pem.splitlines() if not line.startswith("-----")
    )
    assert load_private_key(body).private_bytes_raw() == key.private_bytes_raw()


def test_a_non_ed25519_key_is_refused_where_it_was_configured() -> None:
    from cryptography.hazmat.primitives.asymmetric import ec

    pem = (
        ec.generate_private_key(ec.SECP256R1())
        .private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        .decode("ascii")
    )
    with pytest.raises(BinanceAuthError, match="Ed25519"):
        load_private_key(pem)


def test_garbage_is_refused_rather_than_signed_with() -> None:
    with pytest.raises(BinanceAuthError):
        load_private_key("not a key at all !!")
    with pytest.raises(BinanceAuthError, match="required"):
        load_private_key("")


def test_a_der_pkcs8_key_is_accepted_too() -> None:
    key = Ed25519PrivateKey.generate()
    der = key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    encoded = base64.b64encode(der).decode("ascii")
    assert load_private_key(encoded).private_bytes_raw() == key.private_bytes_raw()


# --- responses -------------------------------------------------------------


def test_a_reply_is_told_from_a_push_by_its_id_alone() -> None:
    reply = BinanceResponse({"id": "abc", "status": 200, "result": {"ok": 1}})
    assert reply.is_reply and not reply.is_push
    assert reply.id == "abc"

    market = BinanceResponse({"stream": "btcusdt@trade", "data": {"p": "1"}})
    assert market.is_push and not market.is_reply

    user = BinanceResponse(
        {"subscriptionId": 0, "event": {"e": "executionReport", "s": "BTCUSDT"}}
    )
    assert user.is_push and not user.is_reply
    assert user.event_type == "executionReport"


def test_an_integer_id_still_correlates_as_a_string() -> None:
    """Binance echoes back whatever we sent; ids are compared as text."""
    assert BinanceResponse({"id": 7, "result": None}).id == "7"


def test_an_error_reply_raises_with_the_venues_own_code() -> None:
    resp = BinanceResponse(
        {
            "id": "x",
            "status": 400,
            "error": {"code": -2010, "msg": "Account has insufficient balance."},
        }
    )
    assert not resp.ok
    with pytest.raises(BinanceWsError) as exc:
        resp.raise_for_error()
    assert exc.value.code == -2010
    assert exc.value.status == 400
    assert "insufficient balance" in str(exc.value)


def test_rows_normalizes_object_and_array_results() -> None:
    assert BinanceResponse({"id": "1", "result": {"a": 1}}).rows() == [{"a": 1}]
    assert BinanceResponse({"id": "1", "result": [{"a": 1}]}).rows() == [{"a": 1}]
    assert BinanceResponse({"id": "1", "result": None}).rows() == []

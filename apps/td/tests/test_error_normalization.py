"""Venue errors → normalized reject codes.

The table itself is data, so what is worth testing is the resolution order and
the fallthrough: a label we know becomes a ``2xx``, and a label we do not know
survives as itself rather than collapsing into a catch-all.
"""

from __future__ import annotations

import pytest
from mft.exchange import venues
from mft.exchange.errors import (
    ExchangeError,
    ExchangeNotConnectedError,
    InstrumentNotFoundError,
    InsufficientBalanceError,
    OrderError,
)
from mft.exchange.gate.spot.private import GateSpotPrivateClient
from mft.exchange.gate.spot.protocol import GateApiError, GateWsError
from mft.exchange.gate.spot.rest import GateRestError
from mft.exchange.paper.private import PaperAuthError, PaperPrivateClient
from mft.exchange.paper.remote import PaperRemotePrivateClient
from mft.protocol.reject_codes import (
    RejectCode,
    describe,
    is_normalized,
    is_td_internal,
    is_venue,
)
from mft_td.errors import VENUES, normalize

GATE = "gate_spot"
PAPER = "paper"


# --- band 2: venue errors we recognise -------------------------------------


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        ("BALANCE_NOT_ENOUGH", RejectCode.VENUE_INSUFFICIENT_BALANCE),
        ("POC_FILL_IMMEDIATELY", RejectCode.VENUE_POST_ONLY_WOULD_CROSS),
        ("IP_FORBIDDEN", RejectCode.VENUE_IP_NOT_WHITELISTED),
        ("INVALID_KEY", RejectCode.VENUE_AUTH_FAILED),
        ("INVALID_SIGNATURE", RejectCode.VENUE_AUTH_FAILED),
        ("READ_ONLY", RejectCode.VENUE_PERMISSION_DENIED),
        ("TOO_MANY_REQUESTS", RejectCode.VENUE_RATE_LIMITED),
        ("ORDER_NOT_FOUND", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("ORDER_CLOSED", RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        ("INVALID_PARAM_VALUE", RejectCode.VENUE_INVALID_PARAM),
        ("AMOUNT_TOO_LITTLE", RejectCode.VENUE_BELOW_MINIMUM),
        ("INVALID_CURRENCY_PAIR", RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
    ],
)
def test_a_known_gate_label_becomes_a_venue_code(
    label: str, expected: RejectCode
) -> None:
    code = normalize(
        GateApiError(label, "no thanks", channel="spot.order_place"), venue=GATE
    )

    assert code is expected


def test_the_four_named_in_the_spec_all_land_in_the_venue_band() -> None:
    """Insufficient balance, post-only cross, IP whitelist, auth failure."""
    labels = (
        "BALANCE_NOT_ENOUGH",
        "POC_FILL_IMMEDIATELY",
        "IP_FORBIDDEN",
        "INVALID_KEY",
    )
    codes = [normalize(GateApiError(x, ""), venue=GATE) for x in labels]

    assert all(is_venue(c) and is_normalized(c) for c in codes)
    assert all(200 <= int(c) < 300 for c in codes)


def test_labels_are_matched_regardless_of_case() -> None:
    code = normalize(GateApiError("balance_not_enough", "x"), venue=GATE)

    assert code is RejectCode.VENUE_INSUFFICIENT_BALANCE


def test_a_rest_error_normalizes_off_the_same_table() -> None:
    """REST and the trading socket use one vocabulary, so one table serves."""
    code = normalize(
        GateRestError(400, "BALANCE_NOT_ENOUGH", "not enough USDT"), venue=GATE
    )

    assert code is RejectCode.VENUE_INSUFFICIENT_BALANCE


def test_a_known_numeric_code_normalizes_too() -> None:
    code = normalize(
        GateWsError(2, "invalid argument", channel="spot.login"), venue=GATE
    )

    assert code is RejectCode.VENUE_INVALID_PARAM


# --- band 3: the fallthrough ------------------------------------------------


def test_an_unmapped_label_comes_back_as_the_venue_spelled_it() -> None:
    """No mapping is not the same as no information."""
    code = normalize(
        GateApiError("SOMETHING_NEW", "we changed the API"), venue=GATE
    )

    assert code == "SOMETHING_NEW"
    assert not is_normalized(code)
    # Still recognisably the venue's problem, not TD's.
    assert is_venue(code)
    assert not is_td_internal(code)


def test_an_unmapped_numeric_code_stays_numeric() -> None:
    code = normalize(GateWsError(9999, "who knows"), venue=GATE)

    assert code == 9999
    assert not is_normalized(code)


def test_an_unknown_venue_falls_through_rather_than_guessing() -> None:
    """A venue with no table yet still reports the venue's own code."""
    code = normalize(
        GateApiError("BALANCE_NOT_ENOUGH", "broke"), venue="somewhere_else"
    )

    assert code == "BALANCE_NOT_ENOUGH"


def test_a_venue_error_with_nothing_to_key_on_is_a_plain_venue_reject() -> None:
    code = normalize(ExchangeError("the venue said no"), venue=GATE)

    assert code is RejectCode.VENUE_REJECTED


# --- typed errors and the paper venue ---------------------------------------


def test_a_disconnected_client_is_tds_problem_not_the_venues() -> None:
    """Nothing was sent, so this belongs in the 1xx band."""
    code = normalize(ExchangeNotConnectedError("not connected"), venue=GATE)

    assert code is RejectCode.TD_VENUE_NOT_CONNECTED
    assert is_td_internal(code)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (InsufficientBalanceError("no funds"), RejectCode.VENUE_INSUFFICIENT_BALANCE),
        (InstrumentNotFoundError("XYZ"), RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
        (PaperAuthError("invalid paper api_secret"), RejectCode.VENUE_AUTH_FAILED),
    ],
)
def test_typed_errors_normalize_without_a_label(
    exc: Exception, expected: RejectCode
) -> None:
    assert normalize(exc, venue=PAPER) is expected


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("duplicate client_order_id=cid-1", RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID),
        ("unknown order_id=7", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("order 7 is not open for account=a", RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        ("qty must be positive", RejectCode.VENUE_INVALID_PARAM),
    ],
)
def test_paper_normalizes_off_its_own_messages(
    message: str, expected: RejectCode
) -> None:
    """Paper raises no labels, and its messages are ours to keep stable."""
    assert normalize(OrderError(message), venue=PAPER) is expected


def test_an_unmatched_paper_message_is_still_a_venue_reject() -> None:
    code = normalize(OrderError("something else entirely"), venue=PAPER)

    assert code is RejectCode.VENUE_REJECTED


# --- the bands themselves ---------------------------------------------------


def test_the_bands_do_not_overlap() -> None:
    for code in RejectCode:
        if code is RejectCode.NONE:
            continue
        assert is_td_internal(code) != is_venue(code)


def test_no_code_is_defined_outside_its_band() -> None:
    for code in RejectCode:
        if code is RejectCode.NONE:
            continue
        assert 100 <= code < 300
        assert code.name.startswith("TD_") == is_td_internal(code)


def test_every_table_is_keyed_by_a_registered_venue() -> None:
    assert set(VENUES) <= set(venues.names())


def test_each_private_client_names_itself_after_its_venue() -> None:
    """TD looks the table up by ``private.name``, so the two have to agree.

    A client renamed without a matching key here would not break — it would
    quietly stop normalizing, which is worse.
    """
    for client in (
        GateSpotPrivateClient,
        PaperPrivateClient,
        PaperRemotePrivateClient,
    ):
        assert client.name in VENUES


def test_describe_names_a_normalized_code_and_echoes_a_native_one() -> None:
    assert describe(RejectCode.VENUE_INSUFFICIENT_BALANCE) == (
        "201 VENUE_INSUFFICIENT_BALANCE"
    )
    assert describe("SOMETHING_NEW") == "SOMETHING_NEW"
    assert describe(9999) == "9999"


def test_none_is_not_a_refusal() -> None:
    assert not is_venue(RejectCode.NONE)
    assert not is_td_internal(RejectCode.NONE)
    assert not is_venue("")

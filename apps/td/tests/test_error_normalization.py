"""Venue errors → normalized reject codes.

The table itself is data, so what is worth testing is the resolution order and
the fallthrough: a label we know becomes a ``2xx``, and a label we do not know
survives as itself rather than collapsing into a catch-all.
"""

from __future__ import annotations

import pytest
from mftik.exchange import venues
from mftik.exchange.binance.spot.private import BinanceSpotPrivateClient
from mftik.exchange.binance.spot.protocol import BinanceWsError
from mftik.exchange.bybit.protocol import BybitRestError, BybitWsError
from mftik.exchange.errors import (
    ExchangeError,
    ExchangeNotConnectedError,
    InstrumentNotFoundError,
    InsufficientBalanceError,
    OrderError,
)
from mftik.exchange.gate.future.private import GateFuturesPrivateClient
from mftik.exchange.gate.spot.private import GateSpotPrivateClient
from mftik.exchange.gate.spot.protocol import GateApiError, GateWsError
from mftik.exchange.gate.spot.rest import GateRestError
from mftik.exchange.okx.protocol import OkxRestError, OkxWsError
from mftik.exchange.paper.private import PaperAuthError, PaperPrivateClient
from mftik.exchange.paper.remote import PaperRemotePrivateClient
from mftik.protocol.reject_codes import (
    RejectCode,
    describe,
    is_normalized,
    is_td_internal,
    is_venue,
)
from mftik_td.errors import VENUES, normalize, normalize_reason

GATE = "Gate"
GATE_FUTURES = "GateFutures"
PAPER = "Paper"
BINANCE = "Binance"
BINANCE_FUTURE = "BinanceFuture"


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


def test_gate_futures_reuses_the_spot_label_table() -> None:
    assert GATE_FUTURES in VENUES
    code = normalize(
        GateApiError("BALANCE_NOT_ENOUGH", "x"), venue=GATE_FUTURES
    )
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
        (
            "post-only order would cross at 50001.01 (best opposite 50001)",
            RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
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


# --- Binance: numeric codes, refined by message where they are coarse -------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (-1003, RejectCode.VENUE_RATE_LIMITED),
        (-1015, RejectCode.VENUE_RATE_LIMITED),
        (-1021, RejectCode.VENUE_AUTH_FAILED),
        (-1022, RejectCode.VENUE_AUTH_FAILED),
        (-2015, RejectCode.VENUE_AUTH_FAILED),
        (-1111, RejectCode.VENUE_INVALID_PARAM),
        (-1121, RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
        (-2013, RejectCode.VENUE_ORDER_NOT_FOUND),
        (-2026, RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        (-1008, RejectCode.VENUE_INTERNAL_ERROR),
    ],
)
def test_a_known_binance_code_becomes_a_venue_code(
    code: int, expected: RejectCode
) -> None:
    assert normalize(BinanceWsError(code, "nope"), venue=BINANCE) is expected


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "Account has insufficient balance for requested action.",
            RejectCode.VENUE_INSUFFICIENT_BALANCE,
        ),
        (
            "Order would immediately match and take.",
            RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
        ("Duplicate order sent.", RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID),
        ("Market is closed.", RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
        (
            "Account has too many open orders.",
            RejectCode.VENUE_RISK_LIMIT,
        ),
        (
            "This account may not place or cancel orders.",
            RejectCode.VENUE_PERMISSION_DENIED,
        ),
        ("Price * QTY is zero or less.", RejectCode.VENUE_BELOW_MINIMUM),
        ("Filter failure: MIN_NOTIONAL", RejectCode.VENUE_BELOW_MINIMUM),
        ("Filter failure: PERCENT_PRICE", RejectCode.VENUE_INVALID_PARAM),
    ],
)
def test_one_binance_code_is_split_apart_by_its_message(
    message: str, expected: RejectCode
) -> None:
    """``-2010`` is every rejected new order; only the message says which.

    Mapping the code alone would tell a strategy "insufficient balance" when
    the real reason was a post-only that would have crossed — a specific,
    wrong answer, which is the failure this table exists to avoid.
    """
    assert normalize(BinanceWsError(-2010, message), venue=BINANCE) is expected


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        # This market's own refusals, which spot has no code for.
        (-5022, RejectCode.VENUE_POST_ONLY_WOULD_CROSS),  # GTX would have taken
        (-2019, RejectCode.VENUE_INSUFFICIENT_BALANCE),  # margin insufficient
        (-2022, RejectCode.VENUE_INVALID_PARAM),  # reduce-only rejected
        (-2027, RejectCode.VENUE_RISK_LIMIT),  # past max position at leverage
        (-4164, RejectCode.VENUE_BELOW_MINIMUM),  # notional under the floor
        # And everything both markets share, from the spot table underneath.
        (-1121, RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
        (-2013, RejectCode.VENUE_ORDER_NOT_FOUND),
        (-1022, RejectCode.VENUE_AUTH_FAILED),
    ],
)
def test_a_known_binance_future_code_becomes_a_venue_code(
    code: int, expected: RejectCode
) -> None:
    """Futures inherits spot's numbering and adds the margined book's own.

    ``-5022`` is the one that has no spot counterpart at all: post-only is an
    order type there, refused inside the ``-2010`` message, and a time-in-force
    here with a code of its own.
    """
    assert normalize(BinanceWsError(code, "nope"), venue=BINANCE_FUTURE) is expected


def test_the_futures_venue_is_normalized_at_all() -> None:
    """A venue missing from the table would fall through as unrecognised."""
    assert BINANCE_FUTURE in VENUES
    assert VENUES["BinanceDelivery"] is VENUES[BINANCE_FUTURE]


def test_a_2010_with_a_message_we_do_not_know_stays_a_plain_reject() -> None:
    """The refinement narrows; it never invents."""
    code = normalize(
        BinanceWsError(-2010, "Some new rejection reason"), venue=BINANCE
    )
    assert code is RejectCode.VENUE_REJECTED


def test_a_cancel_refused_by_restrictions_is_not_a_missing_order() -> None:
    """``-2011`` usually means the order is gone; sometimes it means it moved on."""
    assert normalize(
        BinanceWsError(-2011, "Cancel order is invalid. Check origClOrdId"),
        venue=BINANCE,
    ) is RejectCode.VENUE_ORDER_NOT_FOUND
    assert normalize(
        BinanceWsError(-2011, "Order was not canceled due to cancel restrictions."),
        venue=BINANCE,
    ) is RejectCode.VENUE_ORDER_ALREADY_CLOSED


def test_an_unmapped_binance_code_survives_as_itself() -> None:
    assert normalize(BinanceWsError(-9999, "brand new"), venue=BINANCE) == -9999


def test_binance_codes_cannot_be_mistaken_for_ours() -> None:
    """Native codes pass through, so the two numberings must not overlap.

    ``reject_codes`` warns that a venue numbering inside 100–299 would be
    indistinguishable from a code this platform assigned. Binance's are all
    negative, which is why leaving one unmapped is safe.
    """
    from mftik_td.errors import BINANCE as TABLE

    assert all(code < 0 for code in TABLE.codes)
    assert all(code < 0 for code in TABLE.refine)
    assert not is_normalized(-2010)


# --- the wrapper adapters raise through ------------------------------------


def test_a_venue_error_wrapped_in_an_order_error_still_normalizes() -> None:
    """The order path never hands ``normalize`` the venue's own exception.

    Every adapter re-raises as ``OrderError`` so TD publishes a reject rather
    than a transport failure, and ``OrderError`` carries no code. Before the
    chain was searched this table was unreachable from the order path and
    every rejection came back ``VENUE_REJECTED`` — which is what a live
    ``-1111`` actually did.
    """
    venue_error = BinanceWsError(-1111, "Parameter 'quantity' has too much precision.")
    wrapped = OrderError(str(venue_error))
    wrapped.__cause__ = venue_error

    assert normalize(wrapped, venue=BINANCE) is RejectCode.VENUE_INVALID_PARAM


def test_a_wrapped_gate_label_still_normalizes() -> None:
    """Gate's adapter wraps the same way, so it had the same blind spot."""
    venue_error = GateApiError("BALANCE_NOT_ENOUGH", "not enough USDT")
    wrapped = OrderError(str(venue_error))
    wrapped.__cause__ = venue_error

    assert normalize(wrapped, venue=GATE) is RejectCode.VENUE_INSUFFICIENT_BALANCE


def test_message_refinement_reads_the_cause_not_the_wrapper() -> None:
    """``-2010`` is split by message; the message has to come from the venue."""
    venue_error = BinanceWsError(
        -2010, "Account has insufficient balance for requested action."
    )
    wrapped = OrderError(str(venue_error))
    wrapped.__cause__ = venue_error

    assert normalize(wrapped, venue=BINANCE) is (
        RejectCode.VENUE_INSUFFICIENT_BALANCE
    )


def test_a_local_refusal_with_no_cause_is_still_a_plain_reject() -> None:
    """Nothing to walk to, so nothing changes for TD's own OrderErrors."""
    assert normalize(OrderError("limit order requires a price"), venue=BINANCE) is (
        RejectCode.VENUE_REJECTED
    )


def test_a_cyclic_cause_chain_does_not_hang() -> None:
    first = OrderError("outer")
    second = OrderError("inner")
    first.__cause__ = second
    second.__cause__ = first

    assert normalize(first, venue=BINANCE) is RejectCode.VENUE_REJECTED


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
        BinanceSpotPrivateClient,
        GateSpotPrivateClient,
        GateFuturesPrivateClient,
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


# --- Bybit ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (110007, RejectCode.VENUE_INSUFFICIENT_BALANCE),
        (170131, RejectCode.VENUE_INSUFFICIENT_BALANCE),
        (110001, RejectCode.VENUE_ORDER_NOT_FOUND),
        # The same fact, numbered differently on the other book.
        (170213, RejectCode.VENUE_ORDER_NOT_FOUND),
        (10004, RejectCode.VENUE_AUTH_FAILED),
        (10010, RejectCode.VENUE_IP_NOT_WHITELISTED),
        (10006, RejectCode.VENUE_RATE_LIMITED),
        (110020, RejectCode.VENUE_RISK_LIMIT),
        (170140, RejectCode.VENUE_BELOW_MINIMUM),
    ],
)
def test_bybit_codes_normalize(code: int, expected: RejectCode) -> None:
    assert normalize(BybitWsError(code, "refused"), venue="Bybit") == expected


def test_a_bybit_code_survives_being_wrapped_as_an_order_error() -> None:
    """The connector re-raises venue rejections as ``OrderError``, which
    carries no code — so the cause chain is where the answer lives."""
    cause = BybitWsError(110007, "ab not enough for new order", op="order.create")
    wrapped = OrderError(str(cause))
    wrapped.__cause__ = cause

    assert (
        normalize(wrapped, venue="Bybit")
        == RejectCode.VENUE_INSUFFICIENT_BALANCE
    )


def test_an_unmapped_bybit_code_passes_through_as_itself() -> None:
    """Bybit numbers in five and six digits, so one cannot be mistaken for a
    code this platform assigned — see ``mftik.protocol.reject_codes``."""
    assert normalize(BybitWsError(999999, "new one"), venue="Bybit") == 999999


def test_a_rest_refusal_normalizes_like_a_socket_one() -> None:
    """Same numbers over both transports, which is why one table covers them."""
    rest = BybitRestError(170213, "Order does not exist", status=200)
    assert normalize(rest, venue="Bybit") == RejectCode.VENUE_ORDER_NOT_FOUND


# --- refusals the venue puts on the order, not on the call ------------------
#
# ``normalize`` reads an exception. Bybit never raises for a crossed post-only:
# it accepts the request, kills the order, and names the reason on the ``order``
# topic. That is a second entry point into the same tables.


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (
            "EC_PostOnlyWillTakeLiquidity",
            RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
        ("EC_DuplicatedClOrdID", RejectCode.VENUE_DUPLICATE_CLIENT_ORDER_ID),
        ("EC_OrigClOrdIDDoesNotExist", RejectCode.VENUE_ORDER_NOT_FOUND),
        ("EC_TooLateToCancel", RejectCode.VENUE_ORDER_ALREADY_CLOSED),
        ("EC_LimitOrderInvalidPrice", RejectCode.VENUE_INVALID_PARAM),
        ("EC_InvalidSymbolStatus", RejectCode.VENUE_SYMBOL_NOT_TRADABLE),
    ],
)
def test_a_bybit_reject_reason_becomes_a_venue_code(
    reason: str, expected: RejectCode
) -> None:
    """The one that matters is the first: 202 is what ``ChaseOrder`` branches
    on to know a refusal was the ordinary cost of quoting passively."""
    assert normalize_reason(reason, venue="Bybit") == expected


def test_a_reject_reason_is_matched_whatever_its_case() -> None:
    """The table is written in Bybit's spelling so it can be read against the
    docs; the lookup must not depend on that spelling being reproduced."""
    assert (
        normalize_reason("ec_postonlywilltakeliquidity", venue="Bybit")
        == RejectCode.VENUE_POST_ONLY_WOULD_CROSS
    )


def test_an_unmapped_reject_reason_survives_as_itself() -> None:
    """Same bargain as an unmapped code: the detail outlives the table."""
    assert normalize_reason("EC_SomethingNew", venue="Bybit") == "EC_SomethingNew"


@pytest.mark.parametrize("reason", ["", "   "])
def test_a_refusal_with_no_reason_is_a_plain_venue_reject(reason: str) -> None:
    """Which is exactly what ``VENUE_REJECTED`` says: the venue refused it and
    gave nothing to go on."""
    assert normalize_reason(reason, venue="Bybit") is RejectCode.VENUE_REJECTED


def test_a_reason_from_a_venue_with_no_table_still_survives() -> None:
    assert normalize_reason("WHO_KNOWS", venue="Nowhere") == "WHO_KNOWS"


def test_a_reject_reason_can_match_a_message_fragment_too() -> None:
    """Message fragments are tried after labels, the same order ``normalize``
    ends with — so a venue whose stream carries prose is covered as well."""
    assert (
        normalize_reason(
            "post-only order would cross at 50001.01 (best opposite 50001)",
            venue=PAPER,
        )
        is RejectCode.VENUE_POST_ONLY_WOULD_CROSS
    )


# --- OKX --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (51008, RejectCode.VENUE_INSUFFICIENT_BALANCE),
        (51400, RejectCode.VENUE_ORDER_NOT_FOUND),
        (51603, RejectCode.VENUE_ORDER_NOT_FOUND),
        (50101, RejectCode.VENUE_AUTH_FAILED),
        (50110, RejectCode.VENUE_IP_NOT_WHITELISTED),
        (50011, RejectCode.VENUE_RATE_LIMITED),
        (51020, RejectCode.VENUE_BELOW_MINIMUM),
        (51024, RejectCode.VENUE_RISK_LIMIT),
    ],
)
def test_okx_codes_normalize(code: int, expected: RejectCode) -> None:
    assert normalize(OkxWsError(code, "refused"), venue="Okx") == expected


def test_an_okx_code_survives_being_wrapped_as_an_order_error() -> None:
    cause = OkxWsError(51008, "Insufficient balance", op="order")
    wrapped = OrderError(str(cause))
    wrapped.__cause__ = cause

    assert (
        normalize(wrapped, venue="Okx") == RejectCode.VENUE_INSUFFICIENT_BALANCE
    )


def test_an_unmapped_okx_code_passes_through_as_itself() -> None:
    assert normalize(OkxWsError(59999, "new one"), venue="Okx") == 59999


def test_an_okx_rest_refusal_normalizes_like_a_socket_one() -> None:
    rest = OkxRestError(51603, "Order does not exist", status=200)
    assert normalize(rest, venue="Okx") == RejectCode.VENUE_ORDER_NOT_FOUND

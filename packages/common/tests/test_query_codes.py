"""Market-data query codes — bands, retryability, and the wire round trip."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.models import Kline
from mft.protocol import (
    MD_FETCH_KLINES,
    MD_KLINES_RESULT,
    MD_QUERY_ACK,
    Envelope,
    MdFetchKlines,
    MdFetchKlinesEnvelope,
    MdKlinesResult,
    MdKlinesResultEnvelope,
    MdQueryAck,
    MdQueryAckEnvelope,
    QueryCode,
    Topics,
)
from mft.protocol.query_codes import (
    BAND_END,
    MD_BAND,
    VENUE_BAND,
    describe,
    is_md_internal,
    is_normalized,
    is_retryable,
    is_venue,
)

# --- bands -----------------------------------------------------------------


def test_every_code_sits_in_its_band() -> None:
    """The band is the contract; a code in the wrong one lies about who
    refused the query."""
    for code in QueryCode:
        if code is QueryCode.NONE:
            continue
        if code.name.startswith("MD_"):
            assert MD_BAND <= code < VENUE_BAND, code.name
        else:
            assert VENUE_BAND <= code < BAND_END, code.name


def test_md_band_means_the_venue_was_never_asked() -> None:
    assert is_md_internal(QueryCode.MD_VENUE_NOT_CONNECTED)
    assert not is_venue(QueryCode.MD_VENUE_NOT_CONNECTED)


def test_venue_band_means_the_venue_answered() -> None:
    assert is_venue(QueryCode.VENUE_RATE_LIMITED)
    assert not is_md_internal(QueryCode.VENUE_RATE_LIMITED)


def test_none_is_neither() -> None:
    assert not is_md_internal(QueryCode.NONE)
    assert not is_venue(QueryCode.NONE)
    assert not is_normalized(QueryCode.NONE)


def test_native_venue_codes_pass_through_as_venue() -> None:
    """An unmapped code is the venue's own; only a venue produces one."""
    assert is_venue("INVALID_CURRENCY_PAIR")
    assert is_venue(4001)
    assert not is_normalized("INVALID_CURRENCY_PAIR")
    assert not is_md_internal("INVALID_CURRENCY_PAIR")


# --- retryability ----------------------------------------------------------


def test_retryability_cuts_across_the_bands() -> None:
    """The whole reason ``is_retryable`` exists: the band answers "whose
    fault", not "try again", and the two do not line up."""
    # Venue's fault, worth retrying.
    assert is_retryable(QueryCode.VENUE_RATE_LIMITED)
    # MD's, and not worth retrying.
    assert not is_retryable(QueryCode.MD_INTERVAL_NOT_SUPPORTED)
    # MD's, but transient.
    assert is_retryable(QueryCode.MD_VENUE_CALL_FAILED)
    # Venue's, and permanent until the request changes.
    assert not is_retryable(QueryCode.VENUE_SYMBOL_NOT_FOUND)


@pytest.mark.parametrize(
    "code",
    [
        QueryCode.MD_INVALID_REQUEST,
        QueryCode.MD_UNSUPPORTED_REQUEST,
        QueryCode.MD_VENUE_NOT_CONNECTED,
        QueryCode.MD_VENUE_UNSUPPORTED_READ,
        QueryCode.MD_INTERVAL_NOT_SUPPORTED,
        QueryCode.MD_UNREADABLE_ACK,
        QueryCode.VENUE_REJECTED,
        QueryCode.VENUE_INVALID_PARAM,
        QueryCode.VENUE_SYMBOL_NOT_FOUND,
    ],
)
def test_standing_conditions_are_not_retryable(code: QueryCode) -> None:
    """Retrying any of these on a timer retries it forever."""
    assert not is_retryable(code)


def test_unmapped_native_codes_are_not_retryable() -> None:
    """Conservative on purpose: an unknown code could be anything, and a
    caller retrying it on a timer would never stop."""
    assert not is_retryable("SOME_VENUE_LABEL")
    assert not is_retryable(4001)


def test_success_is_not_retryable() -> None:
    assert not is_retryable(QueryCode.NONE)


# --- describe --------------------------------------------------------------


def test_describe_names_normalized_codes() -> None:
    assert describe(QueryCode.VENUE_RATE_LIMITED) == "201 VENUE_RATE_LIMITED"


def test_describe_passes_native_codes_through() -> None:
    assert describe("INVALID_CURRENCY_PAIR") == "INVALID_CURRENCY_PAIR"
    assert describe(4001) == "4001"


# --- wire ------------------------------------------------------------------


def test_fetch_klines_request_roundtrip() -> None:
    env = MdFetchKlinesEnvelope.wrap(
        MdFetchKlines(
            reply_channel="md.fetch.reply.sess-1",
            query_id="sess-1:7",
            ticker="Gate_Spot_BTCUSDT",
            interval="1mo",
            limit=500,
        ),
        type=MD_FETCH_KLINES,
        source="strategy.noop",
        session_id="sess-1",
    )
    restored = MdFetchKlinesEnvelope.from_json(env.to_json())

    assert restored.type == MD_FETCH_KLINES
    # Canonical spelling crosses the wire; the venue's never does.
    assert restored.payload.interval == "1mo"
    assert restored.payload.query_id == "sess-1:7"
    assert restored.payload.limit == 500


def test_ack_defaults_to_no_error() -> None:
    ack = MdQueryAck(query_id="q1", accepted=True)
    assert ack.error_code == QueryCode.NONE
    assert ack.reason == ""


def test_ack_refusal_roundtrip_keeps_the_code_machine_readable() -> None:
    env = MdQueryAckEnvelope.wrap(
        MdQueryAck(
            query_id="q1",
            accepted=False,
            reason="no connected client for Gate",
            error_code=QueryCode.MD_VENUE_NOT_CONNECTED,
        ),
        type=MD_QUERY_ACK,
        source="md",
        session_id="sess-1",
    )
    restored = MdQueryAckEnvelope.from_json(env.to_json())

    assert restored.payload.accepted is False
    # Crosses as a plain int, so ``==`` against the enum still holds.
    assert restored.payload.error_code == QueryCode.MD_VENUE_NOT_CONNECTED
    assert is_md_internal(restored.payload.error_code)


def test_klines_result_roundtrip() -> None:
    env = MdKlinesResultEnvelope.wrap(
        MdKlinesResult(
            query_id="q1",
            ticker="Gate_Spot_BTCUSDT",
            interval="1h",
            klines=[
                Kline(
                    universal_ticker="Gate_Spot_BTCUSDT",
                    interval="1h",
                    open_time=1_700_000_000,
                    open=Decimal("60100"),
                    high=Decimal("60900"),
                    low=Decimal("59900"),
                    close=Decimal("60500"),
                    volume=Decimal("100"),
                    quote_volume=Decimal("6000000"),
                    closed=True,
                )
            ],
        ),
        type=MD_KLINES_RESULT,
        source="md",
        session_id="sess-1",
    )
    restored = MdKlinesResultEnvelope.from_json(env.to_json())

    assert restored.payload.ok is True
    assert restored.payload.error_code == QueryCode.NONE
    kline = restored.payload.klines[0]
    # Decimals survive the round trip; a float here would round prices.
    assert kline.close == Decimal("60500")
    assert isinstance(kline.close, Decimal)
    assert kline.closed is True


def test_failed_result_carries_no_klines_but_a_code() -> None:
    result = MdKlinesResult(
        query_id="q1",
        ticker="Gate_Spot_BTCUSDT",
        interval="1h",
        ok=False,
        reason="[429] TOO_MANY_REQUESTS",
        error_code=QueryCode.VENUE_RATE_LIMITED,
    )
    assert result.klines == []
    assert is_retryable(result.error_code)


def test_empty_success_is_not_a_failure() -> None:
    """``ok`` True with no candles is a real answer — the venue has no
    history that far back — and must not read as an error."""
    result = MdKlinesResult(
        query_id="q1",
        ticker="Gate_Spot_BTCUSDT",
        interval="1h",
    )
    assert result.ok is True
    assert result.klines == []
    assert result.error_code == QueryCode.NONE


def test_untyped_envelope_still_reads_the_payload() -> None:
    """MD's serve loop dispatches on ``type`` before it knows the model."""
    env = Envelope[MdFetchKlines].wrap(
        MdFetchKlines(
            reply_channel="md.fetch.reply.sess-1",
            query_id="q1",
            ticker="Gate_Spot_BTCUSDT",
            interval="1m",
        ),
        type=MD_FETCH_KLINES,
        source="strategy.noop",
    )
    restored = MdFetchKlines.model_validate(
        MdFetchKlinesEnvelope.from_json(env.to_json()).payload
    )
    assert restored.ticker == "Gate_Spot_BTCUSDT"
    assert restored.limit == 100


def test_the_fetch_subject_is_not_keyed() -> None:
    """One subject for everyone: a read is not owned the way an order is, so
    competing consumers spread the work instead of stealing it."""
    assert Topics.md_fetch() == "md.fetch"


def test_a_caller_gets_its_own_reply_channel() -> None:
    """Distinct from ``md.{session_id}``, which only exists while a strategy
    holds a market-data attach."""
    assert Topics.md_fetch_reply("sess-1") == "md.fetch.reply.sess-1"
    assert Topics.md_fetch_reply("sess-1") != Topics.md_session("sess-1")


def test_the_request_carries_where_its_answer_goes() -> None:
    """The fetch plane holds no attachment to its callers, so routing has to
    ride on the request."""
    req = MdFetchKlines(
        reply_channel=Topics.md_fetch_reply("anyone"),
        query_id="q1",
        ticker="Gate_Spot_BTCUSDT",
        interval="1h",
    )
    assert req.reply_channel == "md.fetch.reply.anyone"
    # Nothing identifies the caller beyond where it wants the answer sent.
    assert not hasattr(req, "session_id")

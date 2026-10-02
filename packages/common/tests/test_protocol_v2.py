"""Protocol v2 types (IF-01) and the ``pv`` refusal (B4-01).

The wire shape is the envelope ``pv``, the renamed type strings from the
B0-03 inventory, the new subjects, and the atom hash.
:func:`mftik.protocol.reject_if_pv_mismatch` reads ``pv`` off the raw
frame and does not validate the payload. Where a receiver calls it is
issue #282; these tests do not put the check on the broker.
"""

from __future__ import annotations

import hashlib
import json

import pytest
from mftik.protocol import (
    MD_INTENT_DELETE,
    MD_INTENT_PATCH,
    MD_INTENT_PUT,
    MD_SESSION_LIST,
    MD_UNIVERSE,
    PROCMAN_REPORT,
    PROTOCOL_MISMATCH,
    PROTOCOL_VERSION,
    STS_SESSION_END,
    STS_SESSION_START,
    STS_SESSION_STATUS,
    TD_ACCOUNT_RESET,
    TD_ACCOUNT_STATE,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    TD_ORDER_CANCEL_SESSION,
    IntentOwner,
    MdIntentPut,
    MdUniverseEvent,
    ProcmanReport,
    ProcmanWorker,
    RpcError,
    StsCreateSessionRequest,
    StsCreateSessionRequestEnvelope,
    StsCreateSessionResult,
    StsSessionEndRequest,
    StsSessionStatus,
    TdAccountState,
    TdCancelSessionResult,
    Topics,
    atom_hash,
    reject_if_pv_mismatch,
)
from pydantic import ValidationError

ATOM_ID = "BinanceUM:public:btcusdt@bookTicker"
DOTTED_ATOM_ID = "Deribit:public:ticker.BTC-PERPETUAL.100ms"


# --- what the types are ----------------------------------------------------


def test_an_envelope_carries_the_protocol_version() -> None:
    request = StsCreateSessionRequest(
        session_id="s", created_by=1, strategy="noop"
    )
    envelope = StsCreateSessionRequestEnvelope.wrap(
        request, type=STS_SESSION_START, source="api"
    )

    assert envelope.pv == PROTOCOL_VERSION == 2
    restored = type(envelope).from_json(envelope.to_json())
    assert restored.pv == PROTOCOL_VERSION
    assert json.loads(envelope.to_json())["pv"] == PROTOCOL_VERSION


def test_start_restart_matches_strategy_yml() -> None:
    """``never`` is the default; ``always`` is refused; ``on_failure`` is kept."""
    bare = StsCreateSessionRequest(
        session_id="s", created_by=1, strategy="noop"
    )
    assert bare.restart == "never"

    chosen = StsCreateSessionRequest(
        session_id="s",
        created_by=1,
        strategy="noop",
        restart="on_failure",
    )
    assert chosen.restart == "on_failure"

    with pytest.raises(ValidationError, match="always is gone"):
        StsCreateSessionRequest(
            session_id="s", created_by=1, strategy="noop", restart="always"
        )


def test_start_accepts_with_starting_and_names_no_code_identity() -> None:
    result = StsCreateSessionResult(session_id="s")
    assert result.status == "starting"
    forbidden = {"strategy_digest", "env_generation", "code_ref"}
    assert forbidden.isdisjoint(StsCreateSessionRequest.model_fields)
    assert forbidden.isdisjoint(StsCreateSessionResult.model_fields)
    assert forbidden.isdisjoint(StsSessionStatus.model_fields)


def test_status_snapshot_carries_the_plan_fields() -> None:
    status = StsSessionStatus(
        session_id="s",
        status="starting",
        conditions={"MdReady": "12/14"},
        generation=1,
        observed_generation=0,
        worker_incarnation=2,
        restart_count=0,
    )
    assert status.conditions["MdReady"] == "12/14"
    assert status.worker_incarnation == 2
    # The publisher that already exists constructs the old shape.
    legacy = StsSessionStatus(session_id="s", status="live")
    assert legacy.conditions == {}
    assert legacy.progress is None


def test_inventory_renames_and_the_kept_list_type() -> None:
    assert STS_SESSION_START == "sts.session.start"
    assert STS_SESSION_END == "sts.session.end"
    assert STS_SESSION_STATUS == "sts.session.status"
    assert MD_INTENT_PUT == "md.intent.put"
    assert MD_INTENT_DELETE == "md.intent.delete"
    assert MD_INTENT_PATCH == "md.intent.patch"
    assert TD_INTENT_PUT == "td.intent.put"
    assert TD_INTENT_DELETE == "td.intent.delete"
    assert MD_SESSION_LIST == "md.session.list"
    assert TD_ACCOUNT_STATE == "td.account.state"
    assert TD_ACCOUNT_RESET == "td.account.reset"
    assert TD_ORDER_CANCEL_SESSION == "td.order.cancel_session"
    assert MD_UNIVERSE == "md.universe"
    assert PROCMAN_REPORT == "procman.report"
    assert PROTOCOL_MISMATCH == "protocol_mismatch"

    import mftik.protocol as protocol

    gone = (
        "STS_SESSION_CREATE",
        "STS_SESSION_STOP",
        "MD_SESSION_ATTACH",
        "MD_SESSION_DETACH",
        "TD_SESSION_ATTACH",
        "TD_SESSION_DETACH",
        "TD_SESSION_LIST",
        "CreateSessionRequest",
        "TdAttachRequest",
        "MdAttachRequest",
    )
    for name in gone:
        assert not hasattr(protocol, name), name


def test_subjects_and_the_atom_hash() -> None:
    digest = atom_hash(ATOM_ID)
    assert digest == hashlib.sha256(ATOM_ID.encode("utf-8")).hexdigest()
    assert "." not in digest
    assert Topics.md_atom("BinanceUM", digest) == f"md.a.BinanceUM.{digest}"
    assert Topics.atom_subject(ATOM_ID) == f"md.a.BinanceUM.{digest}"

    dotted = Topics.atom_subject(DOTTED_ATOM_ID)
    assert dotted.split(".") == ["md", "a", "Deribit", atom_hash(DOTTED_ATOM_ID)]

    assert Topics.sts_control("abc") == "sts.ctl.abc"
    assert Topics.sts_status("abc") == "sts.status.abc"
    assert Topics.md_worker("md-jp", "md/conn/Deribit/public/0") == (
        "md.w.md-jp.md/conn/Deribit/public/0"
    )
    assert Topics.md_universe("abc") == "md.universe.abc"
    assert Topics.td_account_state(42) == "td.account.state.42"
    assert Topics.procman_report("sts", "sts-jp") == "procman.report.sts.sts-jp"
    assert atom_hash(ATOM_ID) != atom_hash(
        "BinanceUM:market:btcusdt@bookTicker"
    )


def test_intent_put_is_idempotent_in_shape_and_carries_an_owner() -> None:
    owner = IntentOwner(sts_instance="sts-jp", session_id="s")
    put = MdIntentPut(
        session_id="s",
        owner=owner,
        feeds=["ticker.Paper_Spot_BTCUSDT"],
    )
    assert put.feeds == {"*": ["ticker.Paper_Spot_BTCUSDT"]}
    assert put.owner.sts_instance == "sts-jp"
    assert put.selects == []

    end = StsSessionEndRequest(session_id="s", reason="operator_stop")
    assert end.reason == "operator_stop"

    change = MdUniverseEvent(
        session_id="s",
        name="btc_q",
        added=["Deribit_Future_BTC-27MAR26"],
        current="Deribit_Future_BTC-27MAR26",
        epoch=3,
    )
    assert change.removed == []
    assert change.epoch == 3


def test_account_state_vocabulary_and_report_omit_code_identity_axes() -> None:
    state = TdAccountState(
        api_id=7, incarnation=1, state="degraded", version=4, reason="private ws"
    )
    assert state.state == "degraded"
    with pytest.raises(ValidationError):
        TdAccountState(
            api_id=7, incarnation=1, state="attached", version=1
        )

    report = ProcmanReport(
        generation=2,
        workers=[ProcmanWorker(id="td/account/7", code_ref="1.4.0", rss_bytes=10)],
    )
    assert report.generation == 2
    assert "strategy_digest" not in ProcmanReport.model_fields
    assert "env_generation" not in ProcmanWorker.model_fields
    assert "strategy_digest" not in ProcmanWorker.model_fields


def test_cancel_session_reports_what_it_could_not_confirm() -> None:
    result = TdCancelSessionResult(
        session_id="s", ok=False, unconfirmed=["cid-1"]
    )
    assert result.unconfirmed == ["cid-1"]


# --- what B4-01 makes true -------------------------------------------------


def _frame(*, pv: object | None, payload: object) -> str:
    body: dict[str, object] = {
        "id": "1",
        "type": STS_SESSION_START,
        "source": "api",
        "ts": 0,
        "payload": payload,
    }
    if pv is not None:
        body["pv"] = pv
    return json.dumps(body)


def test_a_different_pv_is_protocol_mismatch_and_the_payload_is_not_parsed() -> None:
    """F26: a different version is refused, and the payload is not read.

    The payload here is not a start request. A receiver that validated it
    would raise ``ValidationError``. The refusal is ``protocol_mismatch``
    instead, which is how a peer learns the versions differ rather than
    that its payload was misshapen.
    """
    error = reject_if_pv_mismatch(
        _frame(pv=PROTOCOL_VERSION + 1, payload={"not": "a start request"})
    )
    assert error is not None
    assert error.code == PROTOCOL_MISMATCH

    wrong_type = reject_if_pv_mismatch(
        _frame(pv=str(PROTOCOL_VERSION), payload={})
    )
    assert wrong_type is not None
    assert wrong_type.code == PROTOCOL_MISMATCH


def test_a_missing_pv_is_protocol_mismatch() -> None:
    """The unversioned envelope is not this version.

    Omitting ``pv`` must not fall through to the field's default. That
    default is what a sender stamps. A receiver that trusted it would
    accept every old frame as current.
    """
    error = reject_if_pv_mismatch(_frame(pv=None, payload={}))
    assert error is not None
    assert error.code == PROTOCOL_MISMATCH


def test_a_matching_pv_is_not_a_mismatch_even_when_the_payload_is_garbage() -> None:
    """Matching ``pv`` is not a refusal. Parsing the payload is the next step.

    This function returns ``None`` and does not validate. A garbage
    payload with the right version is the caller's ``ValidationError``,
    not a ``protocol_mismatch``.
    """
    assert (
        reject_if_pv_mismatch(
            _frame(pv=PROTOCOL_VERSION, payload={"not": "a start request"})
        )
        is None
    )


def test_a_body_that_is_not_a_json_object_is_not_protocol_mismatch() -> None:
    """A malformed frame raises ``ValueError``, not ``protocol_mismatch``.

    The refusal code is only for a JSON object. An array, a scalar,
    invalid JSON, or bytes that are not UTF-8 are a broken frame, and
    the caller does not turn them into an ``RpcError``.
    """
    for raw in (
        b"[]",
        b"null",
        b"1",
        b'"pv"',
        b"",
        b"{",
        b"not-json",
        "  ",
        b"\xff",
    ):
        with pytest.raises(ValueError, match="JSON object"):
            reject_if_pv_mismatch(raw)


def test_only_an_integer_pv_matches_and_a_byte_frame_is_read_as_json() -> None:
    """JSON ``true`` and ``2.0`` are not the integer protocol version.

    ``bool`` is a subclass of ``int``, and ``2.0`` compares equal to
    ``2``. The check uses the JSON type. Bytes are the frame a
    transport hands over: ``b"{}"`` has no ``pv``, and an object whose
    only field is the current ``pv`` is not validated any further.
    """
    for pv in (True, False, float(PROTOCOL_VERSION)):
        error = reject_if_pv_mismatch(_frame(pv=pv, payload={}))
        assert error is not None
        assert type(error) is RpcError
        assert error.code == PROTOCOL_MISMATCH

    missing = reject_if_pv_mismatch(b"{}")
    assert missing is not None
    assert missing.code == PROTOCOL_MISMATCH

    assert (
        reject_if_pv_mismatch(json.dumps({"pv": PROTOCOL_VERSION}).encode())
        is None
    )

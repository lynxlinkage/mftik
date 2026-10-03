"""``td.intent.put`` and ``td.intent.delete``, called directly."""

from __future__ import annotations

from mftik.intent_gc import on_sts_report
from mftik.protocol import (
    TD_ERROR,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    Envelope,
    IntentOwner,
    ProcmanReport,
    RpcError,
    TdIntentDeleteResult,
    TdIntentPutResult,
)
from mftik_td.controller import TdIntentBook, intent_handler, trading_active


def _message(payload: dict[str, object], type_: str) -> Envelope[dict[str, object]]:
    session_id = payload.get("session_id")
    return Envelope[dict[str, object]].wrap(
        payload,
        type=type_,
        source="api",
        session_id=session_id if isinstance(session_id, str) else None,
    )


def _put(
    *api_ids: int,
    session_id: str = "abc",
    owner_session: str | None = None,
) -> dict:
    return {
        "session_id": session_id,
        "owner": {
            "sts_instance": "sts",
            "session_id": session_id if owner_session is None else owner_session,
        },
        "api_ids": list(api_ids),
    }


async def test_put_and_delete_move_the_trading_level() -> None:
    book = TdIntentBook()
    handler = intent_handler(book)
    reply = await handler(_message(_put(7, 8), TD_INTENT_PUT))
    assert reply is not None
    assert reply.type == TD_INTENT_PUT
    assert reply.session_id == "abc"
    assert TdIntentPutResult.model_validate(reply.payload).session_id == "abc"
    assert trading_active(7, book.rows()) is True
    await handler(_message(_put(8), TD_INTENT_PUT))
    assert trading_active(7, book.rows()) is False
    assert trading_active(8, book.rows()) is True
    deleted = await handler(
        _message(
            {
                "session_id": "abc",
                "owner": {"sts_instance": "sts", "session_id": "abc"},
                "api_ids": [],
            },
            TD_INTENT_DELETE,
        )
    )
    assert deleted is not None
    assert TdIntentDeleteResult.model_validate(deleted.payload).session_id == "abc"
    assert book.rows() == ()


async def test_owner_mismatch_and_a_bad_payload_are_errors() -> None:
    book = TdIntentBook()
    handler = intent_handler(book)
    mismatch = await handler(_message(_put(7, owner_session="other"), TD_INTENT_PUT))
    assert mismatch is not None
    assert mismatch.type == TD_ERROR
    assert mismatch.session_id == "abc"
    assert RpcError.model_validate(mismatch.payload).code == "owner_mismatch"
    assert book.rows() == ()
    invalid = await handler(_message({}, TD_INTENT_DELETE))
    assert invalid is not None
    assert RpcError.model_validate(invalid.payload).code == "invalid_payload"


async def test_delete_of_an_absent_owner_succeeds() -> None:
    book = TdIntentBook()
    reply = await intent_handler(book)(
        _message(
            {
                "session_id": "abc",
                "owner": {"sts_instance": "sts", "session_id": "abc"},
            },
            TD_INTENT_DELETE,
        )
    )
    assert reply is not None
    assert reply.type == TD_INTENT_DELETE
    assert book.rows() == ()


async def test_two_reports_that_omit_the_owner_release_it() -> None:
    book = TdIntentBook()
    await intent_handler(book)(_message(_put(7), TD_INTENT_PUT))
    owner = IntentOwner(sts_instance="sts", session_id="abc")
    first = on_sts_report(
        book.gc_states,
        sts_instance="sts",
        held=book.owners(),
        report=ProcmanReport(generation=1, workers=[]),
    )
    book.release_owners(first)
    assert owner in book.owners()
    second = on_sts_report(
        book.gc_states,
        sts_instance="sts",
        held=book.owners(),
        report=ProcmanReport(generation=2, workers=[]),
    )
    book.release_owners(second)
    assert owner not in book.owners()
    assert trading_active(7, book.rows()) is False

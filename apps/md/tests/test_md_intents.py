"""``md.intent.put`` and ``md.intent.delete``, called directly."""

from __future__ import annotations

from mftik.intent_gc import on_sts_report
from mftik.protocol import (
    MD_ERROR,
    MD_INTENT_DELETE,
    MD_INTENT_PUT,
    Envelope,
    IntentOwner,
    MdIntentDeleteResult,
    MdIntentPutResult,
    ProcmanReport,
    ProcmanWorker,
    RpcError,
)
from mftik_md.intents import MdIntentBook, md_intent_handler


def _message(payload: dict[str, object], type_: str) -> Envelope[dict[str, object]]:
    session_id = payload.get("session_id")
    return Envelope[dict[str, object]].wrap(
        payload,
        type=type_,
        source="api",
        session_id=session_id if isinstance(session_id, str) else None,
    )


def _put(session_id: str = "abc", *, owner_session: str | None = None) -> dict:
    return {
        "session_id": session_id,
        "owner": {
            "sts_instance": "sts",
            "session_id": session_id if owner_session is None else owner_session,
        },
        "feeds": ["book"],
        "selects": [
            {
                "kind": "rolling_future",
                "name": "btc_q",
                "venue": "Deribit",
                "underlying": "BTC",
                "tenor": "quarterly",
                "topics": ["ticker"],
            }
        ],
    }


async def test_put_holds_feeds_and_selects_and_answers_no_atoms() -> None:
    book = MdIntentBook()
    reply = await md_intent_handler(book)(_message(_put(), MD_INTENT_PUT))
    assert reply is not None
    assert reply.type == MD_INTENT_PUT
    assert reply.session_id == "abc"
    result = MdIntentPutResult.model_validate(reply.payload)
    assert result.session_id == "abc"
    assert result.atoms == {}
    held = book.puts()
    assert len(held) == 1
    assert held[0].feeds["*"] == ["book"]
    assert held[0].selects[0].name == "btc_q"
    assert book.owners() == frozenset(
        {IntentOwner(sts_instance="sts", session_id="abc")}
    )


async def test_a_second_put_replaces_that_owner() -> None:
    book = MdIntentBook()
    handler = md_intent_handler(book)
    await handler(_message(_put(), MD_INTENT_PUT))
    replacement = _put()
    replacement["feeds"] = {"md-jp": ["ticker"]}
    replacement["selects"] = []
    await handler(_message(replacement, MD_INTENT_PUT))
    held = book.puts()
    assert len(held) == 1
    assert held[0].feeds == {"md-jp": ["ticker"]}
    assert held[0].selects == []


async def test_delete_is_idempotent_and_echoes_session_id() -> None:
    book = MdIntentBook()
    handler = md_intent_handler(book)
    await handler(_message(_put(), MD_INTENT_PUT))
    reply = await handler(
        _message(
            {
                "session_id": "abc",
                "owner": {"sts_instance": "sts", "session_id": "abc"},
            },
            MD_INTENT_DELETE,
        )
    )
    assert reply is not None
    assert MdIntentDeleteResult.model_validate(reply.payload).session_id == "abc"
    assert book.puts() == ()
    again = await handler(
        _message(
            {
                "session_id": "abc",
                "owner": {"sts_instance": "sts", "session_id": "abc"},
            },
            MD_INTENT_DELETE,
        )
    )
    assert again is not None
    assert again.type == MD_INTENT_DELETE


async def test_owner_mismatch_and_a_bad_payload_are_errors() -> None:
    book = MdIntentBook()
    handler = md_intent_handler(book)
    mismatch = await handler(_message(_put(owner_session="other"), MD_INTENT_PUT))
    assert mismatch is not None
    assert mismatch.type == MD_ERROR
    assert mismatch.session_id == "abc"
    assert RpcError.model_validate(mismatch.payload).code == "owner_mismatch"
    assert book.puts() == ()
    invalid = await handler(_message({}, MD_INTENT_PUT))
    assert invalid is not None
    assert RpcError.model_validate(invalid.payload).code == "invalid_payload"


async def test_two_reports_that_omit_the_owner_release_it() -> None:
    book = MdIntentBook()
    await md_intent_handler(book)(_message(_put(), MD_INTENT_PUT))
    owner = IntentOwner(sts_instance="sts", session_id="abc")
    other = ProcmanWorker(
        id="sts/session/other",
        code_ref="v1",
        rss_bytes=None,
        phase="stopping",
        ready=False,
        incarnation=1,
    )
    first = on_sts_report(
        book.gc_states,
        sts_instance="sts",
        held=book.owners(),
        report=ProcmanReport(generation=1, workers=[other]),
    )
    book.release_owners(first)
    assert owner in book.owners()
    second = on_sts_report(
        book.gc_states,
        sts_instance="sts",
        held=book.owners(),
        report=ProcmanReport(generation=2, workers=[other]),
    )
    book.release_owners(second)
    assert owner not in book.owners()

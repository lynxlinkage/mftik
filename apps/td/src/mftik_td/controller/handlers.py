"""``td.intent.put`` / ``td.intent.delete`` as one handler (IF-02, B4-07).

Registered on the running process, on ``td.{instance}``. The held set is
a :class:`TdIntentBook`, not a :class:`~mftik_td.controller.TdOrchestrator`:
constructing the orchestrator needs a
:class:`~mftik.procman.RestartIntensity`, and those numbers are not
chosen (issue #286). The process therefore does not construct one and
does not call :meth:`~mftik_td.controller.TdOrchestrator.reconcile`.

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). A bad payload is an error envelope, not an exception:
:func:`mftik.broker.handler.serve` logs a raised exception and sends
nothing (H5).

**Pooled subjects are not this plane's.** TD has no ``Topics.TD``. Each
put lands on the instance the account is bound to.

Report GC drops an owner with the same :func:`apply_delete` an empty
``api_ids`` delete uses. A delete does not clear the absence streak:
the reports are the samples, and the next one still counts.
"""

from __future__ import annotations

from collections.abc import Collection

from mftik.broker.handler import Handler, Reply
from mftik.instance import validate_instance_name
from mftik.intent_gc import InstanceGcState
from mftik.protocol import (
    TD_ERROR,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    Envelope,
    IntentOwner,
    RpcError,
    RpcErrorEnvelope,
    TdIntentDelete,
    TdIntentDeleteResult,
    TdIntentPut,
    TdIntentPutResult,
    Topics,
    UntypedEnvelope,
)
from pydantic import ValidationError

from mftik_td.controller.decisions import apply_delete, apply_put

#: The two types this subject carries (§8.1, §8.3). A put replaces the
#: owner's whole account set. A delete releases accounts. Both feed the
#: trading-layer level. Neither creates an account worker (F35).
INTENT_TYPES = frozenset({TD_INTENT_PUT, TD_INTENT_DELETE})


class TdIntentBook:
    """The intents this TD process holds, in memory.

    Postgres ``td_intents`` belongs to the API and the STS controller.
    This object does not read or write it, and it does not set
    ``released_at``. A restart starts empty: nothing is pushed to an
    account worker from that empty set (P5). Rebuilding it is not
    B4-07.

    :attr:`gc_states` is the per-STS-instance cursor
    :func:`mftik.intent_gc.on_sts_report` updates. The subscription in
    the running process passes it through so a test can read it.
    """

    def __init__(self) -> None:
        self._rows: tuple[TdIntentPut, ...] = ()
        self.gc_states: dict[str, InstanceGcState] = {}

    def rows(self) -> tuple[TdIntentPut, ...]:
        """Held puts, in the order :func:`apply_put` keeps."""
        return self._rows

    def owners(self) -> frozenset[IntentOwner]:
        """Session owners currently held. The report subscription reads this."""
        return frozenset(row.owner for row in self._rows)

    def put(self, intent: TdIntentPut) -> None:
        self._rows = apply_put(self._rows, intent)

    def delete(self, intent: TdIntentDelete) -> None:
        self._rows = apply_delete(self._rows, intent)

    def release_owners(self, owners: Collection[IntentOwner]) -> None:
        """Drop ``owners`` the way an empty ``api_ids`` delete would.

        The report subscription calls this with the set
        :func:`mftik.intent_gc.on_sts_report` returned. An owner that is
        already gone is a no-op.
        """
        for owner in owners:
            self.delete(
                TdIntentDelete(
                    session_id=owner.session_id,
                    owner=owner,
                    api_ids=[],
                )
            )

    def clear(self) -> None:
        """Drop every row and every cursor. Tests use this between cases."""
        self._rows = ()
        self.gc_states.clear()


_BOOK = TdIntentBook()


def intent_book() -> TdIntentBook:
    """The held set the running TD process answers from.

    One book for every subject this process serves. A unit test builds
    its own book. A test that drives the process calls
    :meth:`TdIntentBook.clear` so the previous case does not leak.
    """
    return _BOOK


def intent_handler(book: TdIntentBook) -> Handler:
    """``td.intent.put`` and ``td.intent.delete`` for ``book``.

    Served on :func:`control_subject`. The subject is not bound here.
    The reply is :class:`~mftik.protocol.v2.TdIntentPutResult` or
    :class:`~mftik.protocol.v2.TdIntentDeleteResult`, echoing
    ``session_id``. Applying the put or delete is
    :func:`mftik_td.controller.apply_put` /
    :func:`mftik_td.controller.apply_delete`.

    ``owner.session_id`` must equal ``session_id``. A mismatch is
    ``owner_mismatch``, and the book is unchanged. There is no database
    lookup for the account binding.

    The order path does not come through here (P1). ``td.order.{api_id}``
    is the account worker's. This handler does not publish the trading
    bit: the running process does not deliver it (P5).
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        if message.type not in INTENT_TYPES:
            return _error(message, "unknown_type", f"unknown type: {message.type}")
        try:
            if message.type == TD_INTENT_PUT:
                return _put(book, message)
            return _delete(book, message)
        except (ValidationError, ValueError) as exc:
            return _error(message, "invalid_payload", str(exc))

    return handle


def _put(book: TdIntentBook, message: UntypedEnvelope) -> Reply:
    put = TdIntentPut.model_validate(message.payload or {})
    refused = _mismatch(message, put.session_id, put.owner.session_id)
    if refused is not None:
        return refused
    book.put(put)
    return Envelope[TdIntentPutResult].wrap(
        TdIntentPutResult(session_id=put.session_id),
        type=TD_INTENT_PUT,
        source="td",
        session_id=put.session_id,
    )


def _delete(book: TdIntentBook, message: UntypedEnvelope) -> Reply:
    delete = TdIntentDelete.model_validate(message.payload or {})
    refused = _mismatch(message, delete.session_id, delete.owner.session_id)
    if refused is not None:
        return refused
    book.delete(delete)
    return Envelope[TdIntentDeleteResult].wrap(
        TdIntentDeleteResult(session_id=delete.session_id),
        type=TD_INTENT_DELETE,
        source="td",
        session_id=delete.session_id,
    )


def _mismatch(
    message: UntypedEnvelope, session_id: str, owner_session_id: str
) -> Reply | None:
    if owner_session_id == session_id:
        return None
    return _error(
        message,
        "owner_mismatch",
        f"owner.session_id {owner_session_id!r} does not match "
        f"session_id {session_id!r}",
        session_id=session_id,
    )


def _error(
    message: UntypedEnvelope,
    code: str,
    text: str,
    *,
    session_id: str | None = None,
) -> Reply:
    echoed = message.session_id if session_id is None else session_id
    return RpcErrorEnvelope.wrap(
        RpcError(code=code, message=text),
        type=TD_ERROR,
        source="td",
        session_id=echoed,
    )


def control_subject(instance: str) -> str:
    """``td.{instance}``, the subject the intent handler is served on."""
    return Topics.td(validate_instance_name(instance))

"""``md.intent.put`` / ``md.intent.delete`` held in this process (B4-07).

**Pooled ``md`` subject.** A declaration ``md: {"*": …}`` is put on
:data:`mftik.protocol.Topics.MD`. Whichever MD instance answers that
put holds the owner's feeds and selects in its own memory. A delete
that lands on an instance that does not hold the owner still succeeds;
delete is idempotent. The copy on the other instance is released by
that instance's report GC (§8.2 rule 3).

Resolving feeds is not this module.
:class:`~mftik.protocol.v2.MdIntentPutResult` ``atoms`` stays empty
until paper ``atoms_for`` (B7-02g). Nothing here reads or writes
``md_intents``. A release drops the owner from memory only. Rebuilding
that memory from the database after a restart is B8-01, and until then
nothing is pushed to a connection worker from an empty held set (P5).

A handler returns an error envelope instead of raising. The serve loop
logs a raised exception and sends no reply (H5).
"""

from __future__ import annotations

from collections.abc import Collection

from mftik.broker.handler import Handler, Reply
from mftik.intent_gc import InstanceGcState
from mftik.protocol import (
    MD_ERROR,
    MD_INTENT_DELETE,
    MD_INTENT_PUT,
    Envelope,
    IntentOwner,
    MdIntentDelete,
    MdIntentDeleteResult,
    MdIntentPut,
    MdIntentPutResult,
    RpcError,
    RpcErrorEnvelope,
    UntypedEnvelope,
)
from pydantic import ValidationError

#: The two types B4-07 answers. ``md.intent.patch`` stays unwired (B8).
INTENT_TYPES = frozenset({MD_INTENT_PUT, MD_INTENT_DELETE})


class MdIntentBook:
    """Feeds and selects held for each owner, in memory.

    One book is shared by every subject this process serves, including
    the pooled :data:`~mftik.protocol.Topics.MD`. A different process
    has a different book.
    """

    def __init__(self) -> None:
        self._by_owner: dict[IntentOwner, MdIntentPut] = {}
        self.gc_states: dict[str, InstanceGcState] = {}

    def puts(self) -> tuple[MdIntentPut, ...]:
        """Held puts. A second put of the same owner keeps its place."""
        return tuple(self._by_owner.values())

    def owners(self) -> frozenset[IntentOwner]:
        """Session owners currently held. The report subscription reads this."""
        return frozenset(self._by_owner)

    def put(self, intent: MdIntentPut) -> None:
        self._by_owner[intent.owner] = intent

    def delete(self, owner: IntentOwner) -> None:
        self._by_owner.pop(owner, None)

    def release_owners(self, owners: Collection[IntentOwner]) -> None:
        """Drop ``owners`` the way ``md.intent.delete`` would."""
        for owner in owners:
            self.delete(owner)

    def clear(self) -> None:
        """Drop every row and every cursor. Tests use this between cases."""
        self._by_owner.clear()
        self.gc_states.clear()


def md_intent_handler(book: MdIntentBook) -> Handler:
    """Answer put and delete against ``book``.

    ``owner.session_id`` must equal ``session_id``. A mismatch is
    ``owner_mismatch`` and the book is unchanged. Success echoes
    ``session_id``. Put's ``atoms`` is ``{}``.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        if message.type not in INTENT_TYPES:
            return _error(message, "unknown_type", f"unknown type: {message.type}")
        try:
            if message.type == MD_INTENT_PUT:
                return _put(book, message)
            return _delete(book, message)
        except (ValidationError, ValueError) as exc:
            return _error(message, "invalid_payload", str(exc))

    return handle


def _put(book: MdIntentBook, message: UntypedEnvelope) -> Reply:
    put = MdIntentPut.model_validate(message.payload or {})
    refused = _mismatch(message, put.session_id, put.owner.session_id)
    if refused is not None:
        return refused
    book.put(put)
    return Envelope[MdIntentPutResult].wrap(
        MdIntentPutResult(session_id=put.session_id, atoms={}),
        type=MD_INTENT_PUT,
        source="md",
        session_id=put.session_id,
    )


def _delete(book: MdIntentBook, message: UntypedEnvelope) -> Reply:
    delete = MdIntentDelete.model_validate(message.payload or {})
    refused = _mismatch(message, delete.session_id, delete.owner.session_id)
    if refused is not None:
        return refused
    book.delete(delete.owner)
    return Envelope[MdIntentDeleteResult].wrap(
        MdIntentDeleteResult(session_id=delete.session_id),
        type=MD_INTENT_DELETE,
        source="md",
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
        type=MD_ERROR,
        source="md",
        session_id=echoed,
    )

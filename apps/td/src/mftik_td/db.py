"""TD persistence helpers over mftik_db.td_sessions."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from mftik.protocol import IntentOwner, TdIntentPut
from mftik_db.models.api import Api
from mftik_db.repositories import (
    ApiRepository,
    IntentRepository,
    StsSessionRepository,
    TdSessionRepository,
)
from mftik_db.session import session_scope

logger = logging.getLogger(__name__)

#: Reads unreleased intents for one TD instance. Tests pass a stand-in.
ReadHeldIntents = Callable[[str], Awaitable[tuple[TdIntentPut, ...]]]


async def count_live_for_api(api_id: int) -> int:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.count_live_for_api(api_id)


async def get_api(api_id: int) -> Api | None:
    """Load the venue credential row backing ``api_id``."""
    async with session_scope() as db:
        return await ApiRepository(db).get(api_id)


@dataclass(frozen=True)
class AccountCredential:
    """The fields an account worker needs, copied out of the session."""

    api_id: int
    venue: str
    api_key: str
    api_secret: str
    passphrase: str | None
    cancel_on_disconnect: bool


@dataclass(frozen=True)
class InstanceBinding:
    """One ``apis`` row bound to an instance, without the secret."""

    api_id: int
    venue: str
    cancel_on_disconnect: bool


async def account_credential(api_id: int) -> AccountCredential | None:
    """The credential for ``api_id``, or ``None`` when the row is gone.

    Copied before the session closes. A detached ORM row would expire
    on the next attribute read.
    """
    async with session_scope() as db:
        row = await ApiRepository(db).get(api_id)
        if row is None:
            return None
        return AccountCredential(
            api_id=row.id,
            venue=row.venue,
            api_key=row.api_key,
            api_secret=row.api_secret,
            passphrase=row.passphrase,
            cancel_on_disconnect=bool(row.cancel_on_disconnect),
        )


async def read_held_intents(
    instance: str,
    *,
    scope=None,
) -> tuple[TdIntentPut, ...]:
    """Unreleased ``td_intents`` for accounts bound to ``instance``.

    One :class:`~mftik.protocol.TdIntentPut` per session, ``api_ids`` in
    row order. ``owner.sts_instance`` is ``sts_sessions.instance``. A
    row whose session is missing or has no instance fails the read:
    dropping it would look like "no intent" and the next push would
    turn that account's trading layer off (P5). An empty result is a
    successful read: this instance really has nothing unreleased.

    ``scope`` defaults to :func:`mftik_db.session.session_scope`. A
    test passes its scratch database. This does not write.
    """
    open_scope = session_scope if scope is None else scope
    async with open_scope() as db:
        apis = await ApiRepository(db).list_by_instance(instance)
        rows = await IntentRepository(db).unreleased_td([api.id for api in apis])
        grouped: dict[str, list[int]] = {}
        for row in rows:
            grouped.setdefault(row.session_id, []).append(int(row.api_id))
        sessions = StsSessionRepository(db)
        puts: list[TdIntentPut] = []
        for session_id, api_ids in grouped.items():
            record = await sessions.get_by_session_id(session_id)
            name = None if record is None else record.instance
            if not isinstance(name, str) or name == "":
                raise RuntimeError(
                    f"td intent session_id={session_id} has no sts instance"
                )
            puts.append(
                TdIntentPut(
                    session_id=session_id,
                    owner=IntentOwner(sts_instance=name, session_id=session_id),
                    api_ids=api_ids,
                )
            )
        return tuple(puts)


def install_intent_seed(book, puts: Sequence[TdIntentPut]) -> None:
    """Copy ``puts`` into ``book`` without dropping a live put.

    The boot book is empty, so this is the whole held set. A put that
    arrived while the read was in flight stays: the database copy does
    not replace that owner. An owner only the database has is added.
    """
    current = book.rows()
    if not current:
        for put in puts:
            book.put(put)
        return
    held = {row.owner for row in current}
    for put in puts:
        if put.owner not in held:
            book.put(put)


async def seed_intent_book(
    book,
    *,
    instance: str,
    read: ReadHeldIntents | None = None,
) -> bool:
    """Seed ``book`` from ``td_intents``. False means do not publish.

    A failed read leaves the book as it was. The caller keeps the
    trading-bit gate closed and tries again on the next pass. Success
    includes an empty held set.
    """
    load = read_held_intents if read is None else read
    try:
        puts = await load(instance)
        install_intent_seed(book, puts)
    except Exception:
        logger.exception(
            "TD intent seed failed instance=%s; trading bit not pushed",
            instance,
        )
        return False
    return True


async def bindings_for_instance(instance: str) -> tuple[InstanceBinding, ...]:
    """Every account bound to ``instance``, secrets left in the database."""
    async with session_scope() as db:
        rows = await ApiRepository(db).list_by_instance(instance)
        return tuple(
            InstanceBinding(
                api_id=row.id,
                venue=row.venue,
                cancel_on_disconnect=bool(row.cancel_on_disconnect),
            )
            for row in rows
        )

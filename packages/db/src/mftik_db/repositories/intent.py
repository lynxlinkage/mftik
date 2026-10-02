"""Intent rows: the store behind ``*.intent.put`` / ``delete`` / ``patch``.

The API writes a session's intents at start. The STS controller writes
them again when it heals. The session worker writes an MD patch while
the session runs. MD and TD controllers read the rows (§3.3). This
module does not decide whether a worker exists, and it does not reclaim
an owner from a liveness report (§8.2 rule 3). That reclaim is not this
writer.

**Identity.** One MD row per ``(session_id, instance)`` — one per MD
instance, not one per feed. One TD row per ``(session_id, api_id)``.
A second put does not insert another row.

**F38.** :meth:`IntentRepository.release` sets ``released_at``. It does
not delete. :meth:`IntentRepository.delete` and a put that drops a key
do the same: the row stays, and a later put of that key clears
``released_at`` on it. A row that is already released keeps the first
timestamp.

**Wholesale, per key.** A TD put replaces the session's ``api_id`` set.
An id that leaves the set is released; an id that arrives is inserted
or, if the row is still there, unmarked. An MD put does the same for
the feed declaration of each instance the message names, and does not
release instances the message does not name. A patch is that pattern
for one instance: ``add`` and ``remove`` are feed keys, applied as
remove then add.

``MdIntentPatch`` does not carry an instance. The subject does
(``md.{instance}``). :meth:`IntentRepository.patch` takes that instance
from the caller. This module does not add it to the message.

``atoms`` is MD's. A put that changes the declaration clears it back to
``{}`` (nothing resolved). A put that does not change the declaration
leaves whatever MD wrote.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from mftik.protocol import (
    MdIntentDelete,
    MdIntentPatch,
    MdIntentPut,
    TdIntentDelete,
    TdIntentPut,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.intent import MdIntent, TdIntent

_Put = TdIntentPut | MdIntentPut
_Delete = TdIntentDelete | MdIntentDelete


def _moment(at: datetime | None) -> datetime:
    if at is None:
        return datetime.now(UTC)
    if at.tzinfo is None:
        raise ValueError("at must be timezone-aware")
    return at


def _owner_matches(session_id: str, owner_session_id: str, sts_instance: str) -> None:
    if not session_id or not owner_session_id:
        raise ValueError("session_id is required")
    if session_id != owner_session_id:
        raise ValueError(
            "owner.session_id must be the intent's session_id"
        )
    if not sts_instance:
        raise ValueError("owner.sts_instance is required")


def _api_ids(ids: Sequence[int]) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for api_id in ids:
        if type(api_id) is not int or api_id < 1:
            raise ValueError(f"api_id must be a positive int, got {api_id!r}")
        if api_id in seen:
            continue
        seen.add(api_id)
        out.append(api_id)
    return out


def _instance_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise ValueError(f"instance must be a non-empty string, got {name!r}")
    return name


def _declaration(feeds: Sequence[str], selects: Sequence[Any]) -> list[Any]:
    """Feed keys, then each ``select:`` block, in message order.

    The column is one list (§8.4). Feed keys are strings. A select is
    the dumped block, which is a mapping, so a reader can tell the two
    apart without a second column.
    """
    body: list[Any] = []
    for feed in feeds:
        if not isinstance(feed, str) or not feed:
            raise ValueError(f"feed must be a non-empty string, got {feed!r}")
        body.append(feed)
    for block in selects:
        dumped = block.model_dump(mode="json")
        if not isinstance(dumped, dict):
            raise ValueError("select did not dump to a mapping")
        body.append(dumped)
    return body


def _feed_keys(stored: Sequence[Any]) -> list[str]:
    return [item for item in stored if isinstance(item, str)]


def _select_blocks(stored: Sequence[Any]) -> list[Any]:
    return [item for item in stored if isinstance(item, dict)]


class IntentRepository:
    """Reads and writes ``md_intents`` and ``td_intents``.

    Callers own the transaction. Nothing here commits, and nothing here
    publishes. A put is the row the RPC is about; sending the RPC is
    the caller's.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def md_for(self, session_id: str) -> Sequence[MdIntent]:
        """Every MD row for ``session_id``, released or not, by instance."""
        result = await self.session.execute(
            select(MdIntent)
            .where(MdIntent.session_id == session_id)
            .order_by(MdIntent.instance)
        )
        return result.scalars().all()

    async def td_for(self, session_id: str) -> Sequence[TdIntent]:
        """Every TD row for ``session_id``, released or not, by api_id."""
        result = await self.session.execute(
            select(TdIntent)
            .where(TdIntent.session_id == session_id)
            .order_by(TdIntent.api_id)
        )
        return result.scalars().all()

    async def unreleased_td(self, api_ids: Sequence[int]) -> Sequence[TdIntent]:
        """Unreleased ``td_intents`` rows whose ``api_id`` is in ``api_ids``.

        Read only. The TD process calls this once after a restart, with
        the accounts bound to its instance, and seeds the in-memory book
        before it publishes the trading bit (P5). A released row is not
        a held intent. An empty ``api_ids`` is no rows, not every row.
        Order is ``session_id``, then ``api_id``.
        """
        ids: list[int] = []
        seen: set[int] = set()
        for api_id in api_ids:
            if type(api_id) is not int or api_id < 1:
                raise ValueError(f"api_id must be a positive int, got {api_id!r}")
            if api_id in seen:
                continue
            seen.add(api_id)
            ids.append(api_id)
        if not ids:
            return ()
        result = await self.session.execute(
            select(TdIntent)
            .where(TdIntent.released_at.is_(None))
            .where(TdIntent.api_id.in_(ids))
            .order_by(TdIntent.session_id, TdIntent.api_id)
        )
        return result.scalars().all()

    async def put(self, message: _Put, *, at: datetime | None = None) -> None:
        """Register ``message``. Idempotent (P-1).

        TD: ``api_ids`` is the whole set for this session. MD: each
        instance named in ``feeds`` has its declaration replaced.
        Instances not named are left alone, including ones that are
        still active. ``selects`` are stored on every instance this
        message names; a caller that means one instance's selectors
        names one instance.
        """
        moment = _moment(at)
        _owner_matches(
            message.session_id,
            message.owner.session_id,
            message.owner.sts_instance,
        )
        if isinstance(message, TdIntentPut):
            await self._put_td(message, moment)
            return
        if isinstance(message, MdIntentPut):
            await self._put_md(message)
            return
        raise TypeError("put takes a TdIntentPut or an MdIntentPut")

    async def delete(
        self,
        message: _Delete,
        *,
        instance: str | None = None,
        at: datetime | None = None,
    ) -> None:
        """Release what ``message`` names. Idempotent. Does not delete.

        TD with an empty ``api_ids`` releases every account this session
        holds. A non-empty list releases those accounts and leaves the
        rest. An account the session does not hold is a no-op.

        MD releases one instance when ``instance`` is passed, and every
        MD row of the session when it is not. The message does not carry
        the instance; the subject does. Passing ``instance`` with a TD
        delete is a type error.
        """
        moment = _moment(at)
        _owner_matches(
            message.session_id,
            message.owner.session_id,
            message.owner.sts_instance,
        )
        if isinstance(message, TdIntentDelete):
            if instance is not None:
                raise TypeError("a TD intent delete has no instance")
            await self._delete_td(message, moment)
            return
        if isinstance(message, MdIntentDelete):
            await self._delete_md(message, instance, moment)
            return
        raise TypeError("delete takes a TdIntentDelete or an MdIntentDelete")

    async def patch(
        self,
        message: MdIntentPatch,
        *,
        instance: str,
    ) -> None:
        """Apply ``add`` and ``remove`` to one MD instance's feed keys.

        ``remove`` runs first, then ``add``, so a key named in both
        stays. Select blocks on the row are kept. A missing row is
        inserted when ``add`` leaves at least one feed; a patch that
        changes nothing does not insert one. Feed keys that change bump
        ``generation`` and clear ``atoms``. A released row whose feeds
        change is unmarked (``released_at`` cleared) and is not a second
        row. A patch that does not change the feeds leaves a released
        row released.
        """
        if not isinstance(message, MdIntentPatch):
            raise TypeError("patch takes an MdIntentPatch")
        _owner_matches(
            message.session_id,
            message.owner.session_id,
            message.owner.sts_instance,
        )
        name = _instance_name(instance)
        row = await self.session.get(MdIntent, (message.session_id, name))
        stored: list[Any] = list(row.feeds) if row is not None else []
        keys = _feed_keys(stored)
        remove = set(message.remove)
        keys = [key for key in keys if key not in remove]
        for feed in message.add:
            if not isinstance(feed, str) or not feed:
                raise ValueError(f"feed must be a non-empty string, got {feed!r}")
            if feed not in keys:
                keys.append(feed)
        new_feeds = [*keys, *_select_blocks(stored)]
        if row is None:
            if not keys:
                return
            self.session.add(
                MdIntent(
                    session_id=message.session_id,
                    instance=name,
                    feeds=new_feeds,
                    atoms={},
                    generation=1,
                )
            )
            await self.session.flush()
            return
        # A no-op patch does not bring a released row back. Gaining or
        # losing a feed does: the declaration changed, so this is the
        # same row becoming active again rather than a second insert.
        self._assign_feeds(row, new_feeds, reactivate=False)
        await self.session.flush()

    async def release(self, session_id: str, *, at: datetime | None = None) -> None:
        """Set ``released_at`` on every intent row of ``session_id``.

        F38. Rows that are already released keep their timestamp. No row
        is deleted. A session with no rows is a no-op.
        """
        if not session_id:
            raise ValueError("session_id is required")
        moment = _moment(at)
        for row in await self.md_for(session_id):
            self._release_row(row, moment)
        for row in await self.td_for(session_id):
            self._release_row(row, moment)
        await self.session.flush()

    async def _put_td(self, message: TdIntentPut, at: datetime) -> None:
        desired = _api_ids(message.api_ids)
        wanted = set(desired)
        rows = {row.api_id: row for row in await self.td_for(message.session_id)}
        for api_id, row in rows.items():
            if api_id in wanted:
                row.released_at = None
            else:
                self._release_row(row, at)
        for api_id in desired:
            if api_id in rows:
                continue
            self.session.add(
                TdIntent(session_id=message.session_id, api_id=api_id)
            )
        await self.session.flush()

    async def _put_md(self, message: MdIntentPut) -> None:
        # Selects ride on every instance this message names. A caller
        # that means one instance's selectors names one instance.
        # Dropping an instance is not this method: a put names the
        # instances it replaces, and the others stay (per instance,
        # unlike a TD put, whose key set is the whole session).
        for name, feeds in message.feeds.items():
            instance = _instance_name(name)
            body = _declaration(feeds, message.selects)
            row = await self.session.get(MdIntent, (message.session_id, instance))
            if row is None:
                self.session.add(
                    MdIntent(
                        session_id=message.session_id,
                        instance=instance,
                        feeds=body,
                        atoms={},
                        generation=1,
                    )
                )
                continue
            self._assign_feeds(row, body, reactivate=True)
        await self.session.flush()

    async def _delete_td(self, message: TdIntentDelete, at: datetime) -> None:
        wanted = set(_api_ids(message.api_ids))
        for row in await self.td_for(message.session_id):
            if wanted and row.api_id not in wanted:
                continue
            self._release_row(row, at)
        await self.session.flush()

    async def _delete_md(
        self, message: MdIntentDelete, instance: str | None, at: datetime
    ) -> None:
        if instance is not None:
            name = _instance_name(instance)
            row = await self.session.get(MdIntent, (message.session_id, name))
            if row is not None:
                self._release_row(row, at)
        else:
            for row in await self.md_for(message.session_id):
                self._release_row(row, at)
        await self.session.flush()

    def _assign_feeds(
        self, row: MdIntent, feeds: list[Any], *, reactivate: bool
    ) -> None:
        changed = list(row.feeds or []) != feeds
        row.feeds = feeds
        if changed:
            row.generation = int(row.generation) + 1
            row.atoms = {}
        # Put names the instance, so the row is desired again even when
        # the declaration did not change. Patch only comes back when the
        # declaration changed (``reactivate`` is false there).
        if reactivate or changed:
            row.released_at = None

    @staticmethod
    def _release_row(row: MdIntent | TdIntent, at: datetime) -> None:
        if row.released_at is None:
            row.released_at = at

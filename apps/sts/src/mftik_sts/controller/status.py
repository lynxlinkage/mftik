"""Session status as the controller writes it (§3.3, §5.2).

The column keeps the words readers already compare (``live``, ``done``,
``failed``). The v2 phase lives in ``conditions["phase"]`` and on the
``sts.status.{session_id}`` snapshot. No migration.

The orchestrator depends on :class:`StatusStore`, not on the ORM. Unit
tests pass a fake or nothing. :class:`DbStatusStore` is what the process
binds, and it no-ops when the API has not inserted the row.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from mftik_db.models.session import SessionStatus as ColumnStatus
from mftik_db.repositories.session import StsSessionRepository
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_sts.controller.types import SessionPhase

#: Phases whose row stays ``live``. Terminal phases use their own word.
_LIVE = frozenset(
    {
        SessionPhase.PENDING,
        SessionPhase.STARTING,
        SessionPhase.RUNNING,
        SessionPhase.STOPPING,
        SessionPhase.RESTARTING,
    }
)


def column_status_for(phase: SessionPhase) -> str:
    """The ``sts_sessions.status`` word for a v2 phase."""
    if phase is SessionPhase.DONE:
        return ColumnStatus.DONE.value
    if phase is SessionPhase.FAILED:
        return ColumnStatus.FAILED.value
    if phase in _LIVE:
        return ColumnStatus.LIVE.value
    return ColumnStatus.LIVE.value


@dataclass(frozen=True)
class StoredSession:
    """One row, as boot reads it. Not a second spec type."""

    session_id: str
    status: str
    instance: str | None
    restart: str
    generation: int
    strategy_digest: str | None
    env_generation: int | None
    strategy: str
    created_by: int
    created_at: float
    api_ids: tuple[int, ...]
    type_name: str | None
    reason: str | None
    finished_at: float | None
    observed_generation: int | None
    worker_incarnation: int | None
    restart_count: int
    conditions: dict[str, str]


@dataclass(frozen=True)
class CodePin:
    """One non-terminal row's code identity, for the keep set."""

    instance: str | None
    strategy_digest: str | None
    env_generation: int | None


@dataclass(frozen=True)
class StatusWrite:
    """The Status columns one commit stores."""

    session_id: str
    status: str
    reason: str | None
    finished_at: float | None
    observed_generation: int | None
    worker_incarnation: int | None
    conditions: dict[str, str]
    restart_count: int


class StatusStore(Protocol):
    """Load one row by session id, and write Status onto a row that exists."""

    async def load(self, session_id: str) -> StoredSession | None:
        """The row, or ``None`` when the API has not inserted it."""

    async def save(self, write: StatusWrite) -> None:
        """Update Status. A missing row is not created."""


def _timestamp(value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


def _conditions(raw: Mapping[str, object] | None) -> dict[str, str]:
    if not raw:
        return {}
    copied: dict[str, str] = {}
    for key, item in raw.items():
        if isinstance(key, str) and key and isinstance(item, str):
            copied[key] = item
    return copied


def _strategy_name(type_name: str | None, legacy: str | None) -> str:
    if type_name:
        return type_name
    if legacy:
        return legacy
    return "session"


class DbStatusStore:
    """:class:`StatusStore` over :class:`StsSessionRepository`."""

    def __init__(
        self,
        scope: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    ) -> None:
        self._scope = scope

    async def load(self, session_id: str) -> StoredSession | None:
        async with self._scope() as db:
            row = await StsSessionRepository(db).get_by_session_id(session_id)
            if row is None:
                return None
            created = _timestamp(row.created_at)
            return StoredSession(
                session_id=row.session_id,
                status=row.status,
                instance=row.instance,
                restart=row.restart,
                generation=row.generation,
                strategy_digest=row.strategy_digest,
                env_generation=row.env_generation,
                strategy=_strategy_name(row.type, row.legacy_strategy),
                created_by=row.created_by,
                created_at=0.0 if created is None else created,
                api_ids=tuple(row.td_api_ids),
                type_name=row.type,
                reason=row.reason,
                finished_at=_timestamp(row.finished_at),
                observed_generation=row.observed_generation,
                worker_incarnation=row.worker_incarnation,
                restart_count=row.restart_count,
                conditions=_conditions(row.conditions),
            )

    async def list_code_pins(self) -> tuple[CodePin, ...]:
        """Non-terminal rows. The caller filters by instance."""
        async with self._scope() as db:
            rows = await StsSessionRepository(db).list_nonterminal()
            return tuple(
                CodePin(
                    instance=row.instance,
                    strategy_digest=row.strategy_digest,
                    env_generation=row.env_generation,
                )
                for row in rows
            )

    async def save(self, write: StatusWrite) -> None:
        async with self._scope() as db:
            await StsSessionRepository(db).record_status(
                write.session_id,
                status=write.status,
                reason=write.reason,
                finished_at=write.finished_at,
                observed_generation=write.observed_generation,
                worker_incarnation=write.worker_incarnation,
                conditions=write.conditions,
                restart_count=write.restart_count,
            )

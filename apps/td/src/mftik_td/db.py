"""TD persistence helpers over mftik_db.td_sessions."""

from __future__ import annotations

from mftik_db.models.api import Api
from mftik_db.repositories import ApiRepository, TdSessionRepository
from mftik_db.session import session_scope


async def count_live_for_api(api_id: int) -> int:
    async with session_scope() as db:
        repo = TdSessionRepository(db)
        return await repo.count_live_for_api(api_id)


async def get_api(api_id: int) -> Api | None:
    """Load the venue credential row backing ``api_id``."""
    async with session_scope() as db:
        return await ApiRepository(db).get(api_id)

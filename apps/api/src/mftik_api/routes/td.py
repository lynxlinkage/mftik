"""TD session listing — a database read, not an RPC.

It used to go through the broker: the API sent `td.session.list` on the plane's
shared subject, some TD process picked it up, ran one query and sent the rows
back. That process holds no state this answer needs — `SessionManager.list_sessions`
consulted `td_sessions` and nothing else — so the round trip bought a
dependency on a plane being up in order to read a table the API is already
connected to.

Removing it is what lets TD stop serving an anycast subject at all: every other
thing that reaches TD carries an `api_id`, and an `api_id` resolves to the one
instance allowed to use that credential. See ``docs/Instances.md``.
"""

from __future__ import annotations

from fastapi import APIRouter
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import AccountRepository, TdSessionRepository
from mftik_db.session import session_scope

from mftik_api.schemas import SessionListResponse, SessionOut

router = APIRouter(prefix="/td", tags=["td"])

#: Mirrors the repository default. A scan that has to see every row must say
#: so, because the default silently truncates.
_LIST_LIMIT = 100


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(status: str | None = "live") -> SessionListResponse:
    async with session_scope() as db:
        rows = await TdSessionRepository(db).list_sessions(
            status=status, limit=_LIST_LIMIT
        )
        accounts = await AccountRepository(db).list_with_api()

    label_by_api: dict[int, tuple[str, str]] = {}
    for account in accounts:
        api = account.api
        if api is not None:
            label_by_api[api.id] = (api.venue, account.name)

    sessions: list[SessionOut] = []
    for row in rows:
        venue, api_name = label_by_api.get(row.api_id, (None, None))
        sessions.append(
            SessionOut(
                session_id=row.session_id,
                domain=SessionDomain.TD.value,
                created_by=row.created_by,
                created_at=(
                    row.created_at.timestamp() if row.created_at else 0.0
                ),
                finished_at=(
                    row.finished_at.timestamp() if row.finished_at else None
                ),
                status=row.status,
                api_id=row.api_id,
                sts_session_id=row.session_id,
                venue=venue,
                api_name=api_name,
            )
        )
    return SessionListResponse(sessions=sessions)

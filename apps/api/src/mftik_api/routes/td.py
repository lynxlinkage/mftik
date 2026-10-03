"""TD session listing — a database read, not an RPC.

It used to go through the broker: the API sent `td.session.list` on the plane's
shared subject, some TD process picked it up, ran one query and sent the rows
back. That process held no state this answer needs — the handler consulted
`td_sessions` and nothing else — so the round trip bought a dependency on a
plane being up in order to read a table the API is already connected to.

Removing it is what lets TD stop serving an anycast subject at all: every other
thing that reaches TD carries an `api_id`, and an `api_id` resolves to the one
instance allowed to use that credential. See ``docs/archive/Instances.md``.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mftik.protocol import (
    TD_ACCOUNT_DRAIN,
    Envelope,
    TdAccountDrain,
    TdAccountDrainResult,
    Topics,
)
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import AccountRepository, ApiRepository, TdSessionRepository
from mftik_db.session import session_scope
from pydantic import BaseModel

from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import BrokerDep
from mftik_api.schemas import SessionListResponse, SessionOut

router = APIRouter(prefix="/td", tags=["td"])

#: Mirrors the repository default. A scan that has to see every row must say
#: so, because the default silently truncates.
_LIST_LIMIT = 100

#: The worker waits up to 30s, then the controller stops it and starts
#: the next incarnation. This is longer than that sequence and shorter
#: than the CLI's wait, so a timeout is the API's answer.
_DRAIN_RPC_TIMEOUT_S = 45.0


class AccountDrainResponse(BaseModel):
    """What ``POST /td/accounts/{api_id}/drain`` returns.

    ``ok`` false is a finished refusal, not a transport failure. The
    CLI prints ``reason`` and exits non-zero. A missing account is 404
    before this body. No TD answering the instance is 503.
    """

    api_id: int
    ok: bool
    incarnation: int | None = None
    reason: str = ""


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


@router.post("/accounts/{api_id}/drain", response_model=AccountDrainResponse)
async def drain_account(api_id: int, broker: BrokerDep) -> AccountDrainResponse:
    """Drain-replace one account on the TD instance that holds it (F27).

    Same auth as the other operator routes: the middleware default-denies,
    and this path is not public. An unknown ``api_id`` is 404. Every
    credential row names an instance, so there is no separate unbound
    state. The TD process refuses an id it does not serve with
    ``ok`` false and ``reason`` ``not_bound``.
    """
    if api_id <= 0:
        raise HTTPException(status_code=400, detail="api_id must be a positive int")
    async with session_scope() as db:
        instance = await ApiRepository(db).instance_name(api_id)
    if instance is None:
        raise HTTPException(status_code=404, detail=f"unknown api_id {api_id}")
    try:
        result = await request_domain(
            broker,
            Topics.td(instance),
            Envelope[TdAccountDrain].wrap(
                TdAccountDrain(api_id=api_id),
                type=TD_ACCOUNT_DRAIN,
                source="api",
            ),
            result_type=TdAccountDrainResult,
            timeout=_DRAIN_RPC_TIMEOUT_S,
        )
    except DomainRpcError as exc:
        status = 503 if exc.no_responders else 502
        raise HTTPException(status_code=status, detail=exc.message) from exc
    return AccountDrainResponse(
        api_id=result.api_id,
        ok=result.ok,
        incarnation=result.incarnation,
        reason=result.reason,
    )

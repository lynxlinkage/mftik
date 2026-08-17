"""Venue API credentials + 1-1 trading accounts (list / create / rename / delete)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mftik.exchange import venues
from mftik_db.models.api import Api, ApiType
from mftik_db.repositories import (
    AccountRepository,
    ApiRepository,
    TdSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.audit_util import record_audit
from mftik_api.auth import OwnerId
from mftik_api.deps import DEFAULT_USER_ID
from mftik_api.schemas import (
    ApiCreateBody,
    ApiDeleteResponse,
    ApiListResponse,
    ApiOut,
    ApiRenameBody,
    VenueListResponse,
    VenueOut,
)

router = APIRouter(tags=["apis"])

_ALLOWED_TYPES = {ApiType.HMAC.value, ApiType.ED25519.value}


def _venue_out(venue: venues.Venue) -> VenueOut:
    return VenueOut(
        name=venue.name,
        label=venue.label,
        categories=sorted(c.value for c in venue.categories),
        api_types=sorted(venue.api_types),
        requires_passphrase=venue.requires_passphrase,
        simulated=venue.simulated,
        ticker_example=venue.ticker_example,
    )


@router.get("/venues", response_model=VenueListResponse)
async def list_venues() -> VenueListResponse:
    """Venues a credential can be registered against."""
    return VenueListResponse(venues=[_venue_out(v) for v in venues.all_venues()])


def _to_out(*, account_id: int, name: str, api: Api) -> ApiOut:
    return ApiOut(
        id=api.id,
        account_id=account_id,
        name=name,
        venue=api.venue,
        api_key=api.api_key,
        type=api.type,
        created_at=api.created_at.timestamp() if api.created_at else 0.0,
        created_by=api.owner_id,
    )


@router.get("/apis", response_model=ApiListResponse)
async def list_apis() -> ApiListResponse:
    async with session_scope() as db:
        accounts = await AccountRepository(db).list_with_api()
    out: list[ApiOut] = []
    for account in accounts:
        api = account.api
        if api is None:
            continue
        out.append(
            _to_out(account_id=account.id, name=account.name, api=api)
        )
    return ApiListResponse(apis=out)


@router.post("/apis", response_model=ApiOut, status_code=201)
async def create_api(
    body: ApiCreateBody, owner: OwnerId = DEFAULT_USER_ID
) -> ApiOut:
    api_type = body.type.strip().upper()
    if api_type not in _ALLOWED_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"type must be one of {sorted(_ALLOWED_TYPES)}",
        )

    created_by = body.created_by if body.created_by is not None else owner
    name = body.name.strip()
    api_key = body.api_key.strip()
    if not name or not body.venue.strip() or not api_key:
        raise HTTPException(
            status_code=400, detail="name, venue, and api_key are required"
        )

    # Resolve against the registry so the row stores the canonical spelling and
    # an unusable venue is rejected here rather than at deploy time.
    try:
        resolved = venues.validate_credential(body.venue, api_type)
    except (venues.UnknownVenueError, venues.UnsupportedApiTypeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    venue = resolved.name
    if resolved.requires_passphrase and not body.passphrase:
        raise HTTPException(
            status_code=400,
            detail=f"venue {venue!r} requires a passphrase",
        )

    async with session_scope() as db:
        apis = ApiRepository(db)
        accounts = AccountRepository(db)

        existing = await apis.get_by_api_key(api_key)
        if existing is not None:
            raise HTTPException(
                status_code=409, detail=f"api_key already exists: {api_key}"
            )
        if await accounts.get_by_name(name) is not None:
            raise HTTPException(
                status_code=409, detail=f"account name already exists: {name}"
            )

        api = await apis.add(
            Api(
                owner_id=created_by,
                venue=venue,
                api_key=api_key,
                api_secret=body.api_secret,
                type=api_type,
                passphrase=body.passphrase,
            )
        )
        account = await accounts.create(
            name=name,
            api_id=api.id,
            created_by=created_by,
        )
        result = _to_out(account_id=account.id, name=account.name, api=api)

    await record_audit(
        user_id=created_by,
        operation="api.create",
        result=(
            f"api_id={result.id} account_id={result.account_id} "
            f"venue={result.venue} name={result.name}"
        ),
    )
    return result


@router.patch("/apis/{api_id}", response_model=ApiOut)
async def rename_api(api_id: int, body: ApiRenameBody) -> ApiOut:
    """Rename the trading account bound 1-1 to this credential.

    strategy.yml ``td`` entries resolve by account name at deploy time, so a
    rename only affects future deploys — live sessions stay keyed by api_id.
    """
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    async with session_scope() as db:
        apis = ApiRepository(db)
        accounts = AccountRepository(db)

        api = await apis.get(api_id)
        if api is None:
            raise HTTPException(status_code=404, detail=f"api not found: {api_id}")

        account = await accounts.get_by_api_id(api_id)
        if account is None:
            raise HTTPException(
                status_code=404, detail=f"account not found for api_id={api_id}"
            )

        if account.name != name:
            conflict = await accounts.get_by_name(name)
            if conflict is not None:
                raise HTTPException(
                    status_code=409, detail=f"account name already exists: {name}"
                )
            old_name = account.name
            account.name = name
            await db.flush()
        else:
            old_name = name

        owner_id = api.owner_id
        result = _to_out(account_id=account.id, name=account.name, api=api)

    if old_name != result.name:
        await record_audit(
            user_id=owner_id,
            operation="api.rename",
            result=(
                f"api_id={result.id} account_id={result.account_id} "
                f"{old_name!r} -> {result.name!r}"
            ),
        )
    return result


@router.delete("/apis/{api_id}", response_model=ApiDeleteResponse)
async def delete_api(api_id: int) -> ApiDeleteResponse:
    async with session_scope() as db:
        apis = ApiRepository(db)
        accounts = AccountRepository(db)
        td = TdSessionRepository(db)

        api = await apis.get(api_id)
        if api is None:
            raise HTTPException(status_code=404, detail=f"api not found: {api_id}")

        account = await accounts.get_by_api_id(api_id)
        if account is None:
            raise HTTPException(
                status_code=404, detail=f"account not found for api_id={api_id}"
            )

        live = await td.count_live_for_api(api_id)
        if live > 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"api_id={api_id} has {live} live TD session(s); "
                    "stop them before deleting"
                ),
            )

        account_id = account.id
        owner_id = api.owner_id
        # DB FK accounts.api_id ON DELETE CASCADE removes the account row.
        await apis.delete(api)

    await record_audit(
        user_id=owner_id,
        operation="api.delete",
        result=f"api_id={api_id} account_id={account_id}",
    )
    return ApiDeleteResponse(id=api_id, account_id=account_id, deleted=True)

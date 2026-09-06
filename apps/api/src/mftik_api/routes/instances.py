"""Declared plane instances — list / declare / annotate / retire.

Declaring a row does not start anything. The node checks and does not
guarantee: a row nobody deployed reads *down* on Home forever rather than
provoking the node into fixing it, and making it true is operations work done
wherever the compose file lives. See ``docs/Instances.md``.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from mftik_db.models.instance import Instance
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import InstanceRepository
from mftik_db.session import session_scope
from sqlalchemy.exc import IntegrityError

from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, PrincipalDep
from mftik_api.deps import DEFAULT_USER_ID
from mftik_api.schemas import (
    InstanceCreateBody,
    InstanceDeleteResponse,
    InstanceListResponse,
    InstanceOut,
    InstanceUpdateBody,
)

router = APIRouter(prefix="/instances", tags=["instances"])

#: Planes that can have more than one process. Not ``sym`` — it is off the hot
#: path behind ``SymbolClient``'s cache — and not ``paper``, whose whole point
#: is one shared book.
_INSTANCED = frozenset(
    {SessionDomain.TD.value, SessionDomain.MD.value, SessionDomain.STS.value}
)


def _to_out(row: Instance) -> InstanceOut:
    return InstanceOut(
        id=row.id,
        name=row.name,
        domain=row.domain,
        region=row.region,
        enabled=row.enabled,
        created_at=row.created_at.timestamp() if row.created_at else 0.0,
        created_by=row.created_by,
    )


@router.get("", response_model=InstanceListResponse)
async def list_instances(domain: str | None = None) -> InstanceListResponse:
    """Every declared instance. Whether one answers is `/stats`'s question."""
    async with session_scope() as db:
        rows = await InstanceRepository(db).list_all(domain=domain)
        return InstanceListResponse(instances=[_to_out(r) for r in rows])


@router.post("", response_model=InstanceOut, status_code=201)
async def create_instance(
    body: InstanceCreateBody,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> InstanceOut:
    created_by = owner
    name = body.name.strip()
    domain = body.domain.strip().lower()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if domain not in _INSTANCED:
        raise HTTPException(
            status_code=400,
            detail=(
                f"domain must be one of {sorted(_INSTANCED)}, got {domain!r} "
                "— sym and paper are not instanced"
            ),
        )

    async with session_scope() as db:
        repo = InstanceRepository(db)
        if await repo.get_by_name(name) is not None:
            raise HTTPException(
                status_code=409, detail=f"instance already exists: {name}"
            )
        row = await repo.create(
            name=name,
            domain=domain,
            region=(body.region or None),
            created_by=created_by,
        )
        result = _to_out(row)

    await record_audit(
        user_id=created_by,
        operation="instance.create",
        result=f"id={result.id} name={result.name} domain={result.domain}",
        principal=principal,
    )
    return result


@router.patch("/{instance_id}", response_model=InstanceOut)
async def update_instance(
    instance_id: int,
    body: InstanceUpdateBody,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> InstanceOut:
    """Edit ``region`` and ``enabled``. There is no rename — see the schema."""
    created_by = owner
    async with session_scope() as db:
        repo = InstanceRepository(db)
        row = await repo.get(instance_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"unknown instance: {instance_id}"
            )
        row = await repo.update(
            row, region=body.region, enabled=body.enabled
        )
        result = _to_out(row)

    await record_audit(
        user_id=created_by,
        operation="instance.update",
        result=(
            f"id={result.id} name={result.name} "
            f"region={result.region} enabled={result.enabled}"
        ),
        principal=principal,
    )
    return result


@router.delete("/{instance_id}", response_model=InstanceDeleteResponse)
async def delete_instance(
    instance_id: int,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> InstanceDeleteResponse:
    """Retire an instance.

    Refused on two counts, and they are enforced in different places because
    they are different facts.

    A **credential** still naming it is refused by the database:
    ``apis.instance_id`` is ``ON DELETE RESTRICT``.

    A **live session** still naming it is refused here, because nothing else
    can. ``md_sessions.instance`` and ``sts_sessions.instance`` are plain
    strings on purpose — they are history, and retiring an instance must not
    break the record of what it did. History is exactly what must not block a
    delete; a session that is still running is not history. Retiring the
    instance a live run is pinned to would leave that run unable to rebuild,
    and it would only be discovered at the next restart.

    Blocked rather than warned. The foreign key above already blocks, so
    warning here would make one kind of reference refuse and another shrug,
    and the operator has an obvious way forward either way: stop the session,
    or wait for it to end.
    """
    created_by = owner
    async with session_scope() as db:
        repo = InstanceRepository(db)
        row = await repo.get(instance_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"unknown instance: {instance_id}"
            )
        name = row.name
        live = await repo.live_sessions_naming(row)
        if live:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"instance {name!r} still has {live} live "
                    f"{row.domain} session(s) — stop them, or wait for them "
                    f"to end, before retiring it"
                ),
            )
        try:
            await repo.delete(row)
        except IntegrityError as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"instance {name!r} is still named by a credential — "
                    "move those apis to another instance first"
                ),
            ) from exc

    await record_audit(
        user_id=created_by,
        operation="instance.delete",
        result=f"id={instance_id} name={name}",
        principal=principal,
    )
    return InstanceDeleteResponse(id=instance_id)

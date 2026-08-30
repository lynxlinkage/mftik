"""STS HTTP facade — strategy.yml deploy + list/control."""

from __future__ import annotations

import base64
import logging
import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from mftik.environment import NodeEnv
from mftik.protocol import (
    DEFAULT_STRATEGY_TYPE,
    STS_EVENTLOG_INFO,
    STS_EVENTLOG_READ,
    STS_SESSION_LIST,
    STS_SESSION_STATUS,
    STS_SESSION_STOP,
    ListSessionsRequest,
    ListSessionsRequestEnvelope,
    ListSessionsResult,
    StrategyTemplate,
    StrategyYamlError,
    StsEventLogChunk,
    StsEventLogInfo,
    StsEventLogInfoRequest,
    StsEventLogInfoRequestEnvelope,
    StsEventLogReadRequest,
    StsEventLogReadRequestEnvelope,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    StsSessionStatus,
    StsSessionStatusEnvelope,
    TdAccountRef,
    TdSettings,
    Topics,
    all_templates,
    attached_api_ids,
    default_template,
    get_template,
    parse_strategy_yml,
)
from mftik.registry import AddedStrategy, RegistryStore, qualify
from mftik_db.models.session import SessionStatus, StsSessionRow
from mftik_db.repositories import (
    AccountRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, Principal, PrincipalDep
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import DEFAULT_USER_ID, BrokerDep, RegistryStoreDep
from mftik_api.orchestrate import deploy_strategy
from mftik_api.schemas import (
    DeployResponse,
    EventLogInfoResponse,
    SessionListResponse,
    SessionOut,
    StrategyDeployBody,
    StrategyListResponse,
    StrategyOut,
    StrategyTemplateOut,
    StrategyTypesResponse,
    StrategyYamlResponse,
    StsControlResponse,
    TdAttachOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sts", tags=["sts"])

#: Replay buffer sizing matches STS manager — cover the gap between a
#: page loading and its socket being live, not history.
_STATUS_BUFFER = 200
_STATUS_TTL_SECONDS = 3600
_ACKABLE = frozenset(
    {SessionStatus.FAILED.value, SessionStatus.INTERRUPTED.value}
)

#: Bytes of log requested per RPC. Small enough that STS answers between two
#: session controls and Redis holds one slice, large enough that a 100 MB log
#: is a few hundred round trips rather than tens of thousands.
_EVENTLOG_CHUNK_BYTES = 262_144
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")

#: A registry tree does not have to ship a deploy document. The picker still
#: needs *some* yaml, and an empty ``sts:`` is a valid starting point — the
#: strategy's ``on_initialized`` is what refuses a bad config after that.
_LOCAL_YAML = "sts: {}\n"


_KNOWN_STATUSES = frozenset(item.value for item in SessionStatus)


def _parse_statuses(status: str | None) -> str | list[str] | None:
    """``done,ack`` is History; a single value is Live. Same shape as Board.

    Omitted (or blank) means every status. A value that is only commas,
    or a token that is not a status, is 422 — an empty page would read
    as the end of history.
    """
    if status is None or not status.strip():
        return None
    parts = [part.strip() for part in status.split(",") if part.strip()]
    if not parts:
        raise HTTPException(status_code=422, detail="status has no values")
    unknown = [part for part in parts if part not in _KNOWN_STATUSES]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"unknown status: {', '.join(unknown)}",
        )
    if len(parts) == 1:
        return parts[0]
    return parts


def _registry_template(
    rec: AddedStrategy,
    store: RegistryStore,
    applied: frozenset[str] | None = None,
) -> StrategyTemplate:
    key = qualify(rec.origin, rec.type)
    requires = list(rec.requires)
    if applied is None:
        applied = NodeEnv.from_env().extras_names()
    return StrategyTemplate(
        type=key,
        label=key,
        description=f"{rec.origin} registry ({rec.digest})",
        yaml=store.read_template(rec) or _LOCAL_YAML,
        source="registry",
        requires=requires,
        env_ok=set(requires) <= applied,
    )


def _deployable_templates(store: RegistryStore) -> list[StrategyTemplate]:
    bundled = list(all_templates())
    bundled_types = {t.type for t in bundled}
    seen = set(bundled_types)
    extra: list[StrategyTemplate] = []
    # One read of the stamp for the whole listing — every row judges its
    # ``requires`` against the same applied set.
    applied = NodeEnv.from_env().extras_names()
    for rec in store.list_all():
        if rec.type in bundled_types:
            continue
        key = qualify(rec.origin, rec.type)
        if key in seen:
            continue
        extra.append(_registry_template(rec, store, applied))
        seen.add(key)
    extra.sort(key=lambda t: t.type)
    return bundled + extra


def _deployable_template(
    strategy_type: str, store: RegistryStore
) -> StrategyTemplate | None:
    template = get_template(strategy_type)
    if template is not None:
        return template
    for rec in store.list_all():
        if qualify(rec.origin, rec.type) == strategy_type:
            return _registry_template(rec, store)
    return None


@router.get("/template")
async def strategy_template() -> dict[str, str]:
    """Template for the default strategy — the editor's starting document."""
    return {"yaml": default_template().yaml}


@router.get("/types", response_model=StrategyTypesResponse)
async def list_strategy_types(
    store: RegistryStoreDep,
) -> StrategyTypesResponse:
    """Deployable strategies, with the template each one starts from."""
    templates = _deployable_templates(store)
    return StrategyTypesResponse(
        types=[t.type for t in templates],
        templates=[
            StrategyTemplateOut.model_validate(t.model_dump())
            for t in templates
        ],
        default=DEFAULT_STRATEGY_TYPE,
    )


@router.get("/types/{strategy_type}/template", response_model=StrategyTemplateOut)
async def strategy_type_template(
    strategy_type: str, store: RegistryStoreDep
) -> StrategyTemplateOut:
    """The starting document for one strategy type."""
    template = _deployable_template(strategy_type, store)
    if template is None:
        known = ", ".join(t.type for t in _deployable_templates(store))
        raise HTTPException(
            status_code=404,
            detail=f"unknown strategy type: {strategy_type}; known: {known}",
        )
    return StrategyTemplateOut.model_validate(template.model_dump())


@router.get("/strategies", response_model=StrategyListResponse)
async def list_strategies(
    status: str | None = None,
    before: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> StrategyListResponse:
    """List STS sessions as deploys, including ones that failed at attach.

    ``status`` is a comma union (``done,ack`` is History). ``before`` is the
    last ``session_id`` of the previous page. An unknown cursor is 422 —
    the row may have gone with its user, and an empty page would read as
    the end of history.
    """
    parsed = _parse_statuses(status)
    fetch = limit + 1
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        if before is not None:
            if await repo.get_by_session_id(before) is None:
                raise HTTPException(
                    status_code=422, detail=f"unknown cursor: {before}"
                )
        rows = await repo.list_sessions(
            status=parsed, before_session=before, limit=fetch
        )

    has_more = len(rows) > limit
    page = rows[:limit]
    return StrategyListResponse(
        strategies=[_strategy_out(row) for row in page],
        has_more=has_more,
    )


def _strategy_out(row: StsSessionRow) -> StrategyOut:
    """Map a ``sts_sessions`` row to the list/detail shape.

    ``td_api_ids`` and ``md_ids`` come from this row, not from TD/MD RPC —
    the page that shows a deploy's attaches must still load when those
    processes are silent.
    """
    return StrategyOut(
        type=row.type,
        config=dict(row.st_paras or {}),
        created_by=row.created_by,
        created_at=row.created_at.timestamp() if row.created_at else 0.0,
        session_id=row.session_id,
        status=row.status,
        reason=row.reason,
        td_api_ids=attached_api_ids(row),
        md_ids=[str(v) for v in (row.md_ids or [])],
    )


@router.get("/sessions/{session_id}", response_model=StrategyOut)
async def get_strategy(session_id: str) -> StrategyOut:
    """One STS session from the database, including the TD/MD it attached.

    Reads ``sts_sessions`` directly. ``GET /sts/sessions`` still asks the
    STS process; this one must not — the strategy page shows attaches
    whether or not STS, TD, or MD are answering.
    """
    async with session_scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(session_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"session not found: {session_id}"
            )
        return _strategy_out(row)


@router.get("/sessions/{session_id}/yaml", response_model=StrategyYamlResponse)
async def strategy_yaml(session_id: str) -> StrategyYamlResponse:
    """The strategy.yml behind a past deploy.

    Served verbatim from what was submitted. Deploys that never stored a
    document — they ended before the text was kept — have nothing to serve.
    """
    async with session_scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(session_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"session not found: {session_id}"
            )

        if row.yaml_text:
            return StrategyYamlResponse(
                type=row.type,
                session_id=row.session_id,
                yaml=row.yaml_text,
            )

    raise HTTPException(
        status_code=404,
        detail=(
            f"session {session_id} has no stored strategy.yml — "
            "this deploy predates document storage"
        ),
    )


@router.get("/sessions", response_model=SessionListResponse)
async def list_sessions(
    broker: BrokerDep, status: str | None = "live"
) -> SessionListResponse:
    try:
        result = await request_domain(
            broker,
            Topics.STS,
            ListSessionsRequestEnvelope.wrap(
                ListSessionsRequest(domain="sts", status=status),
                type=STS_SESSION_LIST,
                source="api",
            ),
            result_type=ListSessionsResult,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc
    return SessionListResponse(
        sessions=[SessionOut.model_validate(s.model_dump()) for s in result.sessions]
    )


@router.post("/sessions/{session_id}/stop", response_model=StsControlResponse)
async def stop_session(
    session_id: str,
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> StsControlResponse:
    return await _control(
        broker,
        session_id,
        STS_SESSION_STOP,
        "sts.session.stop",
        owner,
        principal,
    )


def _epoch(value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


@router.post("/sessions/{session_id}/ack", response_model=StsControlResponse)
async def ack_session(
    session_id: str,
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> StsControlResponse:
    """Mark a failed or interrupted session as acknowledged — a normal stop.

    The process is already gone, so this is a database write rather than an
    STS RPC. The original reason stays so the badge still explains why it
    ended; only the status changes.
    """
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        row = await repo.get_by_session_id(session_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"no session {session_id}"
            )
        if row.status not in _ACKABLE:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"session {session_id} is {row.status}, "
                    "not failed or interrupted"
                ),
            )
        row = await repo.mark_ack(session_id)
        if row is None:
            raise HTTPException(
                status_code=409,
                detail=f"session {session_id} is not failed or interrupted",
            )
        snapshot = (
            row.session_id,
            row.status,
            row.strategy,
            row.reason,
            row.created_by,
            _epoch(row.finished_at),
            row.type,
        )

    session_id_, status, strategy, reason, created_by, finished_at, type_ = (
        snapshot
    )
    envelope = StsSessionStatusEnvelope.wrap(
        StsSessionStatus(
            session_id=session_id_,
            status=status,
            strategy=strategy,
            reason=reason,
            created_by=created_by,
            finished_at=finished_at,
            type=type_,
        ),
        type=STS_SESSION_STATUS,
        source="api",
        session_id=session_id_,
    )
    try:
        await broker.publish_log(
            Topics.status_sts(),
            envelope,
            maxlen=_STATUS_BUFFER,
            ttl_seconds=_STATUS_TTL_SECONDS,
        )
    except Exception:
        logger.exception("STS ack status publish failed session=%s", session_id)

    await record_audit(
        user_id=owner,
        operation="sts.session.ack",
        result=f"session_id={session_id_} status={status}",
        principal=principal,
    )
    return StsControlResponse(
        session_id=session_id_,
        status=status,
        strategy=strategy,
        reason=reason,
    )


@router.get(
    "/sessions/{session_id}/eventlog/info", response_model=EventLogInfoResponse
)
async def eventlog_info(
    session_id: str, broker: BrokerDep
) -> EventLogInfoResponse:
    """What event log STS holds for this session, if any.

    Its own endpoint so the UI can decide whether to offer a download, and how
    large a one, before committing a user to it.
    """
    info = await _eventlog_info(broker, session_id)
    return EventLogInfoResponse(
        session_id=info.session_id,
        available=info.available,
        enabled=info.enabled,
        parts=len(info.parts),
        total_bytes=info.total_bytes,
        live=info.live,
    )


@router.get("/sessions/{session_id}/eventlog")
async def download_eventlog(
    session_id: str,
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> StreamingResponse:
    """Stream the session's event log as one gzip, oldest part first.

    Pulled from STS a slice at a time and passed straight through: each slice
    arrives as its own gzip member, and a concatenation of gzip members is a
    gzip. So neither this process nor the broker ever holds the whole file, and
    nothing here has to decompress what it is only forwarding.
    """
    info = await _eventlog_info(broker, session_id)
    if not info.available:
        detail = (
            "STS is not keeping event logs (STS_EVENTLOG_DIR unset)"
            if not info.enabled
            else f"no event log for session {session_id!r} on the STS that answered"
        )
        raise HTTPException(status_code=404, detail=detail)

    await record_audit(
        user_id=owner,
        operation="sts.eventlog.download",
        result=f"session_id={session_id} bytes={info.total_bytes}",
        principal=principal,
    )
    name = f"{_safe_name(session_id)}.jsonl.gz"
    return StreamingResponse(
        _eventlog_chunks(broker, session_id, info),
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


async def _eventlog_info(broker: BrokerDep, session_id: str) -> StsEventLogInfo:
    try:
        return await request_domain(
            broker,
            Topics.STS,
            StsEventLogInfoRequestEnvelope.wrap(
                StsEventLogInfoRequest(session_id=session_id),
                type=STS_EVENTLOG_INFO,
                source="api",
                session_id=session_id,
            ),
            result_type=StsEventLogInfo,
            timeout=10.0,
        )
    except DomainRpcError as exc:
        raise HTTPException(status_code=502, detail=exc.message) from exc


async def _eventlog_chunks(
    broker: BrokerDep, session_id: str, info: StsEventLogInfo
) -> AsyncIterator[bytes]:
    """Yield every part's slices in order, stopping at the first failure.

    A failure mid-stream cannot become a status code — the headers went out
    with the first chunk — so it ends the response and says so in the process
    log. A truncated ``.gz`` is at least detectable as truncated, which a
    silently short one would not be.
    """
    for part in info.parts:
        offset = 0
        while True:
            try:
                chunk = await request_domain(
                    broker,
                    Topics.STS,
                    StsEventLogReadRequestEnvelope.wrap(
                        StsEventLogReadRequest(
                            session_id=session_id,
                            part=part.name,
                            offset=offset,
                            length=_EVENTLOG_CHUNK_BYTES,
                        ),
                        type=STS_EVENTLOG_READ,
                        source="api",
                        session_id=session_id,
                    ),
                    result_type=StsEventLogChunk,
                    timeout=15.0,
                )
            except DomainRpcError as exc:
                logger.error(
                    "sts eventlog download failed session=%s part=%s "
                    "offset=%d: %s",
                    session_id,
                    part.name,
                    offset,
                    exc.message,
                )
                return
            if chunk.data:
                yield base64.b64decode(chunk.data)
            offset += chunk.raw_bytes
            if chunk.eof:
                break
            if chunk.raw_bytes == 0:
                # Not eof and nothing read: the file is being written but has
                # nothing new yet. Stopping beats spinning on a live session.
                break


def _safe_name(value: str) -> str:
    cleaned = _UNSAFE_NAME.sub("_", value.strip()).strip("._")
    return cleaned or "session"


@router.post("/deploy/{strategy_type}", response_model=DeployResponse)
async def deploy(
    strategy_type: str,
    body: StrategyDeployBody,
    broker: BrokerDep,
    store: RegistryStoreDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> DeployResponse:
    """Deploy ``strategy_type`` with the td / md / sts document in ``body``.

    The type is in the path rather than the document because it decides what
    ``sts:`` is allowed to contain — keeping them together let a user edit one
    into disagreement with the other.
    """
    if _deployable_template(strategy_type, store) is None:
        known = ", ".join(t.type for t in _deployable_templates(store))
        raise HTTPException(
            status_code=404,
            detail=f"unknown strategy type: {strategy_type}; known: {known}",
        )
    try:
        spec = parse_strategy_yml(body.yaml)
    except StrategyYamlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    created_by = body.created_by if body.created_by is not None else owner
    td = await _resolve_td(spec.td)
    try:
        result = await deploy_strategy(
            broker,
            strategy_id=strategy_type,
            td=td,
            md=list(spec.md),
            st_paras=dict(spec.sts),
            created_by=created_by,
            timeout=body.timeout,
            restart=spec.restart,
            strategy_type=strategy_type,
            yaml_text=body.yaml,
        )
    except DomainRpcError as exc:
        code = 404 if exc.code in {"unknown_strategy", "not_found"} else 502
        if exc.code == "timeout":
            code = 504
        if exc.code == "incompatible_environment":
            code = 409
        if exc.code == "strategy_refused":
            # The document is wrong, not the platform. Nothing upstream failed
            # and retrying will do the same thing, which is what separates this
            # from the 502 and 504 beside it.
            code = 400
        raise HTTPException(status_code=code, detail=str(exc)) from exc

    session_id = result["session_id"]
    await record_audit(
        user_id=created_by,
        operation="sts.deploy",
        result=(
            f"session_id={session_id} type={strategy_type} "
            f"td_names={list(spec.td)} "
            f"td={[a['api_id'] for a in result['td']]} md={result['md']}"
        ),
        principal=principal,
    )
    return DeployResponse(
        session_id=session_id,
        type=strategy_type,
        config=dict(spec.sts),
        td=[TdAttachOut(**a) for a in result["td"]],
        md=result["md"],
        status=result["status"],
    )


async def _resolve_td(
    accounts: dict[str, TdSettings],
) -> dict[str, TdAccountRef]:
    """Map strategy.yml account names → resolved attaches."""
    if not accounts:
        return {}
    async with session_scope() as db:
        repo = AccountRepository(db)
        out: dict[str, TdAccountRef] = {}
        for name, settings in accounts.items():
            account = await repo.get_by_name(name)
            if account is None or account.api is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"unknown td account name: {name!r}",
                )
            out[name] = TdAccountRef(api_id=account.api_id, settings=settings)
        return out


async def _control(
    broker: BrokerDep,
    session_id: str,
    type_name: str,
    audit_op: str,
    owner: int,
    principal: Principal | None = None,
) -> StsControlResponse:
    try:
        result = await request_domain(
            broker,
            Topics.STS,
            StsSessionControlRequestEnvelope.wrap(
                StsSessionControlRequest(session_id=session_id),
                type=type_name,
                source="api",
                session_id=session_id,
            ),
            result_type=StsSessionControlResult,
            timeout=10.0,
        )
    except DomainRpcError as exc:
        code = 404 if exc.code == "not_found" else 502
        raise HTTPException(status_code=code, detail=exc.message) from exc

    await record_audit(
        user_id=owner,
        operation=audit_op,
        result=f"session_id={result.session_id} status={result.status}",
        principal=principal,
    )
    return StsControlResponse.model_validate(result.model_dump())

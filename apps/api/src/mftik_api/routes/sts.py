"""STS HTTP facade — strategy.yml deploy + list/control."""

from __future__ import annotations

import asyncio
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
    STS_SESSION_STATUS,
    StrategySpec,
    StrategyTemplate,
    StrategyYamlError,
    StsEventLogChunk,
    StsEventLogInfo,
    StsEventLogInfoRequest,
    StsEventLogInfoRequestEnvelope,
    StsEventLogPart,
    StsEventLogReadRequest,
    StsEventLogReadRequestEnvelope,
    StsSessionStatus,
    StsSessionStatusEnvelope,
    Topics,
    all_templates,
    attached_api_ids,
    default_template,
    get_template,
    md_feeds_of,
    parse_strategy_yml,
)
from mftik.registry import AddedStrategy, RegistryStore, qualify
from mftik_db.models.session import SessionDomain, SessionStatus, StsSessionRow
from mftik_db.repositories import (
    InstanceRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, PrincipalDep
from mftik_api.broker_rpc import DomainRpcError, request_domain
from mftik_api.deps import DEFAULT_USER_ID, BrokerDep, RegistryStoreDep
from mftik_api.orchestrate import start
from mftik_api.paging import ListOffset
from mftik_api.schemas import (
    DeployResponse,
    EventLogInfoResponse,
    SessionListResponse,
    StrategyDeployBody,
    StrategyListResponse,
    StrategyOut,
    StrategyTemplateOut,
    StrategyTypesResponse,
    StrategyYamlResponse,
    StsControlResponse,
)
from mftik_api.sts_fanout import registry_availability

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sts", tags=["sts"])

_ACKABLE = frozenset(
    {SessionStatus.FAILED.value, SessionStatus.INTERRUPTED.value}
)

#: Bytes of log requested per RPC. Small enough that STS answers between two
#: session controls and the broker holds one slice, large enough that a 100 MB log
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
    instances: list[str] | None = None,
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
        instances=instances,
    )


def _deployable_templates(
    store: RegistryStore,
    availability: dict[str, frozenset[str]] | None = None,
) -> list[StrategyTemplate]:
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
        if availability is None:
            instances = None
        else:
            instances = sorted(
                label for label, keys in availability.items() if key in keys
            )
            # On disk at the API, loaded by nobody. Not a deployable type.
            if not instances:
                continue
        extra.append(_registry_template(rec, store, applied, instances))
        seen.add(key)
    extra.sort(key=lambda t: t.type)
    return bundled + extra


def _deployable_template(
    strategy_type: str,
    store: RegistryStore,
    availability: dict[str, frozenset[str]] | None = None,
) -> StrategyTemplate | None:
    template = get_template(strategy_type)
    if template is not None:
        return template
    for candidate in _deployable_templates(store, availability):
        if candidate.type == strategy_type:
            return candidate
    return None


@router.get("/template")
async def strategy_template() -> dict[str, str]:
    """Template for the default strategy — the editor's starting document."""
    return {"yaml": default_template().yaml}


@router.get("/types", response_model=StrategyTypesResponse)
async def list_strategy_types(
    store: RegistryStoreDep,
    broker: BrokerDep,
) -> StrategyTypesResponse:
    """Deployable strategies, with the template each one starts from.

    A registry type is listed only when at least one enabled STS has it
    loaded, and ``instances`` says which. No answer from any STS leaves the
    API store's listing in place — a census miss is not "nothing is
    deployable".
    """
    templates = _deployable_templates(store, await registry_availability(broker))
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
    strategy_type: str, store: RegistryStoreDep, broker: BrokerDep
) -> StrategyTemplateOut:
    """The starting document for one strategy type."""
    availability = await registry_availability(broker)
    template = _deployable_template(strategy_type, store, availability)
    if template is None:
        known = ", ".join(
            t.type for t in _deployable_templates(store, availability)
        )
        raise HTTPException(
            status_code=404,
            detail=f"unknown strategy type: {strategy_type}; known: {known}",
        )
    return StrategyTemplateOut.model_validate(template.model_dump())


@router.get("/strategies", response_model=StrategyListResponse)
async def list_strategies(
    status: str | None = None,
    offset: ListOffset = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> StrategyListResponse:
    """List STS sessions as deploys, including ones that failed at attach.

    ``status`` is a comma union (``done,ack`` is History). ``offset`` /
    ``limit`` page a numbered browse. ``total`` is the match count before
    paging.
    """
    parsed = _parse_statuses(status)
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        total = await repo.count_sessions(status=parsed)
        rows = await repo.list_sessions(
            status=parsed, offset=offset, limit=limit
        )

    return StrategyListResponse(
        strategies=[_strategy_out(row) for row in rows],
        total=total,
        has_more=offset + len(rows) < total,
    )


def _strategy_out(row: StsSessionRow) -> StrategyOut:
    """Map a ``sts_sessions`` row to the list/detail shape.

    ``td_api_ids`` and ``md_ids`` come from this row, not from TD/MD RPC —
    the page that shows a deploy's attaches must still load when those
    processes are silent.

    Both go through a reader rather than being iterated: the columns hold
    mappings now, and iterating one yields its keys. ``attached_api_ids`` has
    done that for ``td`` since it stopped being a list; ``md_feeds_of`` is the
    same job for ``md_ids``.
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
        md_ids=md_feeds_of(row.md_ids),
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
    """Placeholder until IF-04 (#182) gives STS sessions to list again.

    RM-04 (#167) deleted STS's session manager, so ``sts.session.list`` now
    reaches a handler that raises — the request goes unanswered and the wait
    turns into a 502 that reads as a broken plane rather than as a route
    with nothing behind it. ``GET /sts/strategies`` still answers: it reads
    ``sts_sessions`` and does not ask a process.
    """
    del broker, status
    raise HTTPException(
        status_code=501,
        detail="sts session listing is not implemented — waiting for IF-04 "
        "(#182)",
    )


@router.post("/sessions/{session_id}/stop", response_model=StsControlResponse)
async def stop_session(
    session_id: str,
    broker: BrokerDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> StsControlResponse:
    """Placeholder until IF-04 (#182) defines ``sts.session.end``.

    RM-04 (#167) deleted the worker a stop was addressed to, and with it the
    escalation that killed one that would not answer. There is no process
    left to stop, so nothing is sent.
    """
    del session_id, broker, owner, principal
    raise HTTPException(
        status_code=501,
        detail="stopping an sts session is not implemented — waiting for "
        "IF-04 (#182)",
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
            row.type,
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
        await broker.publish(Topics.status_sts(), envelope)
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
    """Ask every declared STS, and merge what they have.

    Not the plane's shared subject, and not one instance either. An event log
    lives on the disk of whichever process wrote it, and one session's can
    genuinely span two: a rebuild elsewhere leaves the earlier parts on the
    volume of the process that died. So there is no single right instance to
    ask — asking one would report a prefix as the whole story, and asking
    whichever answered would report somebody else's silence as "no log".

    Which is also why this cannot be addressed the way stop and fail are.
    Those need the process *holding* the session, and a finished session has
    no holder; this needs the disk, and a finished session's disk is still
    there.
    """
    named: list[str | None] = []
    try:
        async with session_scope() as db:
            rows = await InstanceRepository(db).list_all(
                domain=SessionDomain.STS.value
            )
        named = [row.name for row in rows]
    except Exception:
        # A log is worth reading when the database is not: this route needed
        # only the broker before, and the table is here to say *which* STS to
        # ask rather than whether to ask at all.
        logger.warning(
            "sts eventlog info: could not list instances — asking the shared "
            "subject",
            exc_info=True,
        )
    if not named:
        # Nothing declared, or nothing readable. The shared subject is what a
        # node with one STS has always used, and it is the only one there is.
        named = [None]

    replies = await asyncio.gather(
        *(_ask_eventlog(broker, session_id, name) for name in named)
    )
    found = [r for r in replies if r is not None]
    if not found:
        raise HTTPException(
            status_code=502,
            detail=f"no STS answered for the event log of {session_id}",
        )

    parts = [part for reply in found for part in reply.parts]
    return StsEventLogInfo(
        session_id=session_id,
        available=any(r.available for r in found),
        # True if *anybody* keeps logs. Paired with ``available`` it still
        # separates "we do not keep these" from "we keep these, but not that
        # session's".
        enabled=any(r.enabled for r in found),
        parts=_merged_parts(parts),
        total_bytes=sum(part.size for part in parts),
        live=any(r.live for r in found),
    )


def _merged_parts(parts: list[StsEventLogPart]) -> list[StsEventLogPart]:
    """Oldest first across instances, by modification time.

    Within one instance ``log_parts`` already orders them, and their mtimes
    increase with it. Across instances the handover is what orders them: every
    file the process that died wrote was written before the one that took over
    started. A part with no mtime keeps its place at the end rather than
    guessing.

    Depends on the hosts' clocks agreeing to within the gap between a session
    stopping on one and resuming on another. That is seconds at worst, and the
    alternative — reporting one instance's parts as the whole log — is wrong
    every time rather than under skew.
    """
    return sorted(
        parts, key=lambda p: (p.modified is None, p.modified or 0.0)
    )


async def _ask_eventlog(
    broker: BrokerDep, session_id: str, instance: str | None
) -> StsEventLogInfo | None:
    """One instance's answer. ``None`` when it did not give one.

    An instance that is down contributes nothing rather than failing the
    request: the log may well be entirely on one that did answer.
    """
    try:
        return await request_domain(
            broker,
            Topics.STS if instance is None else Topics.sts(instance),
            StsEventLogInfoRequestEnvelope.wrap(
                StsEventLogInfoRequest(session_id=session_id),
                type=STS_EVENTLOG_INFO,
                source="api",
                session_id=session_id,
            ),
            result_type=StsEventLogInfo,
            timeout=10.0,
        )
    except DomainRpcError:
        logger.warning(
            "sts eventlog info: %s did not answer for session=%s",
            instance or "the shared subject",
            session_id,
        )
        return None


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
                    # The instance the listing said has this part. Names
                    # collide across instances — every process writes the same
                    # ``{session}.jsonl`` — so a read sent to whichever
                    # answered could return a different session's bytes under
                    # the right file name, or none at all.
                    (
                        Topics.STS
                        if part.instance is None
                        else Topics.sts(part.instance)
                    ),
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


@router.post(
    "/deploy/{strategy_type}",
    response_model=DeployResponse,
    status_code=202,
)
async def deploy(
    strategy_type: str,
    body: StrategyDeployBody,
    broker: BrokerDep,
    store: RegistryStoreDep,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> DeployResponse:
    """Accept a deploy before the session is running (F12, §8.1).

    Progress is null. The STS controller writes status and conditions;
    the ingress writes hook progress. ``body.timeout`` is not the accept
    budget — the old synchronous create timeout is gone.
    """
    if _deployable_template(strategy_type, store) is None:
        known = ", ".join(t.type for t in _deployable_templates(store))
        raise HTTPException(
            status_code=404,
            detail=(
                f"unknown strategy type: {strategy_type}; known: {known}"
            ),
        )
    try:
        spec = parse_strategy_yml(body.yaml)
    except StrategyYamlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _refuse_sts_accounts_missing_from_td(spec)
    created_by = body.created_by if body.created_by is not None else owner
    try:
        result = await start(
            spec,
            broker=broker,
            strategy_type=strategy_type,
            yaml_text=body.yaml,
            created_by=created_by,
            instance=body.instance,
        )
    except DomainRpcError as exc:
        raise HTTPException(
            status_code=_deploy_status(exc), detail=exc.message
        ) from exc
    await record_audit(
        user_id=created_by,
        operation="sts.deploy",
        result=(
            f"session_id={result.session_id} type={strategy_type} "
            f"td_names={list(spec.td)}"
        ),
        principal=principal,
    )
    return result


def _deploy_status(exc: DomainRpcError) -> int:
    """HTTP status for a domain refusal during accept.

    The same mapping the synchronous deploy used. A miss is 504. An
    unknown account or strategy is 404. Everything else, including a
    protocol mismatch, is 502 — the plane answered, and the answer was
    not an accept.
    """
    if exc.code in {"unknown_strategy", "not_found", "unknown_api"}:
        return 404
    if exc.code == "timeout":
        return 504
    if exc.code == "incompatible_environment":
        return 409
    if exc.code == "strategy_refused":
        return 400
    return 502


def _refuse_sts_accounts_missing_from_td(spec: StrategySpec) -> None:
    """``quote_account`` / ``hedge_account`` must be keys under ``td``."""
    for field in ("quote_account", "hedge_account"):
        name = spec.sts.get(field)
        if not isinstance(name, str) or not name.strip():
            continue
        if name.strip() not in spec.td:
            raise HTTPException(
                status_code=400,
                detail=f"{field} {name!r} is not a key under td",
            )

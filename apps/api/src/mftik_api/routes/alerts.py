"""Alert graph CRUD. GET never returns ``webhook_url``."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException
from mftik_db.models.alert import Alert, AlertDelivery, AlertMatcher, AlertSource
from mftik_db.repositories import (
    AlertMatcherRepository,
    AlertRepository,
    AlertSourceRepository,
    InvalidAlertSelector,
)
from mftik_db.session import session_scope
from sqlalchemy.exc import IntegrityError

from mftik_api.alert_discord import (
    InvalidWebhookUrl,
    mask_webhook_url,
    post_webhook,
    test_fire_payload,
    validate_webhook_url,
)
from mftik_api.alert_match import current_runtime
from mftik_api.alert_spec import InvalidMatcherSpec, compile_matcher_spec
from mftik_api.audit_util import record_audit
from mftik_api.auth import ANONYMOUS, OwnerId, PrincipalDep
from mftik_api.deps import DEFAULT_USER_ID
from mftik_api.schemas import (
    AlertCreateBody,
    AlertDeleteResponse,
    AlertDeliveryListResponse,
    AlertDeliveryOut,
    AlertListResponse,
    AlertMatcherCreateBody,
    AlertMatcherListResponse,
    AlertMatcherOut,
    AlertMatcherPatchBody,
    AlertOut,
    AlertPatchBody,
    AlertSourceCreateBody,
    AlertSourceListResponse,
    AlertSourceOut,
    AlertTestResponse,
    AlertWireResponse,
)

router = APIRouter(tags=["alerts"])


def _integrity_http(exc: IntegrityError, duplicate: str) -> HTTPException:
    orig = getattr(exc, "orig", None)
    text = str(orig or exc).lower()
    pgcode = getattr(orig, "pgcode", None)
    if pgcode == "23505" or "unique" in text:
        return HTTPException(status_code=409, detail=duplicate)
    if pgcode == "23503" or "foreign key" in text:
        return HTTPException(status_code=422, detail="created_by does not exist")
    return HTTPException(status_code=422, detail="could not store row")


def _epoch(value: datetime | None) -> float:
    if value is None:
        return 0.0
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


def _source_out(row: AlertSource, matcher_ids: list[int]) -> AlertSourceOut:
    return AlertSourceOut(
        id=row.id,
        created_by=row.created_by,
        domain=row.domain,
        selector=row.selector,
        matcher_ids=matcher_ids,
    )


def _matcher_out(
    row: AlertMatcher, source_ids: list[int], alert_ids: list[int]
) -> AlertMatcherOut:
    runtime = current_runtime()
    reason = None if runtime is None else runtime.disabled.get(row.id)
    return AlertMatcherOut(
        id=row.id,
        created_by=row.created_by,
        name=row.name,
        kind=row.kind,
        spec=dict(row.spec or {}),
        source_ids=source_ids,
        alert_ids=alert_ids,
        disabled_reason=reason,
    )


def _alert_out(row: Alert, matcher_ids: list[int]) -> AlertOut:
    return AlertOut(
        id=row.id,
        created_by=row.created_by,
        name=row.name,
        kind=row.kind,
        webhook_masked=mask_webhook_url(row.webhook_url),
        enabled=row.enabled,
        flush_interval_s=row.flush_interval_s,
        max_events_in_payload=row.max_events_in_payload,
        max_buffer_events=row.max_buffer_events,
        dedupe=row.dedupe,
        matcher_ids=matcher_ids,
    )


def _delivery_out(row: AlertDelivery) -> AlertDeliveryOut:
    return AlertDeliveryOut(
        id=row.id,
        alert_id=row.alert_id,
        window_start=_epoch(row.window_start),
        event_count=row.event_count,
        dropped_count=row.dropped_count,
        http_status=row.http_status,
        error=row.error,
        ts=_epoch(row.ts),
    )


async def _wires(
    alerts: AlertRepository,
) -> tuple[
    dict[int, list[int]],
    dict[int, list[int]],
    dict[int, list[int]],
    dict[int, list[int]],
]:
    source_matchers: dict[int, list[int]] = defaultdict(list)
    matcher_sources: dict[int, list[int]] = defaultdict(list)
    matcher_alerts: dict[int, list[int]] = defaultdict(list)
    alert_matchers: dict[int, list[int]] = defaultdict(list)
    for wire in await alerts.list_source_matcher_wires():
        source_matchers[wire.source_id].append(wire.matcher_id)
        matcher_sources[wire.matcher_id].append(wire.source_id)
    for wire in await alerts.list_matcher_alert_wires():
        matcher_alerts[wire.matcher_id].append(wire.alert_id)
        alert_matchers[wire.alert_id].append(wire.matcher_id)
    return source_matchers, matcher_sources, matcher_alerts, alert_matchers


# --- sources -----------------------------------------------------------------


@router.get("/alerts/sources", response_model=AlertSourceListResponse)
async def list_sources() -> AlertSourceListResponse:
    async with session_scope() as db:
        rows = await AlertSourceRepository(db).list_all()
        source_matchers, *_ = await _wires(AlertRepository(db))
    return AlertSourceListResponse(
        sources=[_source_out(row, source_matchers.get(row.id, [])) for row in rows]
    )


@router.post("/alerts/sources", response_model=AlertSourceOut, status_code=201)
async def create_source(
    body: AlertSourceCreateBody,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertSourceOut:
    created_by = body.created_by if body.created_by is not None else owner
    try:
        async with session_scope() as db:
            sources = AlertSourceRepository(db)
            existing = await sources.get_by_domain_selector(
                body.domain, body.selector
            )
            if existing is not None:
                raise HTTPException(
                    status_code=409,
                    detail=f"source already exists: {body.domain}:{body.selector}",
                )
            row = await sources.create(
                created_by=created_by,
                domain=body.domain,
                selector=body.selector,
            )
            result = _source_out(row, [])
    except InvalidAlertSelector as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IntegrityError as exc:
        raise _integrity_http(
            exc, f"source already exists: {body.domain}:{body.selector}"
        ) from exc
    await record_audit(
        user_id=created_by,
        operation="alert.source.create",
        result=f"source_id={result.id} {result.domain}:{result.selector}",
        principal=principal,
    )
    return result


@router.delete("/alerts/sources/{source_id}", response_model=AlertDeleteResponse)
async def delete_source(
    source_id: int, principal: PrincipalDep = ANONYMOUS
) -> AlertDeleteResponse:
    async with session_scope() as db:
        sources = AlertSourceRepository(db)
        row = await sources.get(source_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"source not found: {source_id}"
            )
        owner_id = row.created_by
        label = f"{row.domain}:{row.selector}"
        await sources.delete(source_id)
    await record_audit(
        user_id=owner_id,
        operation="alert.source.delete",
        result=f"source_id={source_id} {label}",
        principal=principal,
    )
    return AlertDeleteResponse(id=source_id)


# --- matchers ----------------------------------------------------------------


@router.get("/alerts/matchers", response_model=AlertMatcherListResponse)
async def list_matchers() -> AlertMatcherListResponse:
    async with session_scope() as db:
        rows = await AlertMatcherRepository(db).list_all()
        _, matcher_sources, matcher_alerts, _ = await _wires(AlertRepository(db))
    return AlertMatcherListResponse(
        matchers=[
            _matcher_out(
                row,
                matcher_sources.get(row.id, []),
                matcher_alerts.get(row.id, []),
            )
            for row in rows
        ]
    )


@router.post("/alerts/matchers", response_model=AlertMatcherOut, status_code=201)
async def create_matcher(
    body: AlertMatcherCreateBody,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertMatcherOut:
    created_by = body.created_by if body.created_by is not None else owner
    name = body.name.strip()
    try:
        compile_matcher_spec(body.kind, body.spec)
    except InvalidMatcherSpec as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        async with session_scope() as db:
            matchers = AlertMatcherRepository(db)
            if await matchers.get_by_name(name) is not None:
                raise HTTPException(
                    status_code=409, detail=f"matcher name already exists: {name}"
                )
            row = await matchers.create(
                created_by=created_by,
                name=name,
                kind=body.kind,
                spec=body.spec,
            )
            result = _matcher_out(row, [], [])
    except IntegrityError as exc:
        raise _integrity_http(
            exc, f"matcher name already exists: {name}"
        ) from exc
    await record_audit(
        user_id=created_by,
        operation="alert.matcher.create",
        result=f"matcher_id={result.id} name={result.name} kind={result.kind}",
        principal=principal,
    )
    return result


@router.patch("/alerts/matchers/{matcher_id}", response_model=AlertMatcherOut)
async def patch_matcher(
    matcher_id: int,
    body: AlertMatcherPatchBody,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertMatcherOut:
    async with session_scope() as db:
        matchers = AlertMatcherRepository(db)
        row = await matchers.get(matcher_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"matcher not found: {matcher_id}"
            )
        if body.name is not None:
            name = body.name.strip()
            conflict = await matchers.get_by_name(name)
            if conflict is not None and conflict.id != matcher_id:
                raise HTTPException(
                    status_code=409, detail=f"matcher name already exists: {name}"
                )
            row.name = name
        if body.kind is not None:
            row.kind = body.kind
        if body.spec is not None:
            row.spec = body.spec
        try:
            compile_matcher_spec(row.kind, dict(row.spec or {}))
        except InvalidMatcherSpec as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        await db.flush()
        _, matcher_sources, matcher_alerts, _ = await _wires(AlertRepository(db))
        owner_id = row.created_by
        result = _matcher_out(
            row,
            matcher_sources.get(row.id, []),
            matcher_alerts.get(row.id, []),
        )
    await record_audit(
        user_id=owner_id,
        operation="alert.matcher.patch",
        result=f"matcher_id={result.id} name={result.name} kind={result.kind}",
        principal=principal,
    )
    return result


@router.delete("/alerts/matchers/{matcher_id}", response_model=AlertDeleteResponse)
async def delete_matcher(
    matcher_id: int, principal: PrincipalDep = ANONYMOUS
) -> AlertDeleteResponse:
    async with session_scope() as db:
        matchers = AlertMatcherRepository(db)
        row = await matchers.get(matcher_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"matcher not found: {matcher_id}"
            )
        owner_id = row.created_by
        name = row.name
        await matchers.delete(matcher_id)
    await record_audit(
        user_id=owner_id,
        operation="alert.matcher.delete",
        result=f"matcher_id={matcher_id} name={name}",
        principal=principal,
    )
    return AlertDeleteResponse(id=matcher_id)


# --- wires -------------------------------------------------------------------


@router.put(
    "/alerts/sources/{source_id}/matchers/{matcher_id}",
    response_model=AlertWireResponse,
)
async def wire_source_matcher(
    source_id: int,
    matcher_id: int,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertWireResponse:
    async with session_scope() as db:
        if await AlertSourceRepository(db).get(source_id) is None:
            raise HTTPException(
                status_code=404, detail=f"source not found: {source_id}"
            )
        matcher = await AlertMatcherRepository(db).get(matcher_id)
        if matcher is None:
            raise HTTPException(
                status_code=404, detail=f"matcher not found: {matcher_id}"
            )
        await AlertRepository(db).wire_source_matcher(source_id, matcher_id)
        owner_id = matcher.created_by
    await record_audit(
        user_id=owner_id,
        operation="alert.wire.source_matcher",
        result=f"source_id={source_id} matcher_id={matcher_id}",
        principal=principal,
    )
    return AlertWireResponse(source_id=source_id, matcher_id=matcher_id)


@router.delete(
    "/alerts/sources/{source_id}/matchers/{matcher_id}",
    response_model=AlertWireResponse,
)
async def unwire_source_matcher(
    source_id: int,
    matcher_id: int,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertWireResponse:
    async with session_scope() as db:
        alerts = AlertRepository(db)
        if not await alerts.unwire_source_matcher(source_id, matcher_id):
            raise HTTPException(status_code=404, detail="wire not found")
    await record_audit(
        user_id=DEFAULT_USER_ID,
        operation="alert.unwire.source_matcher",
        result=f"source_id={source_id} matcher_id={matcher_id}",
        principal=principal,
    )
    return AlertWireResponse(
        wired=False, source_id=source_id, matcher_id=matcher_id
    )


@router.put(
    "/alerts/matchers/{matcher_id}/alerts/{alert_id}",
    response_model=AlertWireResponse,
)
async def wire_matcher_alert(
    matcher_id: int,
    alert_id: int,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertWireResponse:
    async with session_scope() as db:
        matcher = await AlertMatcherRepository(db).get(matcher_id)
        if matcher is None:
            raise HTTPException(
                status_code=404, detail=f"matcher not found: {matcher_id}"
            )
        alert = await AlertRepository(db).get(alert_id)
        if alert is None:
            raise HTTPException(
                status_code=404, detail=f"alert not found: {alert_id}"
            )
        await AlertRepository(db).wire_matcher_alert(matcher_id, alert_id)
        owner_id = alert.created_by
    await record_audit(
        user_id=owner_id,
        operation="alert.wire.matcher_alert",
        result=f"matcher_id={matcher_id} alert_id={alert_id}",
        principal=principal,
    )
    return AlertWireResponse(matcher_id=matcher_id, alert_id=alert_id)


@router.delete(
    "/alerts/matchers/{matcher_id}/alerts/{alert_id}",
    response_model=AlertWireResponse,
)
async def unwire_matcher_alert(
    matcher_id: int,
    alert_id: int,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertWireResponse:
    async with session_scope() as db:
        if not await AlertRepository(db).unwire_matcher_alert(
            matcher_id, alert_id
        ):
            raise HTTPException(status_code=404, detail="wire not found")
    await record_audit(
        user_id=DEFAULT_USER_ID,
        operation="alert.unwire.matcher_alert",
        result=f"matcher_id={matcher_id} alert_id={alert_id}",
        principal=principal,
    )
    return AlertWireResponse(
        wired=False, matcher_id=matcher_id, alert_id=alert_id
    )


# --- alerts ------------------------------------------------------------------


@router.get("/alerts", response_model=AlertListResponse)
async def list_alerts() -> AlertListResponse:
    async with session_scope() as db:
        rows = await AlertRepository(db).list_all()
        *_, alert_matchers = await _wires(AlertRepository(db))
    return AlertListResponse(
        alerts=[_alert_out(row, alert_matchers.get(row.id, [])) for row in rows]
    )


@router.post("/alerts", response_model=AlertOut, status_code=201)
async def create_alert(
    body: AlertCreateBody,
    owner: OwnerId = DEFAULT_USER_ID,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertOut:
    created_by = body.created_by if body.created_by is not None else owner
    name = body.name.strip()
    try:
        url = validate_webhook_url(body.webhook_url)
    except InvalidWebhookUrl as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        async with session_scope() as db:
            alerts = AlertRepository(db)
            if await alerts.get_by_name(name) is not None:
                raise HTTPException(
                    status_code=409, detail=f"alert name already exists: {name}"
                )
            row = await alerts.create(
                created_by=created_by,
                name=name,
                webhook_url=url,
                kind=body.kind,
                enabled=body.enabled,
                flush_interval_s=body.flush_interval_s,
                max_events_in_payload=body.max_events_in_payload,
                max_buffer_events=body.max_buffer_events,
                dedupe=body.dedupe,
            )
            result = _alert_out(row, [])
    except IntegrityError as exc:
        raise _integrity_http(exc, f"alert name already exists: {name}") from exc
    await record_audit(
        user_id=created_by,
        operation="alert.create",
        result=f"alert_id={result.id} name={result.name}",
        principal=principal,
    )
    return result


@router.get("/alerts/{alert_id}/deliveries", response_model=AlertDeliveryListResponse)
async def list_deliveries(alert_id: int) -> AlertDeliveryListResponse:
    async with session_scope() as db:
        alerts = AlertRepository(db)
        if await alerts.get(alert_id) is None:
            raise HTTPException(
                status_code=404, detail=f"alert not found: {alert_id}"
            )
        rows = await alerts.list_deliveries(alert_id)
    return AlertDeliveryListResponse(deliveries=[_delivery_out(row) for row in rows])


@router.post("/alerts/{alert_id}/test", response_model=AlertTestResponse)
async def test_alert(
    alert_id: int, principal: PrincipalDep = ANONYMOUS
) -> AlertTestResponse:
    async with session_scope() as db:
        row = await AlertRepository(db).get(alert_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"alert not found: {alert_id}"
            )
        url = row.webhook_url
        name = row.name
        owner_id = row.created_by
    now = datetime.now(UTC)
    http_status: int | None = None
    error: str | None = None
    try:
        response = await post_webhook(url, test_fire_payload(name))
        http_status = response.status_code
        if response.status_code >= 400:
            error = f"discord returned {response.status_code}"
    except Exception as exc:
        error = exc.__class__.__name__
    async with session_scope() as db:
        delivery = await AlertRepository(db).record_delivery(
            alert_id=alert_id,
            window_start=now,
            event_count=0,
            dropped_count=0,
            http_status=http_status,
            error=error,
            ts=now,
        )
        out = _delivery_out(delivery)
    await record_audit(
        user_id=owner_id,
        operation="alert.test",
        result=f"alert_id={alert_id} name={name} http_status={http_status}",
        principal=principal,
    )
    return AlertTestResponse(delivery=out)


@router.get("/alerts/{alert_id}", response_model=AlertOut)
async def get_alert(alert_id: int) -> AlertOut:
    async with session_scope() as db:
        row = await AlertRepository(db).get(alert_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"alert not found: {alert_id}"
            )
        *_, alert_matchers = await _wires(AlertRepository(db))
    return _alert_out(row, alert_matchers.get(row.id, []))


@router.patch("/alerts/{alert_id}", response_model=AlertOut)
async def patch_alert(
    alert_id: int,
    body: AlertPatchBody,
    principal: PrincipalDep = ANONYMOUS,
) -> AlertOut:
    async with session_scope() as db:
        alerts = AlertRepository(db)
        row = await alerts.get(alert_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"alert not found: {alert_id}"
            )
        if body.name is not None:
            name = body.name.strip()
            conflict = await alerts.get_by_name(name)
            if conflict is not None and conflict.id != alert_id:
                raise HTTPException(
                    status_code=409, detail=f"alert name already exists: {name}"
                )
            row.name = name
        if "webhook_url" in body.model_fields_set:
            try:
                row.webhook_url = validate_webhook_url(body.webhook_url or "")
            except InvalidWebhookUrl as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        if body.enabled is not None:
            row.enabled = body.enabled
        if body.flush_interval_s is not None:
            row.flush_interval_s = body.flush_interval_s
        if body.max_events_in_payload is not None:
            row.max_events_in_payload = body.max_events_in_payload
        if body.max_buffer_events is not None:
            row.max_buffer_events = body.max_buffer_events
        if body.dedupe is not None:
            row.dedupe = body.dedupe
        await db.flush()
        *_, alert_matchers = await _wires(alerts)
        owner_id = row.created_by
        result = _alert_out(row, alert_matchers.get(row.id, []))
    await record_audit(
        user_id=owner_id,
        operation="alert.patch",
        result=f"alert_id={result.id} name={result.name}",
        principal=principal,
    )
    return result


@router.delete("/alerts/{alert_id}", response_model=AlertDeleteResponse)
async def delete_alert(
    alert_id: int, principal: PrincipalDep = ANONYMOUS
) -> AlertDeleteResponse:
    async with session_scope() as db:
        alerts = AlertRepository(db)
        row = await alerts.get(alert_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"alert not found: {alert_id}"
            )
        owner_id = row.created_by
        name = row.name
        await alerts.delete(alert_id)
    await record_audit(
        user_id=owner_id,
        operation="alert.delete",
        result=f"alert_id={alert_id} name={name}",
        principal=principal,
    )
    return AlertDeleteResponse(id=alert_id)

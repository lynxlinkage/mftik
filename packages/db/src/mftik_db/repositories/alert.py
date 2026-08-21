"""Alert graph repositories and the Source selector grammar."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mftik_db.models.alert import (
    Alert,
    AlertDelivery,
    AlertKind,
    AlertMatcher,
    AlertMatcherAlert,
    AlertSource,
    AlertSourceDomain,
    AlertSourceMatcher,
)
from mftik_db.repositories.base import BaseRepository

#: Same rule as ``mftik.registry.qualify.split_qualified``, inlined
#: because ``mftik_db`` does not depend on ``mftik``.
_SEP = "::"
_DOMAINS = frozenset(d.value for d in AlertSourceDomain)


class InvalidAlertSelector(ValueError):
    """``domain`` / ``selector`` pair the repository will not store."""


def validate_source_selector(domain: str, selector: str) -> None:
    """``domain`` in ``{sts,td,md}``; selector ``*`` or a non-empty string.

    An STS selector that contains ``::`` must split as ``origin::name`` with
    no extra separator — the same refusal ``list_live_for_origin`` uses.
    A hex session id has no ``::`` and is accepted; it never matches.
    """
    if domain not in _DOMAINS:
        raise InvalidAlertSelector(
            f"domain must be one of {sorted(_DOMAINS)}, not {domain!r}"
        )
    if not selector:
        raise InvalidAlertSelector("selector must be '*' or a non-empty string")
    if selector == "*":
        return
    if domain == AlertSourceDomain.STS.value and _SEP in selector:
        origin, sep, rest = selector.partition(_SEP)
        if not sep or not origin or not rest or _SEP in rest:
            raise InvalidAlertSelector(
                f"sts selector {selector!r} is not a qualified type"
            )


class AlertSourceRepository(BaseRepository[AlertSource]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, AlertSource)

    async def create(
        self, *, created_by: int, domain: str, selector: str
    ) -> AlertSource:
        validate_source_selector(domain, selector)
        return await self.add(
            AlertSource(
                created_by=created_by, domain=domain, selector=selector
            )
        )

    async def list_all(self) -> Sequence[AlertSource]:
        result = await self.session.execute(
            select(AlertSource).order_by(AlertSource.id)
        )
        return result.scalars().all()

    async def delete(self, source_id: int) -> bool:
        row = await self.get(source_id)
        if row is None:
            return False
        await self.session.delete(row)
        await self.session.flush()
        return True


class AlertMatcherRepository(BaseRepository[AlertMatcher]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, AlertMatcher)

    async def create(
        self,
        *,
        created_by: int,
        name: str,
        kind: str,
        spec: dict[str, Any] | None = None,
    ) -> AlertMatcher:
        return await self.add(
            AlertMatcher(
                created_by=created_by,
                name=name,
                kind=kind,
                spec=dict(spec or {}),
            )
        )

    async def delete(self, matcher_id: int) -> bool:
        row = await self.get(matcher_id)
        if row is None:
            return False
        await self.session.delete(row)
        await self.session.flush()
        return True


class AlertRepository(BaseRepository[Alert]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Alert)

    async def create(
        self,
        *,
        created_by: int,
        name: str,
        webhook_url: str,
        kind: str = AlertKind.DISCORD_WEBHOOK.value,
        enabled: bool = True,
        flush_interval_s: int = 30,
        max_events_in_payload: int = 15,
        max_buffer_events: int = 200,
        dedupe: bool = True,
    ) -> Alert:
        return await self.add(
            Alert(
                created_by=created_by,
                name=name,
                kind=kind,
                webhook_url=webhook_url,
                enabled=enabled,
                flush_interval_s=flush_interval_s,
                max_events_in_payload=max_events_in_payload,
                max_buffer_events=max_buffer_events,
                dedupe=dedupe,
            )
        )

    async def delete(self, alert_id: int) -> bool:
        row = await self.get(alert_id)
        if row is None:
            return False
        await self.session.delete(row)
        await self.session.flush()
        return True

    async def wire_source_matcher(
        self, source_id: int, matcher_id: int
    ) -> AlertSourceMatcher:
        existing = await self.session.get(
            AlertSourceMatcher, (source_id, matcher_id)
        )
        if existing is not None:
            return existing
        row = AlertSourceMatcher(source_id=source_id, matcher_id=matcher_id)
        self.session.add(row)
        await self.session.flush()
        return row

    async def unwire_source_matcher(
        self, source_id: int, matcher_id: int
    ) -> bool:
        row = await self.session.get(
            AlertSourceMatcher, (source_id, matcher_id)
        )
        if row is None:
            return False
        await self.session.delete(row)
        await self.session.flush()
        return True

    async def wire_matcher_alert(
        self, matcher_id: int, alert_id: int
    ) -> AlertMatcherAlert:
        existing = await self.session.get(
            AlertMatcherAlert, (matcher_id, alert_id)
        )
        if existing is not None:
            return existing
        row = AlertMatcherAlert(matcher_id=matcher_id, alert_id=alert_id)
        self.session.add(row)
        await self.session.flush()
        return row

    async def unwire_matcher_alert(
        self, matcher_id: int, alert_id: int
    ) -> bool:
        row = await self.session.get(AlertMatcherAlert, (matcher_id, alert_id))
        if row is None:
            return False
        await self.session.delete(row)
        await self.session.flush()
        return True

    async def record_delivery(
        self,
        *,
        alert_id: int,
        window_start: datetime,
        event_count: int,
        dropped_count: int = 0,
        http_status: int | None = None,
        error: str | None = None,
        ts: datetime | None = None,
    ) -> AlertDelivery:
        row = AlertDelivery(
            alert_id=alert_id,
            window_start=window_start,
            event_count=event_count,
            dropped_count=dropped_count,
            http_status=http_status,
            error=error,
            ts=ts or window_start,
        )
        self.session.add(row)
        await self.session.flush()
        return row

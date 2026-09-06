"""Home dashboard statistics — one row per declared instance."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter
from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    Envelope,
    HealthCheck,
    HealthStatus,
    Topics,
)
from mftik_db.models.instance import Instance
from mftik_db.models.session import SessionDomain, SessionStatus
from mftik_db.repositories import (
    InstanceRepository,
    MdSessionRepository,
    StsSessionRepository,
    TdSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.deps import BrokerDep
from mftik_api.schemas import DomainStats, StatsResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["stats"])

#: How long one instance gets to answer. Every probe runs concurrently, so this
#: is the whole page's budget rather than each row's — three down instances
#: cost one of these, not three.
PROBE_TIMEOUT_S = 1.5

#: What a declared instance is doing, as far as the node can tell.
#:
#: A boolean cannot say this. *Down* is a fact about a machine somebody has to
#: go and look at, and it is only knowable because a row says the instance
#: should be here — collapsing it back into "not healthy" is how a dead plane
#: goes back to being invisible, which is what the declared table exists to
#: prevent. See ``docs/Instances.md``.
STATE_CONNECTED = "connected"
STATE_DOWN = "down"


async def _probe(broker: Broker, row: Instance) -> HealthStatus | None:
    """Ask one instance whether it is there. ``None`` means it did not answer."""
    subject = Topics.health(row.domain, row.name)
    try:
        reply = await broker.probe(
            subject,
            Envelope[HealthCheck].wrap(
                HealthCheck(), type=f"{row.domain}.health", source="api"
            ),
            timeout=PROBE_TIMEOUT_S,
        )
    except RequestTimeoutError:
        return None
    except Exception:
        # A broker that cannot take a write is a problem of its own, and it is
        # not evidence about this instance — but the honest thing to render is
        # still "we did not hear back".
        logger.exception("health probe failed instance=%s", row.name)
        return None
    try:
        return HealthStatus.model_validate(reply.payload)
    except Exception:
        logger.warning(
            "instance %s answered with an unreadable health payload", row.name
        )
        return None


@router.get("/stats", response_model=StatsResponse)
async def get_stats(broker: BrokerDep) -> StatsResponse:
    async with session_scope() as db:
        instances = list(await InstanceRepository(db).list_all())
        sts = StsSessionRepository(db)
        td = TdSessionRepository(db)
        md = MdSessionRepository(db)
        live = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.LIVE.value
            ),
            SessionDomain.TD.value: await td.count(status=SessionStatus.LIVE.value),
            SessionDomain.MD.value: await md.count(status=SessionStatus.LIVE.value),
        }
        done = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.DONE.value
            ),
            SessionDomain.TD.value: await td.count(status=SessionStatus.DONE.value),
            SessionDomain.MD.value: await md.count(status=SessionStatus.DONE.value),
        }
        # Counted apart from done: these are finished, but lumping them in
        # with it is how a run that died — or one STS cut short and someone
        # may want back — stops being noticed.
        failed = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.FAILED.value
            ),
        }
        interrupted = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.INTERRUPTED.value
            ),
        }
        ack = {
            SessionDomain.STS.value: await sts.count(
                status=SessionStatus.ACK.value
            ),
        }

    # Concurrently, so the page costs one timeout however many instances are
    # down. Serially this is the difference between a dashboard and a wait.
    replies = await asyncio.gather(
        *(_probe(broker, row) for row in instances)
    )

    domains: list[DomainStats] = []
    for row, reply in zip(instances, replies, strict=True):
        connected = reply is not None
        # Session counts are per plane, not per instance: the tables record
        # which plane ran a session, and splitting them by instance is INS-7's
        # and INS-8's job, not this one's. Repeating the plane's numbers on
        # each of its instances would be a lie about where they came from, so
        # they are attached to the first row of each plane and left at zero on
        # the rest.
        first_of_plane = row is next(
            r for r in instances if r.domain == row.domain
        )
        domains.append(
            DomainStats(
                domain=row.domain,
                instance=row.name,
                region=row.region,
                enabled=row.enabled,
                state=STATE_CONNECTED if connected else STATE_DOWN,
                healthy=connected,
                version=reply.version if reply else None,
                venues=list(reply.venues) if reply else [],
                api_ids=list(reply.api_ids) if reply else [],
                live=live.get(row.domain, 0) if first_of_plane else 0,
                done=done.get(row.domain, 0) if first_of_plane else 0,
                failed=failed.get(row.domain, 0) if first_of_plane else 0,
                interrupted=(
                    interrupted.get(row.domain, 0) if first_of_plane else 0
                ),
                ack=ack.get(row.domain, 0) if first_of_plane else 0,
            )
        )

    return StatsResponse(domains=domains)

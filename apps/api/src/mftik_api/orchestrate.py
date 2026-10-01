"""Deploy orchestrator — mints the session id and resolves the targets.

The synchronous create → MD attach → TD attach sequence and its rollback are
gone (RM-08). What is left is step 1 of the new start: the id, and the checks
that answer "is there a plane to run this on" before anything is asked to do
anything. IF-13 puts ``start`` / ``end`` on top of them.
"""

from __future__ import annotations

import logging
import secrets

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    Envelope,
    HealthCheck,
    TdAccountRef,
    Topics,
    td_api_ids_of,
)
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import (
    ApiRepository,
    InstanceRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.broker_rpc import DomainRpcError

logger = logging.getLogger(__name__)

#: Random 24-bit ids plus a SELECT is enough; a leftover row must not be
#: adopted silently (STS persist returns the existing row).
_SESSION_ID_MINT_ATTEMPTS = 8


async def mint_session_id() -> str:
    """Six lowercase hex digits that do not already name a session row."""
    for _ in range(_SESSION_ID_MINT_ATTEMPTS):
        session_id = secrets.token_hex(3)
        async with session_scope() as db:
            if await StsSessionRepository(db).get_by_session_id(session_id) is None:
                return session_id
    raise RuntimeError("could not mint a free session_id")


async def _check_md_instances(
    broker: Broker, md: dict[str, list[str]]
) -> None:
    """Every named MD instance must be declared *and* answering.

    Before any attach, and this is PI-2. Failing at attach time instead hands
    the operator a lease timeout where they should have had a sentence — and
    for an unpinned deploy there is nothing to check, because the whole point
    of the anycast pool is that no name was given.
    """
    named = [i for i in md if i != ANY_INSTANCE and md.get(i)]
    if not named:
        return

    async with session_scope() as db:
        repo = InstanceRepository(db)
        declared = {
            row.name: row
            for row in await repo.list_all(domain=SessionDomain.MD.value)
        }

    for name in named:
        row = declared.get(name)
        if row is None:
            raise DomainRpcError(
                "unknown_instance",
                f"no md instance named {name!r} — declare it first, or "
                f"remove the name to use any md",
            )
        if not row.enabled:
            raise DomainRpcError(
                "instance_disabled",
                f"md instance {name!r} is disabled; sessions already attached "
                f"keep running but new ones may not name it",
            )
        if not await _answers(broker, name):
            raise DomainRpcError(
                "instance_down",
                f"md instance {name!r} is declared but did not answer — "
                f"it is deployed nowhere, or the process is down",
            )


async def _sts_target(
    instance: str | None, td: dict[str, TdAccountRef]
) -> str:
    """Which STS subject an unnamed create is sent to.

    A named deploy keeps the name. An unnamed one is derived from the
    credentials' TD region — null on the row still means "derive", and
    the create is addressed to that instance so the pool is not a lottery.
    A derivation that is not unique must be named.
    """
    if instance is not None:
        return instance
    async with session_scope() as db:
        derived = await InstanceRepository(db).derived_sts(td_api_ids_of(td))
    if derived is None:
        raise DomainRpcError(
            "sts_unpinned_ambiguous",
            "this deploy does not name an STS instance and its TD "
            "accounts do not derive to exactly one enabled STS — "
            "name instance= on the deploy",
        )
    return derived


async def _check_sts_instance(
    broker: Broker, instance: str | None
) -> None:
    """Same two checks as MD's, on the plane that runs the strategy.

    Every create has a name by the time it reaches here — either the
    deploy asked for one, or the TD region derived it.
    """
    if instance is None:
        raise DomainRpcError(
            "sts_unpinned_ambiguous",
            "this deploy does not name an STS instance",
        )
    async with session_scope() as db:
        row = await InstanceRepository(db).get_by_name(instance)
    if row is None or row.domain != SessionDomain.STS.value:
        raise DomainRpcError(
            "unknown_instance",
            f"no sts instance named {instance!r} — declare it first, or "
            f"omit it to derive from the TD region",
        )
    if not row.enabled:
        raise DomainRpcError(
            "instance_disabled",
            f"sts instance {instance!r} is disabled; sessions already running "
            f"there keep running but new ones may not name it",
        )
    if not await _answers(broker, instance, domain=SessionDomain.STS.value):
        raise DomainRpcError(
            "instance_down",
            f"sts instance {instance!r} is declared but did not answer — "
            f"it is deployed nowhere, or the process is down",
        )


async def _answers(
    broker: Broker, instance: str, *, domain: str = SessionDomain.MD.value
) -> bool:
    """Whether this MD is there. A timeout is the answer, not an error."""
    try:
        await broker.probe(
            Topics.health(domain, instance),
            Envelope[HealthCheck].wrap(
                HealthCheck(), type=f"{domain}.health", source="api"
            ),
            timeout=_PROBE_TIMEOUT_S,
        )
    except RequestTimeoutError:
        return False
    except Exception:
        logger.exception(
            "%s instance probe failed instance=%s", domain, instance
        )
        return False
    return True


async def _td_instance(api_id: int) -> str:
    """Which TD may use this credential.

    Resolved here, from the ``apis`` row, rather than left to whichever TD
    happens to be free — that is the whole compliance requirement. A credential
    with no instance is not a thing that exists: ``apis.instance_id`` is
    ``NOT NULL``, so a missing answer means the row is gone, and a deploy
    against a credential that no longer exists should say so rather than fall
    back to a plane-wide subject that would let any TD open it.
    """
    async with session_scope() as db:
        name = await ApiRepository(db).instance_name(api_id)
    if name is None:
        raise DomainRpcError(
            "unknown_api", f"no credential with api_id={api_id}"
        )
    return name


#: How long one MD gets to answer the deploy's liveness check. Matches the
#: dashboard's, and for the same reason: this is not a health measurement, it
#: is the difference between "down" and "there".
_PROBE_TIMEOUT_S = 1.5


def _md_venues(feeds: list[str]) -> set[str]:
    venues: set[str] = set()
    for feed in feeds:
        try:
            _topic, ticker = Topics.parse_md_feed(feed)
        except ValueError:
            continue
        venues.add(ticker.venue)
    return venues

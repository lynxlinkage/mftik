"""Deploy orchestrator — API sequences STS then MD then TD."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    MD_SESSION_ATTACH,
    MD_SESSION_DETACH,
    STS_SESSION_CREATE,
    STS_SESSION_FAIL,
    TD_SESSION_ATTACH,
    Envelope,
    HealthCheck,
    MdAttachRequest,
    MdAttachRequestEnvelope,
    MdAttachResult,
    MdDetachRequest,
    MdDetachRequestEnvelope,
    StsCreateSessionRequest,
    StsCreateSessionRequestEnvelope,
    StsCreateSessionResult,
    StsSessionControlRequest,
    StsSessionControlRequestEnvelope,
    StsSessionControlResult,
    TdAccountRef,
    TdAttachRequest,
    TdAttachRequestEnvelope,
    TdAttachResult,
    Topics,
    load_md,
    md_feeds_of,
    md_instances_of,
    publish_md_log,
    publish_sts_log,
)
from mftik_db.models.session import SessionDomain, SessionStatus
from mftik_db.repositories import ApiRepository, InstanceRepository
from mftik_db.session import session_scope

from mftik_api.broker_rpc import DomainRpcError, request_domain

logger = logging.getLogger(__name__)


async def deploy_strategy(
    broker: Broker,
    *,
    strategy_id: str,
    td: dict[str, TdAccountRef] | None = None,
    md: dict[str, list[str]] | list[str] | None = None,
    st_paras: dict[str, Any] | None = None,
    created_by: int,
    timeout: float = 30.0,
    restart: str = "always",
    strategy_type: str | None = None,
    yaml_text: str | None = None,
    instance: str | None = None,
) -> dict[str, Any]:
    """Mint session_id, create STS, attach MD then each TD api_id. Fail-closed."""
    session_id = uuid4().hex
    td = dict(td or {})
    md = load_md(md)
    st_paras = dict(st_paras or {})
    attached_td: list[dict[str, Any]] = []
    attached_md: dict[str, Any] | None = None

    async def sts_log(message: str, *, level: str = "info") -> None:
        await publish_sts_log(
            broker,
            session_id,
            message,
            source="api",
            level=level,
            type=strategy_type,
        )

    await sts_log(f"deploy start strategy={strategy_id} td={td} md={md}")

    try:
        await _check_sts_instance(broker, instance)
    except DomainRpcError as exc:
        await sts_log(
            f"STS instance check failed: {exc.message}", level="error"
        )
        raise

    try:
        sts = await request_domain(
            broker,
            Topics.STS if instance is None else Topics.sts(instance),
            StsCreateSessionRequestEnvelope.wrap(
                StsCreateSessionRequest(
                    session_id=session_id,
                    created_by=created_by,
                    strategy=strategy_id,
                    td=td,
                    md=md,
                    st_paras=st_paras,
                    restart=restart,
                    type=strategy_type,
                    yaml_text=yaml_text,
                    instance=instance,
                ),
                type=STS_SESSION_CREATE,
                source="api",
                session_id=session_id,
            ),
            result_type=StsCreateSessionResult,
            timeout=10.0,
        )
        await sts_log(f"STS created strategy={sts.strategy}")
    except DomainRpcError as exc:
        await sts_log(f"STS create failed: {exc.message}", level="error")
        raise

    if sts.status != SessionStatus.LIVE.value:
        # The strategy read its configuration and refused it. Stop here rather
        # than attaching feeds to a session that no longer exists: MD would
        # wait out its whole timeout for a lease heartbeat from a stopped
        # session, and the operator would be handed that timeout instead of
        # the sentence the strategy wrote explaining what is wrong.
        reason = sts.reason or f"session ended during start ({sts.status})"
        logger.error(
            "STS session ended during start — not attaching session=%s: %s",
            session_id,
            reason,
        )
        await sts_log(
            f"strategy refused this configuration: {reason}", level="error"
        )
        raise DomainRpcError("strategy_refused", reason)

    # Resolved before anything is asked to do anything (PI-2). Two checks,
    # and the two failures are different sentences because they are different
    # problems: a name nothing declared is a typo to fix in the document, and
    # a declared name that does not answer is a machine to go and look at.
    # Neither waits or retries — the node does not make an instance exist.
    try:
        await _check_md_instances(broker, md)
    except DomainRpcError as exc:
        await sts_log(f"MD instance check failed: {exc.message}", level="error")
        await _fail_sts(broker, session_id, f"deploy refused: {exc.message}")
        raise

    attached_instances: list[str] = []
    try:
        for instance in md_instances_of(md):
            feeds = md.get(instance) or []
            if not feeds:
                continue
            where = "any md" if instance == ANY_INSTANCE else instance
            await sts_log(f"MD attach starting {where} feeds={feeds}")
            for venue in _md_venues(feeds):
                await publish_md_log(
                    broker,
                    venue,
                    f"attach starting sts={session_id} feeds={feeds}",
                    source="api",
                    instance=(
                        None if instance == ANY_INSTANCE else instance
                    ),
                )
            md_result = await request_domain(
                broker,
                Topics.MD if instance == ANY_INSTANCE else Topics.md(instance),
                MdAttachRequestEnvelope.wrap(
                    MdAttachRequest(
                        session_id=session_id,
                        created_by=created_by,
                        subscriptions=feeds,
                        timeout=timeout,
                    ),
                    type=MD_SESSION_ATTACH,
                    source="api",
                    session_id=session_id,
                ),
                result_type=MdAttachResult,
                timeout=timeout + 5.0,
            )
            attached_instances.append(instance)
            if attached_md is None:
                attached_md = {"subscriptions": [], "refcounts": {}}
            attached_md["subscriptions"].extend(md_result.subscriptions)
            attached_md["refcounts"].update(md_result.refcounts)
            await sts_log(
                f"MD attached {where} feeds={md_result.subscriptions}"
            )
            for venue in _md_venues(md_result.subscriptions):
                await publish_md_log(
                    broker,
                    venue,
                    (
                        f"attach complete sts={session_id} "
                        f"feeds={md_result.subscriptions}"
                    ),
                    source="api",
                    instance=(
                        None if instance == ANY_INSTANCE else instance
                    ),
                )

        for name, ref in td.items():
            instance = await _td_instance(ref.api_id)
            await sts_log(
                f"TD attach starting {name} api_id={ref.api_id} "
                f"instance={instance}"
            )
            result = await request_domain(
                broker,
                Topics.td(instance),
                TdAttachRequestEnvelope.wrap(
                    TdAttachRequest(
                        api_id=ref.api_id,
                        session_id=session_id,
                        created_by=created_by,
                        timeout=timeout,
                    ),
                    type=TD_SESSION_ATTACH,
                    source="api",
                    session_id=session_id,
                ),
                result_type=TdAttachResult,
                timeout=timeout + 5.0,
            )
            attached_td.append(
                {
                    "api_id": result.api_id,
                    "refcount": result.refcount,
                }
            )
            await sts_log(
                f"TD attached api_id={result.api_id} refcount={result.refcount}"
            )
    except Exception as exc:
        if isinstance(exc, DomainRpcError):
            logger.error(
                "MD/TD attach failed — rolling back STS session=%s: %s",
                session_id,
                exc,
            )
        else:
            logger.exception(
                "MD/TD attach failed — rolling back STS session=%s", session_id
            )
        await sts_log(
            f"attach failed — rolling back STS: {exc}", level="error"
        )
        # New with the fan-out: an attach that fails on the third instance
        # leaves two live, and failing STS alone would leave them pumping
        # feeds for a session that no longer exists until a reaper noticed —
        # two scans and up to a minute later.
        await _detach_md(broker, session_id, attached_instances, sts_log)
        fail_reason = f"attach failed — rolled back during deploy: {exc}"
        try:
            await request_domain(
                broker,
                # The session exists by now — the create returned — so the
                # process holding it is the only one that can fail it.
                Topics.sts_control(session_id),
                StsSessionControlRequestEnvelope.wrap(
                    StsSessionControlRequest(
                        session_id=session_id, reason=fail_reason
                    ),
                    type=STS_SESSION_FAIL,
                    source="api",
                    session_id=session_id,
                ),
                result_type=StsSessionControlResult,
                timeout=10.0,
            )
            await sts_log("STS failed after rollback", level="warning")
        except Exception:
            logger.exception("rollback STS fail failed session=%s", session_id)
        raise

    # ``md_feeds_of``, not ``list(md)``: the argument is a mapping now, and
    # iterating one yields the instance names. Reached whenever nothing was
    # attached — a document naming an instance with an empty feed list, say.
    md_out = (
        list(attached_md["subscriptions"])
        if attached_md is not None
        else md_feeds_of(md)
    )
    await sts_log(
        f"deploy complete strategy={sts.strategy} td={attached_td} md={md_out}"
    )
    return {
        "session_id": session_id,
        "strategy": sts.strategy,
        "td": attached_td,
        "md": md_out,
        "status": "live",
    }


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


async def _check_sts_instance(
    broker: Broker, instance: str | None
) -> None:
    """Same two checks as MD's, on the plane that runs the strategy.

    Nothing to check when no name was given: an unpinned deploy goes to the
    shared pool, and the pool answering is what the create's own timeout is
    for.
    """
    if instance is None:
        return
    async with session_scope() as db:
        row = await InstanceRepository(db).get_by_name(instance)
    if row is None or row.domain != SessionDomain.STS.value:
        raise DomainRpcError(
            "unknown_instance",
            f"no sts instance named {instance!r} — declare it first, or "
            f"omit it to use any sts",
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


async def _detach_md(
    broker: Broker,
    session_id: str,
    instances: list[str],
    log: Any,
) -> None:
    """Unwind the attaches that did land, before failing the session."""
    for instance in instances:
        try:
            await broker.request(
                Topics.MD if instance == ANY_INSTANCE else Topics.md(instance),
                MdDetachRequestEnvelope.wrap(
                    MdDetachRequest(
                        session_id=session_id, reason="deploy_rollback"
                    ),
                    type=MD_SESSION_DETACH,
                    source="api",
                    session_id=session_id,
                ),
                timeout=1.5,
            )
        except Exception:
            # The lease covers this: MD tears the attach down when this
            # session's heartbeat stops, which failing it is about to do.
            logger.warning(
                "MD rollback detach failed instance=%s session=%s",
                instance,
                session_id,
                exc_info=True,
            )
            continue
        await log(f"rolled back MD attach on {instance}", level="warning")


async def _fail_sts(broker: Broker, session_id: str, reason: str) -> None:
    """End a session that was created and can no longer be attached.

    Addressed to the session rather than the plane: it exists by the time this
    runs, so only the process holding it can end it — and on a node with two
    STS the shared subject would let the other one answer ``not_found`` for a
    session that is very much running.
    """
    try:
        await request_domain(
            broker,
            Topics.sts_control(session_id),
            StsSessionControlRequestEnvelope.wrap(
                StsSessionControlRequest(session_id=session_id, reason=reason),
                type=STS_SESSION_FAIL,
                source="api",
                session_id=session_id,
            ),
            result_type=StsSessionControlResult,
            timeout=10.0,
        )
    except Exception:
        logger.exception("rollback STS fail failed session=%s", session_id)


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

"""Deploy orchestrator — API sequences STS then MD then TD."""

from __future__ import annotations

import asyncio
import logging
import secrets
from typing import Any

from mftik.broker import Broker
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    MD_SESSION_ATTACH,
    MD_SESSION_DETACH,
    STOP_FORCE_RPC_TIMEOUT_S,
    STS_SESSION_CREATE,
    STS_SESSION_FAIL,
    STS_SESSION_FORCE_STOP,
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
    create_rpc_timeout,
    load_md,
    md_feeds_of,
    md_instances_of,
    publish_md_log,
    publish_sts_log,
    start_deadline_reason,
    td_api_ids_of,
)
from mftik_db.models.session import SessionDomain, SessionStatus
from mftik_db.repositories import (
    ApiRepository,
    InstanceRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.broker_rpc import DomainRpcError, request_domain

logger = logging.getLogger(__name__)

#: Random 24-bit ids plus a SELECT is enough; a leftover row must not be
#: adopted silently (STS persist returns the existing row).
_SESSION_ID_MINT_ATTEMPTS = 8

#: How long to keep sending ``abort_start`` after the create RPC timed out.
#: A broker blip can outlast the first attempt. The retry stops when the
#: STS acks or the row is terminal, and this is the cap so a dead STS
#: does not retry for the life of the process.
_ABORT_RETRY_BUDGET_S = 120.0
_ABORT_RETRY_INTERVAL_S = 2.0

#: After ``abort_start`` answers ``not_found``, the deadline kill may still
#: be writing the row. This is that wait, not another RPC.
_ABORT_ROW_POLL_S = 1.0
_ABORT_ROW_POLL_INTERVAL_S = 0.05

#: How long the API keeps trying to write ``failed`` after STS reported
#: ``start_deadline`` and the row was still ``live``. The STS retries too.
#: This one survives the STS process going away before its write lands.
_FAILED_ROW_BUDGET_S = 120.0
_FAILED_ROW_BACKOFF_S = 0.5

_abort_tasks: set[asyncio.Task[None]] = set()
_failed_row_tasks: set[asyncio.Task[None]] = set()


def cancel_create_followups() -> None:
    """Cancel abort and failed-row retries. Tests call this on the way out."""
    for task in (*list(_abort_tasks), *list(_failed_row_tasks)):
        task.cancel()


async def mint_session_id() -> str:
    """Six lowercase hex digits that do not already name a session row."""
    for _ in range(_SESSION_ID_MINT_ATTEMPTS):
        session_id = secrets.token_hex(3)
        async with session_scope() as db:
            if await StsSessionRepository(db).get_by_session_id(session_id) is None:
                return session_id
    raise RuntimeError("could not mint a free session_id")


async def deploy_strategy(
    broker: Broker,
    *,
    strategy_id: str,
    td: dict[str, TdAccountRef] | None = None,
    md: dict[str, list[str]] | list[str] | None = None,
    st_paras: dict[str, Any] | None = None,
    created_by: int,
    timeout: float = 30.0,
    start_timeout: float | None = None,
    restart: str = "always",
    strategy_type: str | None = None,
    yaml_text: str | None = None,
    instance: str | None = None,
) -> dict[str, Any]:
    """Mint session_id, create STS, attach MD then each TD api_id. Fail-closed."""
    session_id = await mint_session_id()
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
        target = await _sts_target(instance, td)
        await _check_sts_instance(broker, target)
    except DomainRpcError as exc:
        await sts_log(f"STS instance check failed: {exc.message}", level="error")
        raise

    try:
        sts = await request_domain(
            broker,
            Topics.sts(target),
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
                    start_timeout=start_timeout,
                ),
                type=STS_SESSION_CREATE,
                source="api",
                session_id=session_id,
            ),
            result_type=StsCreateSessionResult,
            timeout=create_rpc_timeout(start_timeout),
        )
        await sts_log(f"STS created strategy={sts.strategy}")
    except DomainRpcError as exc:
        await sts_log(f"STS create failed: {exc.message}", level="error")
        if exc.code == "start_deadline":
            await _ensure_start_failed(session_id, exc.message)
        if exc.code == "timeout":
            exc = await _abort_timed_out_create(broker, session_id, target, exc)
        raise exc

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
        await sts_log(f"strategy refused this configuration: {reason}", level="error")
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
                    instance=(None if instance == ANY_INSTANCE else instance),
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
            await sts_log(f"MD attached {where} feeds={md_result.subscriptions}")
            for venue in _md_venues(md_result.subscriptions):
                await publish_md_log(
                    broker,
                    venue,
                    (
                        f"attach complete sts={session_id} "
                        f"feeds={md_result.subscriptions}"
                    ),
                    source="api",
                    instance=(None if instance == ANY_INSTANCE else instance),
                )

        for name, ref in td.items():
            instance = await _td_instance(ref.api_id)
            await sts_log(
                f"TD attach starting {name} api_id={ref.api_id} instance={instance}"
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
        await sts_log(f"attach failed — rolling back STS: {exc}", level="error")
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
                    StsSessionControlRequest(session_id=session_id, reason=fail_reason),
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


async def _load_sts_row(session_id: str) -> Any:
    async with session_scope() as db:
        return await StsSessionRepository(db).get_by_session_id(session_id)


def _deadline_error(row: Any) -> DomainRpcError:
    return DomainRpcError(
        "start_deadline",
        getattr(row, "reason", None) or start_deadline_reason(),
    )


def _may_be_live(exc: DomainRpcError, session_id: str) -> DomainRpcError:
    """The create timed out and the row is still ``live``.

    The session id is the part a retry needs. Without it the warning
    can only say to run ``mftik ps``.
    """
    return DomainRpcError(
        "timeout",
        f"{exc.message}; session {session_id} may still be live — "
        f"stop it with: mftik stop {session_id}",
    )


def _row_is_terminal(row: Any) -> bool:
    return row is not None and row.status in SessionStatus.terminal()


async def _send_abort(
    broker: Broker, session_id: str, target: str
) -> DomainRpcError | None:
    """One ``abort_start``. None means the STS acked.

    No wall-clock deadline. A broker blip delivers this after any
    deadline this call could have set, and the worker must still die.
    """
    try:
        await request_domain(
            broker,
            Topics.sts(target),
            StsSessionControlRequestEnvelope.wrap(
                StsSessionControlRequest(
                    session_id=session_id,
                    only_if_silent=False,
                    abort_start=True,
                ),
                type=STS_SESSION_FORCE_STOP,
                source="api",
                session_id=session_id,
            ),
            result_type=StsSessionControlResult,
            timeout=STOP_FORCE_RPC_TIMEOUT_S,
        )
    except DomainRpcError as exc:
        return exc
    return None


async def _poll_terminal(session_id: str) -> Any:
    """Re-read until the row ends or the brief wait does.

    ``not_found`` from ``abort_start`` often means the deadline kill is
    mid-write. The next read is still ``live``; the one after is
    ``failed``.
    """
    row = await _load_sts_row(session_id)
    deadline = asyncio.get_running_loop().time() + _ABORT_ROW_POLL_S
    while not _row_is_terminal(row):
        if asyncio.get_running_loop().time() >= deadline:
            return row
        await asyncio.sleep(_ABORT_ROW_POLL_INTERVAL_S)
        row = await _load_sts_row(session_id)
    return row


def _schedule_abort_retry(broker: Broker, session_id: str, target: str) -> None:
    task = asyncio.create_task(
        _retry_abort(broker, session_id, target),
        name=f"abort-start-{session_id}",
    )
    _abort_tasks.add(task)
    task.add_done_callback(_abort_tasks.discard)


async def _retry_abort(broker: Broker, session_id: str, target: str) -> None:
    """Keep killing until the STS acks or the row is terminal.

    The first attempt already timed out. The message may still be in
    the broker, and this sends another so a dropped one is not the
    last. ``not_found`` with a live row is in-process mode: there is
    no worker, and retrying it does not make one.
    """
    started = asyncio.get_running_loop().time()
    while True:
        try:
            await asyncio.sleep(_ABORT_RETRY_INTERVAL_S)
        except asyncio.CancelledError:
            raise
        try:
            row = await _load_sts_row(session_id)
        except Exception:
            logger.exception("create abort could not read session=%s", session_id)
            row = None
        if _row_is_terminal(row):
            return
        err = await _send_abort(broker, session_id, target)
        if err is None or err.code == "not_found":
            try:
                row = await _poll_terminal(session_id)
            except Exception:
                logger.exception("create abort could not read session=%s", session_id)
                row = None
            if _row_is_terminal(row) or (err is not None and err.code == "not_found"):
                return
        if asyncio.get_running_loop().time() - started >= _ABORT_RETRY_BUDGET_S:
            logger.error(
                "create abort gave up session=%s; it may still be live",
                session_id,
            )
            return
        logger.warning("create abort retrying session=%s", session_id)


async def _ensure_start_failed(session_id: str, reason: str) -> None:
    """Write ``failed`` when STS said the deadline fired and the row did not.

    The worker is already dead on this path. A lost write leaves the row
    ``live``, and the reaper then marks it ``interrupted``, which a
    restart rebuilds. The write is retried here as well as in STS: this
    process is still up if that one is not.
    """
    try:
        row = await _load_sts_row(session_id)
    except Exception:
        logger.exception("could not read a start-deadline row session=%s", session_id)
        return
    if row is None or _row_is_terminal(row):
        return
    if await _mark_start_failed(session_id, reason):
        return
    task = asyncio.create_task(
        _retry_failed_row(session_id, reason),
        name=f"start-failed-{session_id}",
    )
    _failed_row_tasks.add(task)
    task.add_done_callback(_failed_row_tasks.discard)


async def _mark_start_failed(session_id: str, reason: str) -> bool:
    try:
        async with session_scope() as db:
            await StsSessionRepository(db).mark_failed(session_id, reason)
    except Exception:
        logger.exception(
            "could not mark a start-deadline row failed session=%s", session_id
        )
        return False
    return True


async def _retry_failed_row(session_id: str, reason: str) -> None:
    delay = _FAILED_ROW_BACKOFF_S
    started = asyncio.get_running_loop().time()
    while True:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        try:
            row = await _load_sts_row(session_id)
        except Exception:
            logger.exception(
                "could not read a start-deadline row session=%s", session_id
            )
            row = None
        if _row_is_terminal(row):
            return
        if row is not None and await _mark_start_failed(session_id, reason):
            return
        if asyncio.get_running_loop().time() - started >= _FAILED_ROW_BUDGET_S:
            logger.error(
                "gave up marking a start-deadline row failed session=%s",
                session_id,
            )
            return
        delay = min(delay * 2, 5.0)


async def _abort_timed_out_create(
    broker: Broker,
    session_id: str,
    target: str,
    exc: DomainRpcError,
) -> DomainRpcError:
    """Force-stop a worker whose create reply missed the API timeout.

    Only the process-per-session supervisor can do this. In-process mode
    keeps no worker slot, so force-stop answers ``not_found`` and a
    blocking ``on_start`` stays on that loop. A parent that is itself
    wedged will not answer either. What this catches is a reply that lost
    the slack after the start deadline: the worker may already be live,
    and leaving it that way would skip the MD attach.

    The abort carries no wall-clock deadline, and a timeout is retried
    in the background until the STS acks or the row is terminal. The
    HTTP response does not wait for that. It names the session, because
    until the retry lands the worker may still be running.

    Sent to ``target``, the instance this deploy already resolved. A null
    ``instance`` on the row is not a reason to ask every STS. TD is
    attached only after a live create, so the kill does not leave a
    resting order.
    """
    row = await _load_sts_row(session_id)
    if _row_is_terminal(row):
        return _deadline_error(row)
    if row is None:
        return _may_be_live(exc, session_id)
    err = await _send_abort(broker, session_id, target)
    if err is not None and err.code == "not_found":
        row = await _poll_terminal(session_id)
    else:
        row = await _load_sts_row(session_id)
        if not _row_is_terminal(row):
            row = await _poll_terminal(session_id)
    if _row_is_terminal(row):
        return _deadline_error(row)
    if err is None or err.code in {"timeout", "expired"}:
        logger.warning(
            "create timeout force-stop has not landed session=%s",
            session_id,
        )
        _schedule_abort_retry(broker, session_id, target)
    elif err.code != "not_found":
        logger.warning(
            "create timeout force-stop failed session=%s code=%s",
            session_id,
            err.code,
        )
        # The supervisor killed the worker and then could not write the
        # row. The process is gone; leaving the row ``live`` is what the
        # reaper rebuilds. Once that write lands the create is the same
        # deadline failure as one STS reported itself.
        if err.code.endswith("_failed"):
            await _ensure_start_failed(session_id, start_deadline_reason())
            try:
                settled = await _load_sts_row(session_id)
            except Exception:
                logger.exception(
                    "could not read a start-deadline row session=%s",
                    session_id,
                )
                settled = None
            if _row_is_terminal(settled):
                return _deadline_error(settled)
    return _may_be_live(exc, session_id)


async def _check_md_instances(broker: Broker, md: dict[str, list[str]]) -> None:
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
            row.name: row for row in await repo.list_all(domain=SessionDomain.MD.value)
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


async def _sts_target(instance: str | None, td: dict[str, TdAccountRef]) -> str:
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


async def _check_sts_instance(broker: Broker, instance: str | None) -> None:
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
        logger.exception("%s instance probe failed instance=%s", domain, instance)
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
                    MdDetachRequest(session_id=session_id, reason="deploy_rollback"),
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
        raise DomainRpcError("unknown_api", f"no credential with api_id={api_id}")
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

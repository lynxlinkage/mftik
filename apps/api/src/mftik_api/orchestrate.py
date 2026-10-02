"""API start / end (IF-13, §8.1, F12, F38).

The synchronous create → MD attach → TD attach sequence and its rollback
(``_detach_md``, ``_fail_sts``) are gone (RM-08). :func:`start` validates,
writes the session spec, registers intents, and asks STS to accept. A
refusal before that accept marks the row failed and releases the intents
(§8.1). :func:`end` asks a session that was accepted to stop, and then
releases intents.

**State authority (§3.3).**

* The session spec — strategy, parameters, ``restart``, the submitted
  document — is this module's. It is written with
  :meth:`StsSessionRepository.create_live`. ``restart`` defaults to
  ``never`` there (F11).
* Session status after an accept — phase, conditions, incarnation,
  ``restart_count``, the failure reason — is the STS controller's.
  ``create_live`` stores ``live``, the column's historical insert.
  A refusal before the accept is this module's to record: the
  supervisor never received the session, and a ``live`` row would
  have no one to clear it. The accept body says ``starting``.
  Progress on that body is null: the ingress has not reported a hook.
* MD and TD intent rows are written here at start, and by the STS
  controller when it heals. :meth:`IntentRepository.release` sets
  ``released_at`` and does not delete (F38). A session that exits on
  its own is released by the B5 STS orchestrator. §8.2 rule 3
  (B3-04) reclaims from the liveness report as the fallback. This
  module does not.
* ``strategy_digest`` and ``env_generation`` are columns (IF-16,
  migration ``0036``). This module does not resolve or write them.
  Pinning them at start is B5-10. ``create_live`` leaves both null.

**Invariants.**

* **F12** After the accept (HTTP 202), a failure is the supervisor's
  to record (§5.2). This module does not roll that back. A refusal
  before the accept is §8.1: the rollback of a start that was not
  accepted.   :func:`start` best-effort sends ``sts.session.end`` when
  the start reply is an unclear timeout or the accept is cancelled
  while that call is in flight, best-effort deletes the
  intents it already put, then ``mark_failed`` and ``release``. The
  row stays. It is ``failed``, with a reason, so registry delete and
  the board do not treat it as running. A cancel is shielded so the
  rollback itself is not cancelled, then raised again (#314).
* **P-1** Intent puts are idempotent. The repository replaces the set.
  It does not refcount.
* **§8.1** End is ``sts.session.end``, then ``md.intent.delete`` and
  ``td.intent.delete``. The subject of the first call is
  :func:`end_subject` — ``sts.{instance}`` for the owner instance
  (B4-02, issue #298) — and nowhere else.
* A reply is checked with :func:`mftik.protocol.reject_if_pv_mismatch`
  on the raw frame before it is parsed (F26, B4-01). ``Broker.request``
  parses first and does not (issue #282), so this module reads the
  frame from the transport.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Sequence
from typing import Any

from mftik.broker import Broker
from mftik.broker.errors import NoRespondersError, RequestTimeoutError
from mftik.protocol import (
    ANY_INSTANCE,
    MD_ERROR,
    MD_INTENT_DELETE,
    MD_INTENT_PUT,
    ON_STOP_TIMEOUT_S,
    STS_ERROR,
    STS_SESSION_END,
    STS_SESSION_START,
    TD_ERROR,
    TD_INTENT_DELETE,
    TD_INTENT_PUT,
    Envelope,
    HealthCheck,
    IntentOwner,
    MdIntentDelete,
    MdIntentDeleteResult,
    MdIntentPut,
    MdIntentPutResult,
    RpcError,
    StrategySpec,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionEndRequest,
    StsSessionEndResult,
    TdAccountRef,
    TdIntentDelete,
    TdIntentDeleteResult,
    TdIntentPut,
    TdIntentPutResult,
    Topics,
    dump_td,
    load_td,
    reject_if_pv_mismatch,
    td_api_ids_of,
)
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import (
    AccountRepository,
    ApiRepository,
    InstanceRepository,
    IntentRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope
from pydantic import BaseModel, ValidationError

from mftik_api.broker_rpc import DomainRpcError
from mftik_api.schemas import DeployResponse

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


#: How long the accept RPCs wait. This is not ``on_start`` and it is not
#: the old 10 second create (F12, #132). A plane with no subscriber
#: fails at once; this bound is only the case where someone accepted
#: the subject and did not answer.
_ACCEPT_TIMEOUT_S = 5.0

#: How long ``sts.session.end`` may take. The controller waits for the
#: worker's ``on_stop`` and up to ``SESSION_STOP_GRACE_S`` before it
#: replies, so :data:`_ACCEPT_TIMEOUT_S` alone times out a stop that
#: succeeds on the controller. Provisional, pending Yi Te (#286).
#: ``SESSION_STOP_GRACE_S`` is :data:`ON_STOP_TIMEOUT_S` today; B4-03
#: may add a teardown margin, and this sum has to stay above that grace
#: and under the CLI's 30s HTTP timeout.
_END_TIMEOUT_S = ON_STOP_TIMEOUT_S + _ACCEPT_TIMEOUT_S

_ERROR_TYPES = frozenset({STS_ERROR, TD_ERROR, MD_ERROR})

#: One MD declaration: instance, feed keys, select blocks.
_MdDecl = tuple[str, list[str], list[Any]]


class _AcceptSent:
    """Which accept RPCs were handed to the transport.

    Recorded before the reply, so a timeout still counts as sent.
    """

    def __init__(self) -> None:
        self.td: list[tuple[str, list[int]]] = []
        self.md: list[str] = []
        self.start: bool = False


def end_subject(instance: str) -> str:
    """Subject :func:`end` publishes :data:`STS_SESSION_END` on.

    The controller's subject, :meth:`~mftik.protocol.Topics.sts`, for
    the STS instance that owns the session (B4-02, issue #298). The
    worker's ``sts.ctl.{session_id}`` is not this. Nothing else in this
    module names the subject.
    """
    return Topics.sts(instance)


def _md_declarations(spec: StrategySpec) -> list[_MdDecl]:
    """Instances that have a feed or a selector, in document order."""
    names = list(dict.fromkeys([*spec.md.keys(), *spec.md_select.keys()]))
    out: list[_MdDecl] = []
    for name in names:
        feeds = list(spec.md.get(name, []))
        selects = list(spec.md_select.get(name, []))
        if not feeds and not selects:
            continue
        out.append((name, feeds, selects))
    return out


def _md_probe(declarations: Sequence[_MdDecl]) -> dict[str, list[str]]:
    """Feed lists :func:`_check_md_instances` will not treat as absent.

    An empty list means "this name pinned nothing", so a select-only
    instance would skip the declared-and-answering check. The
    placeholder is not stored and not sent.
    """
    probed: dict[str, list[str]] = {}
    for name, feeds, _selects in declarations:
        probed[name] = list(feeds) if feeds else ["*"]
    return probed


def _md_subject(instance: str) -> str:
    if instance == ANY_INSTANCE:
        return Topics.MD
    return Topics.md(instance)


async def _resolve_td(spec: StrategySpec) -> dict[str, TdAccountRef]:
    """Account name → ``api_id``. A name with no row is ``unknown_api``."""
    if not spec.td:
        return {}
    async with session_scope() as db:
        repo = AccountRepository(db)
        out: dict[str, TdAccountRef] = {}
        for name, settings in spec.td.items():
            account = await repo.get_by_name(name)
            if account is None or account.api is None:
                raise DomainRpcError(
                    "unknown_api",
                    f"unknown td account name: {name!r}",
                )
            out[name] = TdAccountRef(api_id=account.api_id, settings=settings)
        return out


async def _td_by_instance(
    td: dict[str, TdAccountRef],
) -> dict[str, list[int]]:
    """``api_id``s grouped by the TD instance :func:`_td_instance` names.

    One credential, one instance. Two accounts on the same TD share one
    ``td.intent.put``.
    """
    groups: dict[str, list[int]] = {}
    seen: set[int] = set()
    for ref in td.values():
        if ref.api_id in seen:
            continue
        seen.add(ref.api_id)
        name = await _td_instance(ref.api_id)
        groups.setdefault(name, []).append(ref.api_id)
    return groups


async def _request_v2[T: BaseModel](
    broker: Broker,
    subject: str,
    envelope: Envelope[Any],
    *,
    result_type: type[T],
    timeout: float = _ACCEPT_TIMEOUT_S,
) -> T:
    """Request, then refuse a reply whose ``pv`` is not ours (F26).

    The raw frame is what :func:`reject_if_pv_mismatch` reads. Parsing
    first would fill in a missing ``pv`` and hide the mismatch
    (issue #282). The transport is the only place that frame is still
    a string.
    """
    inbox = broker.transport.reply_inbox(envelope.id)
    if inbox is not None and envelope.reply_to != inbox:
        envelope = envelope.model_copy(update={"reply_to": inbox})
    try:
        raw = await broker.transport.request(
            subject,
            envelope.to_json(),
            request_id=envelope.id,
            inbox=envelope.reply_to,
            timeout=timeout,
        )
    except NoRespondersError as exc:
        raise DomainRpcError("timeout", str(exc), no_responders=True) from exc
    except RequestTimeoutError as exc:
        raise DomainRpcError("timeout", str(exc)) from exc

    try:
        mismatch = reject_if_pv_mismatch(raw)
    except ValueError as exc:
        raise DomainRpcError("bad_reply", "reply is not a JSON object") from exc
    if mismatch is not None:
        raise DomainRpcError(mismatch.code, mismatch.message)

    reply = Envelope[dict[str, Any]].from_json(raw)
    if reply.type in _ERROR_TYPES:
        try:
            err = RpcError.model_validate(reply.payload)
        except ValidationError as exc:
            raise DomainRpcError(
                "bad_reply", "error reply payload did not match"
            ) from exc
        raise DomainRpcError(err.code, err.message)
    try:
        return result_type.model_validate(reply.payload)
    except ValidationError as exc:
        raise DomainRpcError(
            "bad_reply", "reply payload did not match"
        ) from exc


async def _persist_start(
    spec: StrategySpec,
    *,
    session_id: str,
    created_by: int,
    strategy_type: str,
    yaml_text: str,
    instance: str | None,
    td: dict[str, TdAccountRef],
    target: str,
    declarations: Sequence[_MdDecl],
) -> None:
    """Write the spec and the intent rows. One transaction. No RPC."""
    owner = IntentOwner(sts_instance=target, session_id=session_id)
    async with session_scope() as db:
        await StsSessionRepository(db).create_live(
            session_id=session_id,
            created_by=created_by,
            type=strategy_type,
            yaml_text=yaml_text,
            td=dump_td(td),
            md_ids=dict(spec.md),
            st_paras=dict(spec.sts),
            restart=spec.restart,
            instance=instance,
        )
        repo = IntentRepository(db)
        api_ids = td_api_ids_of(td)
        if api_ids:
            await repo.put(
                TdIntentPut(
                    session_id=session_id,
                    owner=owner,
                    api_ids=api_ids,
                )
            )
        for name, feeds, selects in declarations:
            await repo.put(
                MdIntentPut(
                    session_id=session_id,
                    owner=owner,
                    feeds={name: feeds},
                    selects=selects,
                )
            )


async def _publish_start(
    broker: Broker,
    *,
    session_id: str,
    created_by: int,
    strategy_type: str,
    yaml_text: str,
    instance: str | None,
    td: dict[str, TdAccountRef],
    target: str,
    td_groups: dict[str, list[int]],
    declarations: Sequence[_MdDecl],
    spec: StrategySpec,
    sent: _AcceptSent,
) -> None:
    """``td.intent.put``, ``md.intent.put``, then ``sts.session.start``.

    The rows are already committed. ``sent`` records each call before
    it is made, so a refusal knows which planes were asked. Atom maps
    on the MD reply are MD's to store; this does not write ``atoms``.
    """
    owner = IntentOwner(sts_instance=target, session_id=session_id)
    for td_instance, api_ids in td_groups.items():
        sent.td.append((td_instance, list(api_ids)))
        await _request_v2(
            broker,
            Topics.td(td_instance),
            Envelope[TdIntentPut].wrap(
                TdIntentPut(
                    session_id=session_id,
                    owner=owner,
                    api_ids=api_ids,
                ),
                type=TD_INTENT_PUT,
                source="api",
                session_id=session_id,
            ),
            result_type=TdIntentPutResult,
        )
    for name, feeds, selects in declarations:
        sent.md.append(name)
        await _request_v2(
            broker,
            _md_subject(name),
            Envelope[MdIntentPut].wrap(
                MdIntentPut(
                    session_id=session_id,
                    owner=owner,
                    feeds={name: list(feeds)},
                    selects=list(selects),
                ),
                type=MD_INTENT_PUT,
                source="api",
                session_id=session_id,
            ),
            result_type=MdIntentPutResult,
        )
    sent.start = True
    await _request_v2(
        broker,
        Topics.sts(target),
        Envelope[StsCreateSessionRequest].wrap(
            StsCreateSessionRequest(
                session_id=session_id,
                created_by=created_by,
                strategy=strategy_type,
                td=td,
                md=dict(spec.md),
                st_paras=dict(spec.sts),
                restart=spec.restart,
                type=strategy_type,
                yaml_text=yaml_text,
                instance=instance,
            ),
            type=STS_SESSION_START,
            source="api",
            session_id=session_id,
        ),
        result_type=StsCreateSessionResult,
    )


def _refused_reason(exc: DomainRpcError) -> str:
    return f"start not accepted: {exc.code}: {exc.message}"


async def _best_effort_v2(
    broker: Broker,
    subject: str,
    envelope: Envelope[Any],
    *,
    result_type: type[BaseModel],
    session_id: str,
) -> None:
    """One abandon notify. A failure is logged and does not propagate."""
    try:
        await _request_v2(
            broker, subject, envelope, result_type=result_type
        )
    except DomainRpcError as exc:
        logger.error(
            "unaccepted start notify failed session=%s subject=%s: %s",
            session_id,
            subject,
            exc,
        )


async def _abandon_unaccepted(
    broker: Broker,
    *,
    session_id: str,
    target: str,
    sent: _AcceptSent,
    exc: DomainRpcError,
) -> None:
    """§8.1 rollback of a start the planes did not accept.

    An unclear ``sts.session.start`` — timed out, and somebody was
    subscribed — may have created a worker. That case sends
    ``sts.session.end`` on :func:`end_subject` of the STS the start was
    sent to. A ``no_responders`` miss and any definite refusal do not:
    nothing accepted the start. Deletes go only to instances whose put
    was sent, MD then TD, the same order as :func:`end`.

    The row is then ``failed`` and every intent is released. Neither
    is deleted.
    """
    reason = _refused_reason(exc)
    owner = IntentOwner(sts_instance=target, session_id=session_id)
    # A timeout with a subscriber, or a cancel while the start was in
    # flight, may already have created a worker. A miss and a definite
    # refusal have not.
    unclear = sent.start and (
        exc.code == "cancelled"
        or (exc.code == "timeout" and not exc.no_responders)
    )
    if unclear:
        await _best_effort_v2(
            broker,
            end_subject(target),
            Envelope[StsSessionEndRequest].wrap(
                StsSessionEndRequest(session_id=session_id, reason=reason),
                type=STS_SESSION_END,
                source="api",
                session_id=session_id,
            ),
            result_type=StsSessionEndResult,
            session_id=session_id,
        )
    for md_instance in sent.md:
        await _best_effort_v2(
            broker,
            _md_subject(md_instance),
            Envelope[MdIntentDelete].wrap(
                MdIntentDelete(
                    session_id=session_id,
                    owner=owner,
                    reason=reason,
                ),
                type=MD_INTENT_DELETE,
                source="api",
                session_id=session_id,
            ),
            result_type=MdIntentDeleteResult,
            session_id=session_id,
        )
    for td_instance, api_ids in sent.td:
        await _best_effort_v2(
            broker,
            Topics.td(td_instance),
            Envelope[TdIntentDelete].wrap(
                TdIntentDelete(
                    session_id=session_id,
                    owner=owner,
                    api_ids=api_ids,
                    reason=reason,
                ),
                type=TD_INTENT_DELETE,
                source="api",
                session_id=session_id,
            ),
            result_type=TdIntentDeleteResult,
            session_id=session_id,
        )
    async with session_scope() as db:
        await StsSessionRepository(db).mark_failed(session_id, reason)
        await IntentRepository(db).release(session_id)


async def _abandon_recorded(
    broker: Broker,
    *,
    session_id: str,
    target: str,
    sent: _AcceptSent,
    exc: DomainRpcError,
    shield: bool = False,
) -> None:
    """Run :func:`_abandon_unaccepted` and log if that record fails.

    ``shield`` is the cancel path. The task is already cancelled, so a
    bare await would raise again before the row was marked failed.
    Uncancelling for the cleanup and restoring the request afterwards
    is what lets the rollback finish and the cancel still propagate.
    """
    task = asyncio.current_task()
    count = task.cancelling() if shield and task is not None else 0
    if task is not None and count:
        while task.cancelling():
            task.uncancel()
    try:
        try:
            rollback = _abandon_unaccepted(
                broker,
                session_id=session_id,
                target=target,
                sent=sent,
                exc=exc,
            )
            if shield:
                await asyncio.shield(rollback)
            else:
                await rollback
        except Exception:
            logger.exception(
                "refused start was not fully recorded session=%s",
                session_id,
            )
    finally:
        if task is not None:
            for _ in range(count):
                task.cancel()


async def start(
    spec: StrategySpec,
    *,
    broker: Broker,
    strategy_type: str,
    yaml_text: str,
    created_by: int,
    instance: str | None = None,
) -> DeployResponse:
    """Accept a deploy and return before the session is running (§8.1).

    The body is HTTP 202: ``session_id`` and ``status="starting"``.
    ``progress`` is null. ``td`` and ``md`` are empty — the accept does
    not wait for attach results.

    Validation (the document is already parsed) resolves account names
    to ``api_id``, resolves the STS and MD instances, and reuses
    :func:`_sts_target`, :func:`_check_sts_instance`,
    :func:`_check_md_instances` and :func:`_td_instance`. It does not
    dry-run feeds into atoms or check MD capacity. Those need the atom
    adapters (B7, B8), which are not this ticket. Nothing is written
    until the checks pass.

    The spec row is :meth:`StsSessionRepository.create_live`. Its
    ``instance`` is the name the deploy asked for, null when the deploy
    named none. The owner on the intent messages is the STS the start
    is sent to, which for an unnamed deploy is the derived target.
    ``strategy_digest`` and ``env_generation`` are columns on the row
    (IF-16). This function does not write them, and
    :class:`StsCreateSessionRequest` does not carry them. Pinning the
    pair is B5-10.

    A plane that refuses the accept does not leave a ``live`` row.
    :func:`_abandon_unaccepted` notifies what was already sent, marks
    the row failed, releases the intents, and the original
    :class:`DomainRpcError` is raised again. The HTTP mapping is that
    error's.
    """
    if not isinstance(spec, StrategySpec):
        raise TypeError("spec must be a StrategySpec")
    td = await _resolve_td(spec)
    target = await _sts_target(instance, td)
    await _check_sts_instance(broker, target)
    declarations = _md_declarations(spec)
    await _check_md_instances(broker, _md_probe(declarations))
    td_groups = await _td_by_instance(td)

    session_id = await mint_session_id()
    await _persist_start(
        spec,
        session_id=session_id,
        created_by=created_by,
        strategy_type=strategy_type,
        yaml_text=yaml_text,
        instance=instance,
        td=td,
        target=target,
        declarations=declarations,
    )
    sent = _AcceptSent()
    try:
        await _publish_start(
            broker,
            session_id=session_id,
            created_by=created_by,
            strategy_type=strategy_type,
            yaml_text=yaml_text,
            instance=instance,
            td=td,
            target=target,
            td_groups=td_groups,
            declarations=declarations,
            spec=spec,
            sent=sent,
        )
    except DomainRpcError as exc:
        logger.error(
            "start was not accepted session=%s: %s",
            session_id,
            exc,
        )
        await _abandon_recorded(
            broker,
            session_id=session_id,
            target=target,
            sent=sent,
            exc=exc,
        )
        raise
    except asyncio.CancelledError:
        # The client hung up before the accept reply (#314). The same
        # rollback as a refusal, shielded so the cancel does not skip
        # the failed row. The cancel is raised again.
        await _abandon_recorded(
            broker,
            session_id=session_id,
            target=target,
            sent=sent,
            exc=DomainRpcError(
                "cancelled",
                "accept cancelled before the reply",
            ),
            shield=True,
        )
        raise

    return DeployResponse(
        session_id=session_id,
        type=strategy_type,
        config=dict(spec.sts),
        status="starting",
        progress=None,
    )


async def _owner_instance(session_id: str) -> str:
    """The STS instance :func:`end` addresses. Raises before any send.

    The row's ``instance`` when the deploy named one, otherwise
    :func:`_sts_target` on the row's accounts — the same derivation the
    deletes already use. A missing row, or a row that still cannot name
    an instance, raises and the caller does not release.
    """
    async with session_scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(session_id)
        asked = None if row is None else row.instance
        td_map = load_td(row.td) if row is not None else {}
    if row is None:
        raise DomainRpcError(
            "not_found",
            f"session {session_id} does not exist",
        )
    owner = asked
    if owner is None and td_map:
        owner = await _sts_target(None, td_map)
    if owner is None:
        raise DomainRpcError(
            "sts_unpinned_ambiguous",
            "this session does not name an STS instance and its accounts "
            "do not derive to exactly one enabled STS",
        )
    return owner


async def end(
    session_id: str, reason: str, *, broker: Broker
) -> StsSessionEndResult:
    """Stop a session, then release its intents (§8.1).

    The first call is :data:`STS_SESSION_END` on :func:`end_subject` of
    the owner instance: the row's ``instance`` when it is set, otherwise
    :func:`_sts_target` on the row's accounts. That is computed before
    the send. An owner that cannot be named raises
    :class:`~mftik_api.broker_rpc.DomainRpcError` and does not send and
    does not release. Only a successful reply releases rows. Release
    sets ``released_at`` and does not delete (F38). The MD and TD
    deletes are the same fact told to those planes; a notify that fails
    is not undone.

    The end request waits :data:`_END_TIMEOUT_S`, not the accept budget.
    The controller does not reply until the worker has exited or the
    stop grace has run out.

    Returns the controller's :class:`StsSessionEndResult`. The terminal
    status on that reply is what the stop route answers with.

    There is no spec column for desired ``stopped``. An ``end`` whose
    reply never comes is an error for the caller to retry. This
    function does not add a column, and it does not write the derived
    owner back onto the row (issue #328).

    A session that exits on its own does not pass through here. The
    B5 STS orchestrator writes ``released_at`` for that exit. §8.2
    rule 3 (B3-04) reclaims an owner from the liveness report as the
    fallback.

    An unnamed deploy leaves ``sts_sessions.instance`` null. The owner
    used for the deletes is that column when it is set, and otherwise
    :func:`_sts_target` on the row's accounts.
    """
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id is required")
    if not isinstance(reason, str):
        raise TypeError("reason must be a str")

    owner_instance = await _owner_instance(session_id)
    result = await _request_v2(
        broker,
        end_subject(owner_instance),
        Envelope[StsSessionEndRequest].wrap(
            StsSessionEndRequest(session_id=session_id, reason=reason),
            type=STS_SESSION_END,
            source="api",
            session_id=session_id,
        ),
        result_type=StsSessionEndResult,
        timeout=_END_TIMEOUT_S,
    )
    await release_held_intents(session_id, reason, broker=broker)
    return result


async def release_held_intents(
    session_id: str, reason: str, *, broker: Broker
) -> None:
    """Release intent rows and tell MD and TD. No ``sts.session.end``.

    The stop route calls this for a row whose column is already
    terminal. After a controller restart that process no longer holds
    the session and would answer ``unknown_session``, so the end RPC
    is not sent. Nothing is sent when no intent is still held.

    The same sequence follows a successful :func:`end`.
    """
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id is required")
    if not isinstance(reason, str):
        raise TypeError("reason must be a str")

    async with session_scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(session_id)
        repo = IntentRepository(db)
        md_instances = [
            held.instance
            for held in await repo.md_for(session_id)
            if held.released_at is None
        ]
        td_ids = [
            held.api_id
            for held in await repo.td_for(session_id)
            if held.released_at is None
        ]
        asked = None if row is None else row.instance
        td_map = load_td(row.td) if row is not None else {}
        await repo.release(session_id)

    owner_instance = asked
    if owner_instance is None and td_map:
        owner_instance = await _sts_target(None, td_map)
    if owner_instance is None:
        if md_instances or td_ids:
            logger.error(
                "end released intents but could not name an STS owner "
                "to notify session=%s",
                session_id,
            )
        return

    owner = IntentOwner(sts_instance=owner_instance, session_id=session_id)
    for md_instance in md_instances:
        await _request_v2(
            broker,
            _md_subject(md_instance),
            Envelope[MdIntentDelete].wrap(
                MdIntentDelete(
                    session_id=session_id,
                    owner=owner,
                    reason=reason,
                ),
                type=MD_INTENT_DELETE,
                source="api",
                session_id=session_id,
            ),
            result_type=MdIntentDeleteResult,
        )
    groups: dict[str, list[int]] = {}
    for api_id in td_ids:
        groups.setdefault(await _td_instance(api_id), []).append(api_id)
    for td_instance, api_ids in groups.items():
        await _request_v2(
            broker,
            Topics.td(td_instance),
            Envelope[TdIntentDelete].wrap(
                TdIntentDelete(
                    session_id=session_id,
                    owner=owner,
                    api_ids=api_ids,
                    reason=reason,
                ),
                type=TD_INTENT_DELETE,
                source="api",
                session_id=session_id,
            ),
            result_type=TdIntentDeleteResult,
        )

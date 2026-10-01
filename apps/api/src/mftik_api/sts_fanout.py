"""Ask every declared STS, fail closed.

Env apply and registry mutation used to send one RPC on the anycast
subject ``sts``. With two instances only one process answers; the other
keeps its in-memory stamp until restart. MD attach and the event-log
listing already walk the ``instances`` table. This is that walk for the
control plane that must reach *every* interpreter.

Every enabled STS is addressed by name (``sts.{name}``), including when
only one is declared. A disabled process still subscribed to the shared
``sts`` subject must not be able to take a write meant for the enabled
instance. Anycast is only the non-authoritative fallback: no enabled
rows, or a table we cannot read. A timeout, RPC error, or that fallback
fails the whole fan-out: a write has already committed, and the caller
reports the miss rather than pretending every process saw it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from mftik.broker import Broker
from mftik.envapply import APPLY_TIMEOUT_S
from mftik.environment import EnvStamp
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_LOADED,
    STS_REGISTRY_RELOAD,
    STS_REGISTRY_SYNC,
    StsEnvPackagePin,
    StsEnvSyncRequest,
    StsEnvSyncRequestEnvelope,
    StsEnvSyncResult,
    StsRegistryGenerationRequest,
    StsRegistryGenerationRequestEnvelope,
    StsRegistryGenerationResult,
    StsRegistryLoadedRequest,
    StsRegistryLoadedRequestEnvelope,
    StsRegistryLoadedResult,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    StsRegistrySyncRequest,
    StsRegistrySyncRequestEnvelope,
    StsRegistrySyncResult,
    StsRegistryTreeOp,
    Topics,
)
from mftik.registry import RegistryError, RegistryStore, qualify
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import InstanceRepository
from mftik_db.session import session_scope
from pydantic import BaseModel

from mftik_api.broker_rpc import DomainRpcError, request_domain

logger = logging.getLogger(__name__)

#: Installer cap plus a little for the reload that follows it. Instances
#: run in parallel, so two hosts cost one timeout, not two.
SYNC_TIMEOUT_S = APPLY_TIMEOUT_S + 30.0

#: Instance table empty or unreadable — anycast is not a census.
CENSUS_ERROR = "STS instance list is missing or unreadable"


@dataclass(frozen=True, slots=True)
class StsTarget:
    """One address. ``name`` is None when we fell back to the shared subject."""

    name: str | None
    subject: str
    authoritative: bool = True

    @property
    def label(self) -> str:
        return self.name or "sts"


@dataclass(frozen=True, slots=True)
class FanoutReply[T]:
    target: StsTarget
    result: T | None
    error: str | None


#: One JSON envelope must fit under NATS' default ``max_payload`` of 1 MiB.
#: A tree past this is refused with a named limit, not sent in pieces.
REGISTRY_SYNC_BUDGET = 700 * 1024


class RegistryPayloadTooLarge(Exception):
    """One tree cannot ride in a single broker message."""

    def __init__(self, key: str, nbytes: int) -> None:
        self.key = key
        self.nbytes = nbytes
        super().__init__(
            f"{key} encoded size is {nbytes} bytes, over the "
            f"{REGISTRY_SYNC_BUDGET} byte broker payload limit"
        )


@dataclass(frozen=True, slots=True)
class StsFanoutResult:
    """Intersection of loaded keys, whether every process matches the stamp.

    ``loaded_by`` and ``skipped`` are per instance. ``loaded`` stays the
    intersection: a push is deployable only when every STS has the key.
    ``skipped`` is label → qualified key → why that upsert is absent.
    """

    loaded: frozenset[str]
    in_sync: bool
    error: str | None
    loaded_by: dict[str, frozenset[str]] = field(default_factory=dict)
    skipped: dict[str, dict[str, str]] = field(default_factory=dict)


def pins_of_stamp(stamp: EnvStamp) -> dict[str, tuple[str, str]]:
    return {name: (rec.version, rec.dist) for name, rec in stamp.packages.items()}


def pins_of_wire(
    packages: dict[str, StsEnvPackagePin],
) -> dict[str, tuple[str, str]]:
    return {name: (pin.version, pin.dist) for name, pin in packages.items()}


def extras_match(
    generation: int,
    packages: dict[str, StsEnvPackagePin],
    stamp: EnvStamp,
    *,
    overlay_live: bool | None = None,
) -> bool:
    """Whether this process's in-memory extras are the stamp.

    Package equality is the real check (unshared volumes do not share
    generation counters). ``overlay_live is False`` means the process
    cannot import its stamp, so a non-empty stamp is a miss.

    ``overlay_live is None`` is a pre-upgrade reply: an empty
    ``packages`` falls back to generation. A current STS with a live
    empty overlay sends ``overlay_live=True`` and ``packages={}``.
    """
    want = pins_of_stamp(stamp)
    if overlay_live is False:
        return not want
    have = pins_of_wire(packages)
    if have == want:
        return True
    if overlay_live is None and not packages:
        return generation >= stamp.generation
    return False


def format_errors(replies: list[FanoutReply[Any]]) -> str | None:
    failed = [reply for reply in replies if reply.error]
    if not failed:
        return None
    return "; ".join(
        f"{reply.target.label} ({reply.error})" for reply in failed
    )


async def list_targets() -> list[StsTarget]:
    """Enabled STS subjects, each by name.

    Zero enabled rows, or a table we cannot read, still send anycast so
    the process that answers can adopt the write — but the target is
    marked not authoritative, and the caller must not claim in-sync.
    One enabled row is ``sts.{name}`` too. Anycast would let a disabled
    process that is still subscribed to ``sts`` take the write, and the
    reply would be labeled with the enabled instance.
    """
    try:
        async with session_scope() as db:
            rows = await InstanceRepository(db).list_all(
                domain=SessionDomain.STS.value
            )
        enabled = [row for row in rows if row.enabled]
    except Exception:
        logger.warning(
            "sts fan-out: could not list instances — asking the shared "
            "subject, answer is not authoritative",
            exc_info=True,
        )
        return [
            StsTarget(name=None, subject=Topics.STS, authoritative=False)
        ]
    if not enabled:
        logger.warning(
            "sts fan-out: no enabled STS instance is declared — asking "
            "the shared subject, answer is not authoritative"
        )
        return [
            StsTarget(name=None, subject=Topics.STS, authoritative=False)
        ]
    return [
        StsTarget(name=row.name, subject=Topics.sts(row.name))
        for row in enabled
    ]


async def fanout[T: BaseModel](
    broker: Broker,
    *,
    make_envelope: Callable[[], Any],
    result_type: type[T],
    timeout: float = 5.0,
    targets: list[StsTarget] | None = None,
) -> list[FanoutReply[T]]:
    """Send one fresh envelope per target. Reusing an id would collide replies.

    ``targets`` overrides the census. A reconcile of one instance that just
    started must not wait on every other STS.
    """
    if targets is None:
        targets = await list_targets()

    async def one(target: StsTarget) -> FanoutReply[T]:
        try:
            result = await request_domain(
                broker,
                target.subject,
                make_envelope(),
                result_type=result_type,
                timeout=timeout,
            )
            return FanoutReply(target=target, result=result, error=None)
        except DomainRpcError as exc:
            logger.warning(
                "STS %s on %s failed: %s",
                target.label,
                target.subject,
                exc.message,
            )
            return FanoutReply(target=target, result=None, error=exc.message)

    return list(await asyncio.gather(*(one(target) for target in targets)))


def _intersect_loaded(
    replies: list[FanoutReply[Any]],
) -> frozenset[str]:
    loaded: set[str] | None = None
    for reply in replies:
        if reply.result is None:
            continue
        keys = set(reply.result.loaded)
        loaded = keys if loaded is None else loaded & keys
    return frozenset(loaded or ())


def _per_target(
    replies: list[FanoutReply[Any]],
) -> tuple[dict[str, frozenset[str]], dict[str, dict[str, str]]]:
    loaded_by: dict[str, frozenset[str]] = {}
    skipped: dict[str, dict[str, str]] = {}
    for reply in replies:
        if reply.result is None:
            continue
        loaded_by[reply.target.label] = frozenset(reply.result.loaded)
        raw = getattr(reply.result, "skipped", None)
        if raw:
            skipped[reply.target.label] = dict(raw)
    return loaded_by, skipped


@dataclass(frozen=True, slots=True)
class SyncPlan:
    """Ops that fit in a message, and upserts that do not.

    An oversized tree is not sliced and is not a reason to drop the rest.
    The caller still sends a rescan so a shared disk can load it.
    """

    batches: list[list[StsRegistryTreeOp]]
    oversized: list[tuple[str, int]]


def sync_batches(ops: list[StsRegistryTreeOp]) -> list[list[StsRegistryTreeOp]]:
    """Split ops so each envelope stays under :data:`REGISTRY_SYNC_BUDGET`.

    An empty op list is one empty batch: the caller still wants a rescan.
    Oversized upserts are omitted here; :func:`plan_sync` reports them.
    """
    return plan_sync(ops).batches


def plan_sync(ops: list[StsRegistryTreeOp]) -> SyncPlan:
    """Batch what fits. Name what does not, and still leave a rescan batch."""
    oversized: list[tuple[str, int]] = []
    shippable: list[StsRegistryTreeOp] = []
    for op in ops:
        if op.op == "upsert":
            size = len(op.model_dump_json().encode())
            if size > REGISTRY_SYNC_BUDGET:
                oversized.append((qualify(op.origin, op.name), size))
                continue
        shippable.append(op)
    if not shippable:
        return SyncPlan(batches=[[]], oversized=oversized)
    batches: list[list[StsRegistryTreeOp]] = []
    current: list[StsRegistryTreeOp] = []
    current_size = 2
    for op in shippable:
        size = len(op.model_dump_json().encode())
        if current and current_size + size + 1 > REGISTRY_SYNC_BUDGET:
            batches.append(current)
            current = []
            current_size = 2
        current.append(op)
        current_size += size + 1
    if current:
        batches.append(current)
    return SyncPlan(batches=batches, oversized=oversized)


_ABSENT_ON_DISK = "not present on this registry disk"


def _note_oversized(
    loaded_by: dict[str, frozenset[str]],
    skipped: dict[str, dict[str, str]],
    oversized: list[tuple[str, int]],
) -> None:
    """Say why a tree that could not be shipped is still absent.

    A rescan that loaded it (the file was already on that disk) stays
    loaded. An import error already recorded is more specific than the
    size, and wins.
    """
    for key, nbytes in oversized:
        reason = str(RegistryPayloadTooLarge(key, nbytes))
        for label, keys in loaded_by.items():
            current = skipped.get(label, {}).get(key)
            if key in keys and current is None:
                continue
            if current is not None and current != _ABSENT_ON_DISK:
                continue
            skipped.setdefault(label, {})[key] = reason


def _without_skipped(
    loaded: frozenset[str], skipped: dict[str, dict[str, str]]
) -> frozenset[str]:
    """A key an STS skipped did not take this write, even if the old class remains."""
    bad = {key for reasons in skipped.values() for key in reasons}
    return frozenset(key for key in loaded if key not in bad)


async def reload_sts(broker: Broker) -> StsFanoutResult:
    """``sts.registry.reload`` on every enabled STS.

    Loaded keys are the intersection of every reply.
    """
    replies = await fanout(
        broker,
        make_envelope=lambda: StsRegistryReloadRequestEnvelope.wrap(
            StsRegistryReloadRequest(),
            type=STS_REGISTRY_RELOAD,
            source="api",
        ),
        result_type=StsRegistryReloadResult,
        timeout=30.0,
    )
    return _finish(replies, in_sync=True)


def _stamp_pins(stamp: EnvStamp) -> dict[str, StsEnvPackagePin]:
    return {
        name: StsEnvPackagePin(
            version=rec.version, dist=rec.dist, source=rec.source
        )
        for name, rec in stamp.packages.items()
    }


def _census_ok(replies: list[FanoutReply[Any]]) -> bool:
    return all(reply.target.authoritative for reply in replies)


def _finish(
    replies: list[FanoutReply[Any]],
    *,
    in_sync: bool,
) -> StsFanoutResult:
    error = format_errors(replies)
    if error is not None:
        return StsFanoutResult(loaded=frozenset(), in_sync=False, error=error)
    if not _census_ok(replies):
        return StsFanoutResult(
            loaded=frozenset(), in_sync=False, error=CENSUS_ERROR
        )
    loaded_by, skipped = _per_target(replies)
    return StsFanoutResult(
        loaded=_intersect_loaded(replies),
        in_sync=in_sync,
        error=None,
        loaded_by=loaded_by,
        skipped=skipped,
    )


#: Serialises a store mutation with the reconcile that snapshots it.
#: ``store.add`` used to finish before this lock was taken. A reconcile
#: that had already read ``retain`` then pruned the new tree, and on a
#: shared volume that prune is the API's own disk. An oversized tree is
#: not in the following sync payload, so nothing writes it back.
_registry_lock = asyncio.Lock()


@asynccontextmanager
async def registry_mutation():
    """Hold :data:`_registry_lock` across a disk write and its sync."""
    async with _registry_lock:
        yield


def registry_manifest() -> tuple[list[StsRegistryTreeOp], list[str]]:
    """Every tree on the API disk, and the keys a reconcile must keep.

    Raises when the registry directory is missing or a tree cannot be
    read. The caller must not prune on that failure: an empty read and a
    failed read are different, and only the first means the disk should
    match nothing.
    """
    store = RegistryStore.from_env()
    if not store.registry_dir.is_dir():
        raise RegistryError(
            "API registry directory is missing; refusing to prune STS disks"
        )
    records = store.list_all()
    ops: list[StsRegistryTreeOp] = []
    retain: list[str] = []
    for rec in records:
        key = qualify(rec.origin, rec.name)
        retain.append(key)
        files = store.read_contents(rec)
        ops.append(
            StsRegistryTreeOp(
                op="upsert",
                origin=rec.origin,
                name=rec.name,
                digest=rec.digest,
                files=files,
            )
        )
    return ops, retain


async def sync_registry(
    broker: Broker,
    ops: list[StsRegistryTreeOp],
    *,
    retain: list[str] | None = None,
    targets: list[StsTarget] | None = None,
) -> StsFanoutResult:
    """Copy ``ops`` onto every enabled STS, then rescan on the last batch."""
    async with _registry_lock:
        return await _sync_unlocked(
            broker, ops, retain=retain, targets=targets
        )


async def sync_registry_locked(
    broker: Broker,
    ops: list[StsRegistryTreeOp],
    *,
    retain: list[str] | None = None,
    targets: list[StsTarget] | None = None,
) -> StsFanoutResult:
    """:func:`sync_registry` while the caller holds :func:`registry_mutation`."""
    if not _registry_lock.locked():
        raise RuntimeError(
            "sync_registry_locked requires registry_mutation; "
            "the write and the sync have to be one critical section"
        )
    return await _sync_unlocked(broker, ops, retain=retain, targets=targets)


async def reconcile_instance(broker: Broker, name: str) -> StsFanoutResult:
    """Push the API store to ``name``, and delete trees the store lacks.

    One instance, not the census. A process that just started, or a row
    that was just enabled, is the only disk that can be behind.
    """
    async with _registry_lock:
        try:
            ops, retain = registry_manifest()
        except Exception as exc:
            logger.exception(
                "registry reconcile refused to read the API store"
            )
            return StsFanoutResult(
                loaded=frozenset(),
                in_sync=False,
                error=f"API registry could not be read: {exc}",
            )
        return await _sync_unlocked(
            broker,
            ops,
            retain=retain,
            targets=[
                StsTarget(
                    name=name,
                    subject=Topics.sts(name),
                    authoritative=True,
                )
            ],
        )


async def _sync_unlocked(
    broker: Broker,
    ops: list[StsRegistryTreeOp],
    *,
    retain: list[str] | None = None,
    targets: list[StsTarget] | None = None,
) -> StsFanoutResult:
    """Copy ``ops``, then rescan. Caller holds :data:`_registry_lock`.

    A tree over the broker budget is left out of the payload and still
    gets a rescan, so a shared disk can load a file that is already
    there. ``retain`` on the last batch deletes keys the API store does
    not have. ``None`` does not prune.
    """
    plan = plan_sync(ops)
    merged: dict[str, dict[str, str]] = {}
    replies: list[FanoutReply[Any]] = []
    batches = plan.batches
    for index, batch in enumerate(batches):
        last = index == len(batches) - 1
        # The rescan runs on the last batch only, so it is the only place
        # that can say why an earlier batch's upsert did not load.
        earlier = (
            [
                qualify(op.origin, op.name)
                for prior in batches[:index]
                for op in prior
                if op.op == "upsert"
            ]
            if last
            else []
        )
        # ``retain`` may be an empty list, which means prune everything.
        # A falsy check would turn that into "do not prune".
        batch_retain = retain if last else None
        def _envelope(
            batch: list[StsRegistryTreeOp] = batch,
            last: bool = last,
            earlier: list[str] = earlier,
            batch_retain: list[str] | None = batch_retain,
        ) -> StsRegistrySyncRequestEnvelope:
            return StsRegistrySyncRequestEnvelope.wrap(
                StsRegistrySyncRequest(
                    trees=list(batch),
                    reload=last,
                    explain=earlier,
                    retain=batch_retain,
                ),
                type=STS_REGISTRY_SYNC,
                source="api",
            )

        replies = await fanout(
            broker,
            make_envelope=_envelope,
            result_type=StsRegistrySyncResult,
            timeout=30.0,
            targets=targets,
        )
        _loaded_by, skipped = _per_target(replies)
        for label, reasons in skipped.items():
            # The first reason wins: an earlier batch's write failure is more
            # specific than the last batch's rescan finding nothing there.
            into = merged.setdefault(label, {})
            for key, reason in reasons.items():
                into.setdefault(key, reason)
        error = format_errors(replies)
        if error is not None or not _census_ok(replies):
            return StsFanoutResult(
                loaded=frozenset(),
                in_sync=False,
                error=error or CENSUS_ERROR,
                loaded_by=_loaded_by,
                skipped=merged,
            )
    loaded_by, _skipped = _per_target(replies)
    _note_oversized(loaded_by, merged, plan.oversized)
    return StsFanoutResult(
        loaded=_without_skipped(_intersect_loaded(replies), merged),
        in_sync=True,
        error=None,
        loaded_by=loaded_by,
        skipped=merged,
    )


#: ``sts.registry.loaded`` is an in-memory read; a live STS answers at once.
REGISTRY_CENSUS_TIMEOUT_S = 1.5


async def registry_availability(
    broker: Broker,
) -> dict[str, frozenset[str]] | None:
    """Label → keys that process has loaded. None when nobody answered.

    A non-authoritative anycast answer is treated as unknown: the label
    would be ``sts``, which is not an instance name the picker can match.
    """
    replies = await fanout(
        broker,
        make_envelope=lambda: StsRegistryLoadedRequestEnvelope.wrap(
            StsRegistryLoadedRequest(),
            type=STS_REGISTRY_LOADED,
            source="api",
        ),
        result_type=StsRegistryLoadedResult,
        # Every picker load waits on this. A dead STS should not add the
        # full default RPC timeout to each page.
        timeout=REGISTRY_CENSUS_TIMEOUT_S,
    )
    answered = [
        reply
        for reply in replies
        if reply.result is not None and reply.target.authoritative
    ]
    if not answered:
        return None
    return {
        reply.target.label: frozenset(reply.result.loaded) for reply in answered
    }


async def sync_sts(
    broker: Broker,
    stamp: EnvStamp,
    *,
    allow_disruptive: bool = False,
) -> StsFanoutResult:
    """Make every STS overlay match ``stamp``, then reload its registry."""
    packages = _stamp_pins(stamp)
    replies = await fanout(
        broker,
        make_envelope=lambda: StsEnvSyncRequestEnvelope.wrap(
            StsEnvSyncRequest(
                generation=stamp.generation,
                packages=packages,
                allow_disruptive=allow_disruptive,
            ),
            type=STS_ENV_SYNC,
            source="api",
        ),
        result_type=StsEnvSyncResult,
        timeout=SYNC_TIMEOUT_S,
    )
    error = format_errors(replies)
    if error is not None:
        return StsFanoutResult(loaded=frozenset(), in_sync=False, error=error)
    if not _census_ok(replies):
        return StsFanoutResult(
            loaded=frozenset(), in_sync=False, error=CENSUS_ERROR
        )
    in_sync = all(
        extras_match(
            reply.result.generation,
            reply.result.packages,
            stamp,
            overlay_live=reply.result.overlay_live,
        )
        for reply in replies
        if reply.result is not None
    )
    return StsFanoutResult(
        loaded=_intersect_loaded(replies),
        in_sync=in_sync,
        error=None,
    )


async def sts_extras_in_sync(
    broker: Broker, stamp: EnvStamp
) -> tuple[bool, str | None]:
    """Read-only: did every STS adopt this stamp? Error if anyone is silent."""
    replies = await fanout(
        broker,
        make_envelope=lambda: StsRegistryGenerationRequestEnvelope.wrap(
            StsRegistryGenerationRequest(),
            type=STS_REGISTRY_GENERATION,
            source="api",
        ),
        result_type=StsRegistryGenerationResult,
    )
    error = format_errors(replies)
    if error is not None:
        return False, error
    if not _census_ok(replies):
        return False, CENSUS_ERROR
    return (
        all(
            extras_match(
                reply.result.generation,
                reply.result.packages,
                stamp,
                overlay_live=reply.result.overlay_live,
            )
            for reply in replies
            if reply.result is not None
        ),
        None,
    )


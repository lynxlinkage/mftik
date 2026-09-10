"""Ask every declared STS, fail closed.

Env apply and registry mutation used to send one RPC on the anycast
subject ``sts``. With two instances only one process answers; the other
keeps its in-memory stamp until restart. MD attach and the event-log
listing already walk the ``instances`` table. This is that walk for the
control plane that must reach *every* interpreter.

One enabled STS (or none readable) stays on ``Topics.STS``. Two or more
go to ``sts.{name}`` concurrently. A timeout or RPC error fails the
whole fan-out: a write has already committed the stamp, and the caller
reports ``restart_required`` rather than pretending every process saw it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from mftik.broker import Broker
from mftik.envapply import APPLY_TIMEOUT_S
from mftik.environment import EnvStamp
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_RELOAD,
    StsEnvPackagePin,
    StsEnvSyncRequest,
    StsEnvSyncRequestEnvelope,
    StsEnvSyncResult,
    StsRegistryGenerationRequest,
    StsRegistryGenerationRequestEnvelope,
    StsRegistryGenerationResult,
    StsRegistryReloadRequest,
    StsRegistryReloadRequestEnvelope,
    StsRegistryReloadResult,
    Topics,
)
from mftik_db.models.session import SessionDomain
from mftik_db.repositories import InstanceRepository
from mftik_db.session import session_scope
from pydantic import BaseModel

from mftik_api.broker_rpc import DomainRpcError, request_domain

logger = logging.getLogger(__name__)

#: Installer cap plus a little for the reload that follows it. Instances
#: run in parallel, so two hosts cost one timeout, not two.
SYNC_TIMEOUT_S = APPLY_TIMEOUT_S + 30.0

@dataclass(frozen=True, slots=True)
class StsTarget:
    """One address. ``name`` is None when we fell back to the shared subject."""

    name: str | None
    subject: str

    @property
    def label(self) -> str:
        return self.name or "sts"


@dataclass(frozen=True, slots=True)
class FanoutReply[T]:
    target: StsTarget
    result: T | None
    error: str | None


@dataclass(frozen=True, slots=True)
class StsFanoutResult:
    """Intersection of loaded keys, whether every process matches the stamp."""

    loaded: frozenset[str]
    in_sync: bool
    error: str | None


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
) -> bool:
    """Whether this process's in-memory extras are the stamp.

    Package equality is the real check (unshared volumes do not share
    generation counters). An empty ``packages`` falls back to generation
    so a pre-upgrade STS that reached the number still reads as in sync.
    """
    want = pins_of_stamp(stamp)
    have = pins_of_wire(packages)
    if have == want:
        return True
    if not packages:
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
    """Enabled STS subjects. Anycast when there is not more than one."""
    try:
        async with session_scope() as db:
            rows = await InstanceRepository(db).list_all(
                domain=SessionDomain.STS.value
            )
        enabled = [row for row in rows if row.enabled]
    except Exception:
        logger.warning(
            "sts fan-out: could not list instances — asking the shared subject",
            exc_info=True,
        )
        return [StsTarget(name=None, subject=Topics.STS)]
    if len(enabled) <= 1:
        name = enabled[0].name if enabled else None
        return [StsTarget(name=name, subject=Topics.STS)]
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
) -> list[FanoutReply[T]]:
    """Send one fresh envelope per target. Reusing an id would collide replies."""
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
    error = format_errors(replies)
    if error is not None:
        return StsFanoutResult(loaded=frozenset(), in_sync=False, error=error)
    return StsFanoutResult(
        loaded=_intersect_loaded(replies),
        in_sync=True,
        error=None,
    )


def _stamp_pins(stamp: EnvStamp) -> dict[str, StsEnvPackagePin]:
    return {
        name: StsEnvPackagePin(
            version=rec.version, dist=rec.dist, source=rec.source
        )
        for name, rec in stamp.packages.items()
    }


async def sync_sts(
    broker: Broker,
    stamp: EnvStamp,
    *,
    allow_disruptive: bool = True,
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
    in_sync = all(
        extras_match(reply.result.generation, reply.result.packages, stamp)
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
    return (
        all(
            extras_match(reply.result.generation, reply.result.packages, stamp)
            for reply in replies
            if reply.result is not None
        ),
        None,
    )


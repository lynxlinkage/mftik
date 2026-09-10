"""Make this STS overlay match the node's extras, then reload the registry.

The API applies first and owns the stamp. A second STS host may not mount
that volume, so this process installs the same pins locally when they are
not already on disk. Shared volume is a no-op install: the stamp matches,
``refresh`` adopts it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.envapply import (
    ApplyFailed,
    ApplyInProgress,
    ApplySpec,
    EnvironmentDisruptive,
    EnvironmentInvalid,
    Installer,
    run_uv_installer,
)
from mftik.environment import EnvironmentLocked, EnvStamp, NodeEnv
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_ERROR,
    RpcError,
    RpcErrorEnvelope,
    StsEnvPackagePin,
    StsEnvSyncRequest,
    StsEnvSyncResult,
    StsEnvSyncResultEnvelope,
)

from mftik_sts.runtime_env import current_stamp, refresh

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)

#: Tests replace this so sync never talks to an index.
installer_for_sync: Installer | None = None


def _installer() -> Installer:
    return installer_for_sync or run_uv_installer


def _pins(stamp: EnvStamp) -> dict[str, tuple[str, str]]:
    return {name: (rec.version, rec.dist) for name, rec in stamp.packages.items()}


def _requested_pins(
    packages: dict[str, StsEnvPackagePin],
) -> dict[str, tuple[str, str]]:
    return {name: (pin.version, pin.dist) for name, pin in packages.items()}


def _specs(packages: dict[str, StsEnvPackagePin]) -> dict[str, ApplySpec]:
    return {
        name: ApplySpec(version=pin.version, dist=pin.dist, source=pin.source)
        for name, pin in packages.items()
    }


def local_matches(env: NodeEnv, packages: dict[str, StsEnvPackagePin]) -> bool:
    """True when this volume already has these pins, importably."""
    stamp = env.read_stamp()
    if _pins(stamp) != _requested_pins(packages):
        return False
    if stamp.generation > 0:
        if not stamp.matches_runtime():
            return False
        if env.overlay_for(stamp) is None:
            return False
    return True


def apply_requested(request: StsEnvSyncRequest) -> None:
    """Install ``request.packages`` when this volume does not already have them."""
    env = NodeEnv.from_env()
    if local_matches(env, request.packages):
        return
    with ApplyInProgress(
        env,
        _specs(request.packages),
        allow_disruptive=request.allow_disruptive,
        installer=_installer(),
        generation=request.generation,
    ) as pending:
        pending.commit()


def _pins_out(stamp: EnvStamp) -> dict[str, StsEnvPackagePin]:
    return {
        name: StsEnvPackagePin(
            version=rec.version, dist=rec.dist, source=rec.source
        )
        for name, rec in stamp.packages.items()
    }


async def handle_env_sync(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    request = StsEnvSyncRequest.model_validate(req.envelope.payload)
    try:
        # ``uv`` is a blocking subprocess. The scan that follows mutates
        # ``sys.modules`` and stays on the event loop, same as reload.
        await asyncio.to_thread(apply_requested, request)
        loaded, stamp = refresh()
    except (
        ApplyFailed,
        EnvironmentDisruptive,
        EnvironmentInvalid,
        EnvironmentLocked,
    ) as exc:
        logger.warning("env sync refused: %s", exc)
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(code="env_sync_failed", message=str(exc)),
                type=STS_ERROR,
                source="sts",
                session_id=req.envelope.session_id,
            )
        )
        return
    except Exception as exc:
        logger.exception("env sync failed")
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(code="env_sync_failed", message=str(exc)),
                type=STS_ERROR,
                source="sts",
                session_id=req.envelope.session_id,
            )
        )
        return

    logger.info(
        "env synced generation=%d packages=%s",
        stamp.generation,
        ",".join(sorted(stamp.packages)) or "(none)",
    )
    await req.reply(
        StsEnvSyncResultEnvelope.wrap(
            StsEnvSyncResult(
                loaded=loaded,
                generation=stamp.generation,
                packages=_pins_out(stamp),
            ),
            type=STS_ENV_SYNC,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )


def current_packages() -> dict[str, StsEnvPackagePin]:
    """In-memory stamp as wire pins — what generation RPC reports."""
    return _pins_out(current_stamp())

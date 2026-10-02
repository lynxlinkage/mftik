"""What B5-10 has to make true of the registry and env handlers.

Every test here is ``xfail(strict=True)``. The signatures raise
``NotImplementedError("IF-16")``, so the tests fail, and ``strict`` means
the day the handlers answer the suite goes red until the marker is removed.

The behaviours IF-16 already implements — the replica, the pin, GC,
deployable, the rehang's digest, and the probe — are not in this file.
They pass in ``test_hostdisk.py`` and ``test_hostdisk_probe.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.procman import Supervisor
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_RELOAD,
    STS_REGISTRY_SYNC,
    Envelope,
    StsEnvSyncRequest,
    StsEnvSyncResult,
    StsRegistryReloadRequest,
    StsRegistryReloadResult,
    StsRegistrySyncRequest,
    StsRegistrySyncResult,
)
from mftik_sts.controller import (
    StsOrchestrator,
    env_sync_handler,
    registry_reload_handler,
    registry_sync_handler,
)

_B5 = "B5-10: controller serves registry and env on the digest-addressed replica"


def _orch(tmp_path: Path) -> StsOrchestrator:
    return StsOrchestrator(Supervisor(tmp_path, plane="sts", instance="sts"))


def _message(payload: object, type_: str) -> Envelope[dict]:
    body = payload.model_dump() if hasattr(payload, "model_dump") else payload
    return Envelope[dict].wrap(body, type=type_, source="api")  # type: ignore[arg-type]


def _model(reply_payload: object, model: type):
    if hasattr(reply_payload, "model_dump"):
        reply_payload = reply_payload.model_dump()
    return model.model_validate(reply_payload)


@pytest.mark.xfail(strict=True, reason=_B5)
async def test_registry_sync_replies_with_the_sync_result(tmp_path: Path) -> None:
    """The reply is the sync result. The handler writes the replica and
    does not import the tree; the probe is a subprocess."""
    request = StsRegistrySyncRequest(trees=[], reload=False)
    reply = await registry_sync_handler(_orch(tmp_path))(
        _message(request, STS_REGISTRY_SYNC)
    )
    assert reply is not None
    _model(reply.payload, StsRegistrySyncResult)


@pytest.mark.xfail(strict=True, reason=_B5)
async def test_registry_reload_replies_without_importing(tmp_path: Path) -> None:
    """Reload rescans the index. It does not import a strategy tree."""
    request = StsRegistryReloadRequest()
    reply = await registry_reload_handler(_orch(tmp_path))(
        _message(request, STS_REGISTRY_RELOAD)
    )
    assert reply is not None
    _model(reply.payload, StsRegistryReloadResult)


@pytest.mark.xfail(strict=True, reason=_B5)
async def test_env_sync_replies_with_the_env_result(tmp_path: Path) -> None:
    request = StsEnvSyncRequest()
    reply = await env_sync_handler(_orch(tmp_path))(
        _message(request, STS_ENV_SYNC)
    )
    assert reply is not None
    _model(reply.payload, StsEnvSyncResult)

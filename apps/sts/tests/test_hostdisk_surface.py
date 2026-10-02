"""IF-16's shape: pins, stubs, and the rule that the parent does not import.

The disk behaviour is in ``test_hostdisk.py``. The probe's subprocess is
in ``test_hostdisk_probe.py``. Handlers that B5-10 has to answer are
xfail in ``test_hostdisk_contract.py``.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest
from mftik.procman import Supervisor
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_RELOAD,
    STS_REGISTRY_SYNC,
    Envelope,
)
from mftik_sts.controller import (
    LABEL_ENV_GENERATION,
    LABEL_STRATEGY_DIGEST,
    StsOrchestrator,
    catch_up_registry,
    env_sync_handler,
    registry_reload_handler,
    registry_sync_handler,
)
from mftik_sts.hostdisk import TICKET, release_accepts
from mftik_sts.rpc.env import handle_env_sync
from mftik_sts.rpc.registry import handle_registry_reload, handle_registry_sync
from mftik_sts.rpc.router import _HANDLERS

_HOSTDISK = Path(__file__).resolve().parents[1] / "src" / "mftik_sts" / "hostdisk"
_CONTROLLER = Path(__file__).resolve().parents[1] / "src" / "mftik_sts" / "controller"
_FORBIDDEN = {
    "load_class",
    "handshake_info",
    "mftik.registry.load",
    "mftik.registry.protocol",
    "mftik_sts.impl",
}


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                found.add(node.module)
            found.update(alias.name for alias in node.names)
    return found


def test_worker_env_names_match_the_hostdisk_constants() -> None:
    from mftik_sts.hostdisk.identity import (
        ENV_GENERATION_ENV,
        STRATEGY_DIGEST_ENV,
    )
    from mftik_sts.pinned_strategy import (
        ENV_GENERATION_ENV as pinned_generation,
    )
    from mftik_sts.pinned_strategy import (
        STRATEGY_DIGEST_ENV as pinned_digest,
    )
    from mftik_sts.session_worker.process import (
        ENV_GENERATION_ENV as worker_generation,
    )
    from mftik_sts.session_worker.process import (
        STRATEGY_DIGEST_ENV as worker_digest,
    )

    assert worker_digest == pinned_digest == STRATEGY_DIGEST_ENV
    assert worker_digest == "MFTIK_STRATEGY_DIGEST"
    assert worker_generation == pinned_generation == ENV_GENERATION_ENV
    assert worker_generation == "MFTIK_ENV_GENERATION"


def test_ticket_id_is_if_16() -> None:
    assert TICKET == "IF-16"
    assert LABEL_STRATEGY_DIGEST == "strategy_digest"
    assert LABEL_ENV_GENERATION == "env_generation"


def test_release_accepts_a_numeric_minimum() -> None:
    """A tree that asks for 0.1.0 still runs on a later release. A tree
    that asks for something this release is behind does not. A final
    release accepts a pre-release of the same numbers. A declaration
    that is not a version fails closed. A source-tree ``0.0.0`` accepts
    nothing unless the dev flag is on, and then it accepts any
    requirement."""
    assert release_accepts("0.1.0", "0.1.0")
    assert release_accepts("0.1.0", "0.2.0")
    assert release_accepts("0.2", "0.2.0")
    assert release_accepts("0.1.0", "v0.2.0")
    assert release_accepts("0.2.0rc1", "0.2.0")
    assert release_accepts("0.2.0a1", "0.2.0b1")
    assert not release_accepts("0.2.0b1", "0.2.0a1")
    assert not release_accepts("0.2.0", "0.2.0rc1")
    assert not release_accepts("99.0.0", "0.1.0")
    assert not release_accepts("0.1.0", "latest")
    assert not release_accepts("0.1.0", "1!0.2.0")
    assert not release_accepts("0.0.0", "0.0.0", allow_dev=False)
    assert not release_accepts("0.0.0", "v0.0.0", allow_dev=False)
    assert not release_accepts("0.0.0", "0", allow_dev=False)
    assert release_accepts("99.0.0", "0.0.0", allow_dev=True)
    assert release_accepts("not-a-version", "0.0.0", allow_dev=True)


def test_only_the_probe_child_imports_strategy_loading() -> None:
    """``load_class`` and ``handshake_info`` are the child's. The
    controller and the rest of hostdisk do not name them as imports."""
    child = _imports(_HOSTDISK / "probe_child.py")
    assert "load_class" in child
    assert "handshake_info" in child
    assert "mftik.registry.load" in child
    assert "mftik.registry.protocol" in child
    parents = [
        *_HOSTDISK.glob("*.py"),
        *_CONTROLLER.glob("*.py"),
    ]
    for path in parents:
        if path.name == "probe_child.py":
            continue
        overlap = _FORBIDDEN & _imports(path)
        assert not overlap, f"{path.name} imports {sorted(overlap)}"


def test_the_running_router_still_owns_registry_and_env() -> None:
    """``dispatch`` still calls the legacy importers. ``control_handler``
    answers the digest replica and does not fall through to them."""
    assert _HANDLERS[STS_REGISTRY_SYNC] is handle_registry_sync
    assert _HANDLERS[STS_REGISTRY_RELOAD] is handle_registry_reload
    assert _HANDLERS[STS_ENV_SYNC] is handle_env_sync


def test_handler_signatures_are_handlers(tmp_path: Path) -> None:
    """A plain async function is a handler (IF-02). ``Handler`` is a
    Protocol and is not checked with ``isinstance``."""
    orch = StsOrchestrator(Supervisor(tmp_path, plane="sts", instance="sts"))
    for handler in (
        registry_sync_handler(orch),
        registry_reload_handler(orch),
        env_sync_handler(orch),
    ):
        assert inspect.iscoroutinefunction(handler)


@pytest.mark.component
async def test_registry_and_env_handlers_reply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    orch = StsOrchestrator(Supervisor(tmp_path, plane="sts", instance="sts"))
    message = Envelope[dict].wrap({}, type=STS_REGISTRY_SYNC, source="api")
    for handler in (
        registry_sync_handler(orch),
        registry_reload_handler(orch),
        env_sync_handler(orch),
    ):
        reply = await handler(message)
        assert reply is not None


async def test_catch_up_is_a_client_call() -> None:
    """``api.registry.catchup`` is not a controller handler. The signature
    is the call the controller makes. It does not replace
    ``registry_catchup.catch_up_until_matched``."""

    class _Reply:
        payload = {"ok": True, "error": None}

    class _Broker:
        async def request(
            self, subject: str, envelope: object, *, timeout: float
        ) -> _Reply:
            assert subject == "api.registry.catchup"
            assert timeout == 60
            assert envelope is not None
            return _Reply()

    result = await catch_up_registry(_Broker(), "sts")  # type: ignore[arg-type]
    assert result.ok
    assert result.error is None

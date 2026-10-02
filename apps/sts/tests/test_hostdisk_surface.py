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


def test_ticket_id_is_if_16() -> None:
    assert TICKET == "IF-16"
    assert LABEL_STRATEGY_DIGEST == "strategy_digest"
    assert LABEL_ENV_GENERATION == "env_generation"


def test_release_accepts_a_numeric_minimum() -> None:
    """A tree that asks for 0.1.0 still runs on a later release. A tree
    that asks for something this release is behind does not. A declaration
    that is not dotted integers fails closed."""
    assert release_accepts("0.1.0", "0.1.0")
    assert release_accepts("0.1.0", "0.2.0")
    assert not release_accepts("99.0.0", "0.1.0")
    assert not release_accepts("0.2.0rc1", "0.2.0")
    assert not release_accepts("0.1.0", "latest")


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
    """IF-16 does not move the handlers that are already serving. B5-10 does."""
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


async def test_registry_and_env_handlers_raise_if_16(tmp_path: Path) -> None:
    orch = StsOrchestrator(Supervisor(tmp_path, plane="sts", instance="sts"))
    message = Envelope[dict].wrap({}, type=STS_REGISTRY_SYNC, source="api")
    for handler in (
        registry_sync_handler(orch),
        registry_reload_handler(orch),
        env_sync_handler(orch),
    ):
        with pytest.raises(NotImplementedError, match="^IF-16$"):
            await handler(message)


async def test_catch_up_is_a_client_call_and_raises_if_16() -> None:
    """``api.registry.catchup`` is not a controller handler. The signature
    is the call the controller will make. It does not replace
    ``registry_catchup.catch_up_until_matched``."""
    with pytest.raises(NotImplementedError, match="^IF-16$"):
        await catch_up_registry(object(), "sts")  # type: ignore[arg-type]

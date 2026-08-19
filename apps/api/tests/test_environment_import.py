"""Import a peer's extras — preview first, confirm is what writes the stamp."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest
from fastapi import HTTPException
from mftik.envapply import ApplyFailed, ApplySpec
from mftik.environment import NodeEnv
from mftik.registry import RegistryStore
from mftik.registry.errors import RegistryError
from mftik.registry.protocol import (
    PROTOCOL,
    PROTOCOL_MIN,
    PROTOCOL_VERSION,
)
from mftik.registry.sync import connect_remote
from mftik_api.auth.principal import Principal
from mftik_api.routes import environment as environment_routes
from mftik_api.routes.environment import import_environment
from mftik_api.routes.sts import list_strategy_types
from mftik_api.schemas import EnvironmentImportBody
from test_environment import EnvBroker, _write_pkg

_SKLEARN = """\
from mftik.strategy import Strategy

class UsesSklearn(Strategy):
    name = "uses_sklearn"
    requires = ("sklearn",)
"""


def _handshake(extras: object) -> dict[str, object]:
    return {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "protocol_min": PROTOCOL_MIN,
        "mftik_version": "0.0.1",
        "extras": extras,
        "env_generation": 1,
    }


def _peer(extras: object) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path.rstrip("/") or "/"
        if path == "/registry/v1/info":
            return httpx.Response(200, json=_handshake(extras))
        if path == "/registry/v1/strategies":
            return httpx.Response(200, json={"strategies": []})
        return httpx.Response(404)

    return httpx.MockTransport(handler)


@pytest.fixture
def env_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.setattr(environment_routes, "installer_for_apply", _write_pkg)

    async def _no_audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(environment_routes, "record_audit", _no_audit)
    yield tmp_path
    environment_routes.import_client = None


async def _call(
    extras: object,
    store: RegistryStore,
    broker: EnvBroker,
    *,
    confirm: bool = False,
    dist: dict[str, str] | None = None,
    url: str = "http://peer",
    name: str | None = None,
) -> object:
    async with httpx.AsyncClient(transport=_peer(extras)) as client:
        environment_routes.import_client = client
        return await import_environment(
            EnvironmentImportBody(
                url=url if name is None else None,
                name=name,
                confirm=confirm,
                dist=dist or {},
            ),
            broker=broker,
            store=store,
            owner=1,
            principal=Principal.owner(1, via="password"),
        )


async def test_preview_lists_numpy_and_does_not_apply(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    out = await _call(
        {"numpy": {"version": "2.2.1", "dist": "numpy"}},
        store,
        EnvBroker(),
    )
    assert [row.name for row in out.added] == ["numpy"]
    assert out.added[0].dist == "numpy"
    assert out.applied is False
    assert NodeEnv(env_dir).read_stamp().generation == 0
    assert calls == []
    assert store.list_remotes() == []

    with pytest.raises(RegistryError, match="numpy"):
        async with httpx.AsyncClient(
            transport=_peer({"numpy": {"version": "2.2.1", "dist": "numpy"}})
        ) as client:
            await connect_remote(
                store, name="peer", url="http://peer", client=client
            )
    assert store.list_remotes() == []


async def test_sklearn_object_confirm_installs_the_pypi_dist(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    seen: list[dict[str, ApplySpec]] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        seen.append(dict(packages))
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    extras = {"sklearn": {"version": "1.6.1", "dist": "scikit-learn"}}
    preview = await _call(extras, store, EnvBroker())
    assert preview.added[0].dist == "scikit-learn"
    assert preview.added[0].guessed is False

    out = await _call(extras, store, EnvBroker(), confirm=True)
    assert out.applied is True
    assert seen[0]["sklearn"].requirement() == "scikit-learn==1.6.1"
    assert seen[0]["sklearn"].source == "peer:http://peer"
    stamp = NodeEnv(env_dir).read_stamp()
    assert "sklearn" in stamp.packages
    assert stamp.packages["sklearn"].dist == "scikit-learn"

    store.add({"strategy.py": _SKLEARN}, applied_extras={"sklearn": "1.6.1"})
    listed = await list_strategy_types(store=store)
    row = next(t for t in listed.templates if t.type == "private::UsesSklearn")
    assert row.requires == ["sklearn"]
    assert row.env_ok is True
    assert store.list_remotes() == []


async def test_legacy_flat_sklearn_is_refused_at_confirm(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    extras = {"sklearn": "1.6.1"}
    preview = await _call(extras, store, EnvBroker())
    assert preview.added[0].guessed is True
    assert "sklearn" in preview.guessed

    with pytest.raises(HTTPException) as caught:
        await _call(extras, store, EnvBroker(), confirm=True)
    assert caught.value.status_code == 409
    assert "guessed" in str(caught.value.detail)
    assert calls == []
    assert NodeEnv(env_dir).read_stamp().generation == 0


async def test_confirm_then_connect_succeeds(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    extras = {"numpy": {"version": "2.2.1", "dist": "numpy"}}
    out = await _call(extras, store, EnvBroker(), confirm=True)
    assert out.environment is not None
    assert out.environment.generation == 1
    assert store.list_remotes() == []

    async with httpx.AsyncClient(transport=_peer(extras)) as client:
        result = await connect_remote(
            store, name="peer", url="http://peer", client=client
        )
    assert result.name == "peer"
    assert store.get_remote("peer") is not None


async def test_confirm_installer_failure_leaves_generation_zero(env_dir: Path) -> None:
    store = RegistryStore(env_dir)

    def boom(dest: Path, packages: dict[str, ApplySpec]) -> None:
        raise ApplyFailed("nope")

    environment_routes.installer_for_apply = boom
    extras = {"numpy": {"version": "1.0", "dist": "numpy"}}
    with pytest.raises(HTTPException) as caught:
        await _call(extras, store, EnvBroker(), confirm=True)
    assert caught.value.status_code == 502
    assert NodeEnv(env_dir).read_stamp().generation == 0

    with pytest.raises(RegistryError, match="numpy"):
        async with httpx.AsyncClient(transport=_peer(extras)) as client:
            await connect_remote(
                store, name="peer", url="http://peer", client=client
            )


async def test_pin_clash_is_409_on_confirm_not_preview(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    await _call(
        {"numpy": {"version": "2.2.1", "dist": "numpy"}},
        store,
        EnvBroker(),
        confirm=True,
    )
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    extras = {"numpy": {"version": "1.26.4", "dist": "numpy"}}
    preview = await _call(extras, store, EnvBroker())
    assert preview.conflicts[0].local_version == "2.2.1"
    assert preview.conflicts[0].version == "1.26.4"

    with pytest.raises(HTTPException) as caught:
        await _call(extras, store, EnvBroker(), confirm=True)
    assert caught.value.status_code == 409
    assert "2.2.1" in str(caught.value.detail)
    assert calls == []
    assert NodeEnv(env_dir).read_stamp().packages["numpy"].version == "2.2.1"


async def test_named_remote_does_not_need_the_url_retyped(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    store.put_remote("node1", "http://peer")
    extras = {"numpy": {"version": "1.0", "dist": "numpy"}}
    out = await _call(extras, store, EnvBroker(), name="node1")
    assert [row.name for row in out.added] == ["numpy"]
    confirmed = await _call(extras, store, EnvBroker(), name="node1", confirm=True)
    assert confirmed.applied is True
    assert (
        NodeEnv(env_dir).read_stamp().packages["numpy"].source == "peer:node1"
    )


async def test_unknown_named_remote_is_404(env_dir: Path) -> None:
    store = RegistryStore(env_dir)
    with pytest.raises(HTTPException) as caught:
        await import_environment(
            EnvironmentImportBody(name="missing"),
            broker=EnvBroker(),
            store=store,
            owner=1,
            principal=Principal.owner(1, via="password"),
        )
    assert caught.value.status_code == 404

"""ENV-11 sequences that cross API, store, handshake, connect, and deploy.

S9 (registry key cannot write extras) is ``test_auth_registry_keys``: GET
``/environment`` and POST ``/environment/import`` are 403 with that key, while
``/registry/v1/info`` and ``/registry/v1/strategies`` stay 200.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest
from fastapi import HTTPException
from mftik.envapply import ApplyFailed, ApplySpec
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import (
    STS_SESSION_CREATE,
    StsCreateSessionResult,
    StsCreateSessionResultEnvelope,
)
from mftik.registry import RegistryStore
from mftik.registry.errors import RegistryError
from mftik.registry.inspect import inspect_files
from mftik.registry.protocol import handshake_info
from mftik.registry.sync import connect_remote
from mftik_api.auth.principal import Principal
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.routes import environment as environment_routes
from mftik_api.routes.environment import (
    delete_package,
    get_environment,
    import_environment,
    put_environment,
)
from mftik_api.routes.registry import add_strategy, registry_info
from mftik_api.routes.sts import deploy, list_strategy_types
from mftik_api.schemas import (
    EnvironmentImportBody,
    EnvironmentPutBody,
    EnvPackageIn,
    RegistryAddBody,
    StrategyDeployBody,
)
from test_environment import EnvBroker, _write_pkg
from test_registry_add import ReloadingBroker

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_NUMPY = """\
from mftik.strategy import Strategy

class UsesNumpy(Strategy):
    name = "uses_numpy"
    requires = ("numpy",)
"""

_NUMPY_BARE = """\
import numpy
from mftik.strategy import Strategy

class UsesNumpy(Strategy):
    name = "uses_numpy"
"""

_SKLEARN = """\
from mftik.strategy import Strategy

class UsesSklearn(Strategy):
    name = "uses_sklearn"
    requires = ("sklearn",)
"""

_TORCH = """\
from mftik.strategy import Strategy

class UsesTorch(Strategy):
    name = "uses_torch"
    requires = ("torch",)
"""

_OWNER = Principal.owner(1, via="password")


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.setattr(environment_routes, "installer_for_apply", _write_pkg)

    async def _no_audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(environment_routes, "record_audit", _no_audit)
    yield tmp_path
    environment_routes.import_client = None


def _store(data_dir: Path) -> RegistryStore:
    return RegistryStore(data_dir)


def _reload(store: RegistryStore, loaded: list[str] | None = None) -> ReloadingBroker:
    return ReloadingBroker(loaded).with_store(store)


async def _put(
    packages: dict[str, tuple[str, str]],
    broker: EnvBroker,
    *,
    force: bool = False,
) -> object:
    body = EnvironmentPutBody(
        packages={
            name: EnvPackageIn(version=version, dist=dist)
            for name, (version, dist) in packages.items()
        },
        force=force,
    )
    return await put_environment(
        body, broker=broker, force=force, owner=1, principal=_OWNER
    )


def _peer_transport(
    peer: RegistryStore, extras: object | None = None
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path.rstrip("/") or "/"
        if path == "/registry/v1/info":
            info = handshake_info(data_dir=peer.data_dir)
            if extras is not None:
                info = {**info, "extras": extras}
            return httpx.Response(200, json=info)
        if path == "/registry/v1/strategies":
            return httpx.Response(
                200,
                json={
                    "strategies": [
                        {
                            "name": rec.name,
                            "type": rec.type,
                            "digest": rec.digest,
                            "requires_mftik": rec.requires_mftik,
                            "origin": rec.origin,
                            "files": list(rec.files),
                        }
                        for rec in peer.list_public()
                    ]
                },
            )
        prefix = "/registry/v1/strategies/"
        if path.startswith(prefix):
            rec = peer.get_public(path[len(prefix) :])
            if rec is None:
                return httpx.Response(404)
            return httpx.Response(
                200,
                json={
                    "name": rec.name,
                    "type": rec.type,
                    "digest": rec.digest,
                    "contents": peer.read_contents(rec),
                },
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


async def test_s1_bare_node_stdlib_tree(data_dir: Path) -> None:
    store = _store(data_dir)
    out = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}),
        store=store,
        broker=_reload(store),
    )
    assert out.loaded is True
    dest = data_dir / "registry" / "private" / "tiny" / "strategy.py"
    assert dest.is_file()
    info = await registry_info()
    assert info.extras == {}
    listed = await list_strategy_types(store=store)
    assert "private::Tiny" in listed.types
    assert "NoopStrategy" in listed.types
    await deploy(
        "private::Tiny",
        body=StrategyDeployBody(yaml="sts: {}\n"),
        broker=_live_deploy(store),
        store=store,
    )


class _LiveDeploy(ReloadingBroker):
    async def publish_log(self, *args: object, **kwargs: object) -> int:
        return 1

    async def publish(self, *args: object, **kwargs: object) -> int:
        return 1

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        if envelope.type == STS_SESSION_CREATE:
            return StsCreateSessionResultEnvelope.wrap(
                StsCreateSessionResult(
                    session_id=envelope.payload.session_id,
                    strategy=envelope.payload.strategy,
                    td=[],
                    status="live",
                ),
                type=STS_SESSION_CREATE,
                source="sts",
            )
        return await super().request(subject, envelope, timeout=timeout)


def _live_deploy(store: RegistryStore) -> _LiveDeploy:
    return _LiveDeploy().with_store(store)


async def test_s2_declare_then_apply_then_add(data_dir: Path) -> None:
    store = _store(data_dir)
    with pytest.raises(HTTPException) as undeclared:
        await add_strategy(
            RegistryAddBody(files={"strategy.py": _NUMPY_BARE}),
            store=store,
            broker=_reload(store),
        )
    assert undeclared.value.status_code == 400
    assert "requires" in str(undeclared.value.detail)
    assert not (data_dir / "registry" / "private" / "uses_numpy").exists()

    inspect_files({"strategy.py": _NUMPY})
    with pytest.raises(HTTPException) as bare:
        await add_strategy(
            RegistryAddBody(files={"strategy.py": _NUMPY}),
            store=store,
            broker=_reload(store),
        )
    assert bare.value.status_code == 400
    assert "numpy" in str(bare.value.detail)

    def boom(dest: Path, packages: dict[str, ApplySpec]) -> None:
        raise ApplyFailed("nope")

    environment_routes.installer_for_apply = boom
    with pytest.raises(HTTPException) as failed:
        await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    assert failed.value.status_code == 502
    assert handshake_info(data_dir=data_dir)["extras"] == {}
    assert NodeEnv(data_dir).read_stamp().generation == 0

    environment_routes.installer_for_apply = _write_pkg
    broker = EnvBroker()
    applied = await _put({"numpy": ("1.0", "numpy")}, broker)
    assert applied.generation == 1
    assert broker.reload_calls == 1
    info = await registry_info()
    assert info.extras["numpy"].version == "1.0"

    added = await add_strategy(
        RegistryAddBody(files={"strategy.py": _NUMPY}),
        store=store,
        broker=_reload(store),
    )
    assert added.loaded is True
    await deploy(
        "private::UsesNumpy",
        body=StrategyDeployBody(yaml="sts: {}\n"),
        broker=_live_deploy(store),
        store=store,
    )


async def test_s4_new_connect_blocked_on_names(data_dir: Path) -> None:
    peer = RegistryStore(data_dir / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    store = _store(data_dir)
    extras = {"numpy": {"version": "1.0", "dist": "numpy"}}
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        with pytest.raises(RegistryError, match="numpy"):
            await connect_remote(
                store, name="peer", url="http://peer", client=client
            )
    assert store.list_remotes() == []
    assert not (data_dir / "registry" / "pulled").exists()


async def test_s4b_new_connect_allows_pin_drift(data_dir: Path) -> None:
    await _put({"numpy": ("2.2.2", "numpy")}, EnvBroker())
    peer = RegistryStore(data_dir / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    store = _store(data_dir)
    extras = {"numpy": {"version": "2.2.1", "dist": "numpy"}}
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        result = await connect_remote(
            store, name="peer", url="http://peer", client=client
        )
    assert [rec.name for rec in result.pulled] == ["tiny"]


async def test_s5_import_then_connect(data_dir: Path) -> None:
    peer = RegistryStore(data_dir / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    store = _store(data_dir)
    extras = {"numpy": {"version": "2.2.1", "dist": "numpy"}}
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        environment_routes.import_client = client
        preview = await import_environment(
            EnvironmentImportBody(url="http://peer"),
            broker=EnvBroker(),
            store=store,
            owner=1,
            principal=_OWNER,
        )
        assert [row.name for row in preview.added] == ["numpy"]
        assert preview.applied is False
        assert NodeEnv(data_dir).read_stamp().generation == 0
        assert store.list_remotes() == []

        confirmed = await import_environment(
            EnvironmentImportBody(url="http://peer", confirm=True),
            broker=EnvBroker(),
            store=store,
            owner=1,
            principal=_OWNER,
        )
        assert confirmed.applied is True
        assert store.list_remotes() == []
        info = handshake_info(data_dir=data_dir)
        assert info["extras"]["numpy"]["version"] == "2.2.1"

        result = await connect_remote(
            store, name="peer", url="http://peer", client=client
        )
    assert [rec.name for rec in result.pulled] == ["tiny"]


async def test_s5_sklearn_dist_is_what_the_installer_sees(data_dir: Path) -> None:
    store = _store(data_dir)
    seen: list[dict[str, ApplySpec]] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        seen.append(dict(packages))
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    extras = {"sklearn": {"version": "1.6.1", "dist": "scikit-learn"}}
    peer = RegistryStore(data_dir / "peer")
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        environment_routes.import_client = client
        preview = await import_environment(
            EnvironmentImportBody(url="http://peer"),
            broker=EnvBroker(),
            store=store,
            owner=1,
            principal=_OWNER,
        )
        assert preview.added[0].dist == "scikit-learn"
        await import_environment(
            EnvironmentImportBody(url="http://peer", confirm=True),
            broker=EnvBroker(),
            store=store,
            owner=1,
            principal=_OWNER,
        )
    assert seen[0]["sklearn"].requirement() == "scikit-learn==1.6.1"
    store.add({"strategy.py": _SKLEARN}, applied_extras={"sklearn": "1.6.1"})
    listed = await list_strategy_types(store=store)
    row = next(t for t in listed.templates if t.type == "private::UsesSklearn")
    assert row.env_ok is True


async def test_s5_failed_confirm_leaves_connect_refused(data_dir: Path) -> None:
    store = _store(data_dir)

    def boom(dest: Path, packages: dict[str, ApplySpec]) -> None:
        raise ApplyFailed("nope")

    environment_routes.installer_for_apply = boom
    extras = {"numpy": {"version": "1.0", "dist": "numpy"}}
    peer = RegistryStore(data_dir / "peer")
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        environment_routes.import_client = client
        with pytest.raises(HTTPException) as caught:
            await import_environment(
                EnvironmentImportBody(url="http://peer", confirm=True),
                broker=EnvBroker(),
                store=store,
                owner=1,
                principal=_OWNER,
            )
        assert caught.value.status_code == 502
        with pytest.raises(RegistryError, match="numpy"):
            await connect_remote(
                store, name="peer", url="http://peer", client=client
            )
    assert NodeEnv(data_dir).read_stamp().generation == 0


async def test_s6_already_connected_can_pull_a_heavier_tree(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    peer = RegistryStore(data_dir / "peer")
    peer.add({"strategy.py": _NUMPY}, origin="public")
    store = _store(data_dir)
    extras = {"numpy": {"version": "1.0", "dist": "numpy"}}
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        await connect_remote(store, name="peer", url="http://peer", client=client)
    peer.add({"strategy.py": _TORCH}, origin="public")
    heavier = {
        "numpy": {"version": "1.0", "dist": "numpy"},
        "torch": {"version": "2.0", "dist": "torch"},
    }
    async with httpx.AsyncClient(transport=_peer_transport(peer, heavier)) as client:
        result = await connect_remote(
            store, name="peer", url="http://peer", client=client
        )
    names = {rec.name for rec in result.pulled}
    assert names == {"uses_numpy", "uses_torch"}
    listed = await list_strategy_types(store=store)
    types = {t.type for t in listed.templates}
    assert "peer::UsesNumpy" in types
    assert "peer::UsesTorch" in types
    torch = next(t for t in listed.templates if t.type == "peer::UsesTorch")
    assert torch.env_ok is False

    await deploy(
        "peer::UsesNumpy",
        body=StrategyDeployBody(yaml="sts: {}\n"),
        broker=_live_deploy(store),
        store=store,
    )
    with pytest.raises(HTTPException) as caught:
        await deploy(
            "peer::UsesTorch",
            body=StrategyDeployBody(yaml="sts: {}\n"),
            broker=_EnvDeploy(),
            store=store,
        )
    assert caught.value.status_code == 409
    assert "torch" in str(caught.value.detail)
    assert "unknown strategy" not in str(caught.value.detail).lower()


class _EnvDeploy:
    async def publish_log(self, *args: object, **kwargs: object) -> int:
        return 1

    async def publish(self, *args: object, **kwargs: object) -> int:
        return 1

    async def request(self, *args: object, **kwargs: object) -> None:
        raise DomainRpcError(
            "incompatible_environment",
            "peer::UsesTorch requires torch which this node does not have",
        )


async def test_s7_delete_extra_breaks_deploy(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    store = _store(data_dir)
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _NUMPY}),
        store=store,
        broker=_reload(store),
    )
    out = await delete_package(
        "numpy",
        broker=EnvBroker(),
        store=store,
        owner=1,
        principal=_OWNER,
    )
    assert "numpy" not in NodeEnv(data_dir).read_stamp().packages
    assert [row.name for row in out.broken] == ["uses_numpy"]
    listed = await list_strategy_types(store=store)
    row = next(t for t in listed.templates if t.type == "private::UsesNumpy")
    assert row.env_ok is False

    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    environment_routes.installer_for_apply = record
    with pytest.raises(HTTPException) as live:
        await delete_package(
            "numpy",
            broker=EnvBroker(live=["sess-1"]),
            store=store,
            force=False,
            owner=1,
            principal=_OWNER,
        )
    assert live.value.status_code == 409
    assert calls == []
    forced = await delete_package(
        "numpy",
        broker=EnvBroker(live=["sess-1"]),
        store=store,
        force=True,
        owner=1,
        principal=_OWNER,
    )
    assert forced.restart_required is True
    assert forced.broken[0].name == "uses_numpy"


async def test_s8_pin_clash_on_confirm(data_dir: Path) -> None:
    await _put({"numpy": ("2.2.1", "numpy")}, EnvBroker())
    store = _store(data_dir)
    extras = {"numpy": {"version": "1.26.4", "dist": "numpy"}}
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    peer = RegistryStore(data_dir / "peer")
    async with httpx.AsyncClient(transport=_peer_transport(peer, extras)) as client:
        environment_routes.import_client = client
        preview = await import_environment(
            EnvironmentImportBody(url="http://peer"),
            broker=EnvBroker(),
            store=store,
            owner=1,
            principal=_OWNER,
        )
        assert preview.conflicts[0].local_version == "2.2.1"
        with pytest.raises(HTTPException) as caught:
            await import_environment(
                EnvironmentImportBody(url="http://peer", confirm=True),
                broker=EnvBroker(),
                store=store,
                owner=1,
                principal=_OWNER,
            )
    assert caught.value.status_code == 409
    assert calls == []
    assert NodeEnv(data_dir).read_stamp().packages["numpy"].version == "2.2.1"


async def test_s10_undeclared_import_is_refused_even_when_applied(
    data_dir: Path,
) -> None:
    await _put({"sklearn": ("1.6.1", "scikit-learn")}, EnvBroker())
    store = _store(data_dir)
    with pytest.raises(HTTPException) as caught:
        await add_strategy(
            RegistryAddBody(
                files={
                    "strategy.py": (
                        "import sklearn\n"
                        "from mftik.strategy import Strategy\n\n"
                        "class UsesSklearn(Strategy):\n"
                        '    name = "uses_sklearn"\n'
                    )
                }
            ),
            store=store,
            broker=_reload(store),
        )
    assert caught.value.status_code == 400
    assert "requires" in str(caught.value.detail)
    assert "does not have" not in str(caught.value.detail)


async def test_s11_loaded_still_means_sts_imported_it(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    store = _store(data_dir)
    ok = await add_strategy(
        RegistryAddBody(files={"strategy.py": _NUMPY}),
        store=store,
        broker=_reload(store),
    )
    assert ok.loaded is True
    skipped = await add_strategy(
        RegistryAddBody(files={"strategy.py": _NUMPY}, replace=True),
        store=store,
        broker=_reload(store, loaded=[]),
    )
    assert skipped.loaded is False
    assert skipped.load_error is not None
    assert "did not load" in skipped.load_error or "STS" in skipped.load_error


async def test_s12_crash_mid_apply_leaves_previous_generation(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    env = NodeEnv(data_dir)
    with env.lock():
        dest = env.begin()
        (dest / "half").write_text("incomplete\n")
    stamp = env.read_stamp()
    assert stamp.generation == 1
    assert stamp.packages["numpy"].version == "1.0"
    current = env.current_path.resolve()
    assert "gen-1" in str(current)
    assert (data_dir / "env" / "gen-2").is_dir()
    info = handshake_info(data_dir=data_dir)
    assert "numpy" in info["extras"]
    assert "half" not in info["extras"]


async def test_s13_lock_holder_makes_the_other_put_409(data_dir: Path) -> None:
    env = NodeEnv(data_dir)
    with env.lock():
        with pytest.raises(HTTPException) as caught:
            await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
        assert caught.value.status_code == 409
    assert env.read_stamp().generation == 0
    out = await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    assert out.generation == 1
    assert (data_dir / "env" / "gen-1").is_dir()
    assert not (data_dir / "env" / "gen-2").exists()


async def test_s14_abi_mismatch_is_visible_then_healed(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    env = NodeEnv(data_dir)
    matching = env.read_stamp()
    env._write_stamp(
        EnvStamp(
            generation=matching.generation,
            python=(3, 11),
            platform=matching.platform,
            nbytes=matching.nbytes,
            packages=matching.packages,
        )
    )
    got = await get_environment(broker=EnvBroker(generation=1))
    assert got.abi_ok is False
    assert got.python == [3, 11]
    assert handshake_info(data_dir=data_dir)["extras"] == {}
    healed = await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    assert healed.abi_ok is True
    assert healed.generation == 2


async def test_s15_helpers_declare_via_a_later_class_file(data_dir: Path) -> None:
    files = {
        "helpers.py": "import numpy\n",
        "strategy.py": _NUMPY,
    }
    inspected = inspect_files(files)
    assert "numpy" in inspected.cls.requires
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    store = _store(data_dir)
    out = await add_strategy(
        RegistryAddBody(files=files),
        store=store,
        broker=_reload(store),
    )
    assert out.loaded is True


async def test_s16_silent_sts_is_not_idle(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    store = _store(data_dir)
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    with pytest.raises(HTTPException) as caught:
        await delete_package(
            "numpy",
            broker=EnvBroker(list_silent=True),
            store=store,
            force=False,
            owner=1,
            principal=_OWNER,
        )
    assert caught.value.status_code == 409
    assert calls == []
    forced = await delete_package(
        "numpy",
        broker=EnvBroker(list_silent=True),
        store=store,
        force=True,
        owner=1,
        principal=_OWNER,
    )
    assert forced.restart_required is True


async def test_s17_session_arriving_mid_install_aborts(data_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    broker = EnvBroker(live=[], live_after=["sess-late"])
    with pytest.raises(HTTPException) as caught:
        await _put({"numpy": ("2.0", "numpy")}, broker)
    assert caught.value.status_code == 409
    stamp = NodeEnv(data_dir).read_stamp()
    assert stamp.generation == 1
    assert stamp.packages["numpy"].version == "1.0"
    assert not (data_dir / "env" / "gen-2").exists()

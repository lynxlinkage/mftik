"""Owner extras API — apply, then tell STS, never assume a silent broker is idle."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException
from mftik.envapply import ApplyFailed, ApplySpec
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import (
    STS_REGISTRY_RELOAD,
    STS_SESSION_LIST,
    ListSessionsResult,
    ListSessionsResultEnvelope,
    SessionInfo,
    StsRegistryReloadResult,
    StsRegistryReloadResultEnvelope,
)
from mftik.registry import RegistryStore
from mftik.registry.protocol import handshake_info
from mftik_api.auth.principal import Principal
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.routes import environment as environment_routes
from mftik_api.routes.environment import (
    delete_package,
    get_environment,
    put_environment,
    upsert_package,
)
from mftik_api.routes.registry import registry_info
from mftik_api.schemas import (
    EnvironmentPackageBody,
    EnvironmentPutBody,
    EnvPackageIn,
    RegistryInfoOut,
)

_NUMPY_STRAT = """\
from mftik.strategy import Strategy

class UsesNumpy(Strategy):
    name = "uses_numpy"
    requires = ("numpy",)
"""


def _write_pkg(dest: Path, packages: dict[str, ApplySpec]) -> None:
    for name in packages:
        pkg = dest / name
        pkg.mkdir()
        (pkg / "__init__.py").write_text("ok\n")


def _session(session_id: str) -> SessionInfo:
    return SessionInfo(
        session_id=session_id,
        domain="sts",
        created_by=1,
        created_at=0.0,
        status="live",
    )


class EnvBroker:
    """Answers session list and registry reload."""

    def __init__(
        self,
        *,
        live: list[str] | None = None,
        live_after: list[str] | None = None,
        list_silent: bool = False,
        reload_silent: bool = False,
        generation: int | None = None,
    ) -> None:
        self.live = list(live or [])
        self.live_after = live_after
        self.list_silent = list_silent
        self.reload_silent = reload_silent
        self.generation = generation
        self.list_calls = 0
        self.reload_calls = 0

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        if envelope.type == STS_SESSION_LIST:
            self.list_calls += 1
            if self.list_silent:
                raise DomainRpcError("timeout", "no reply from sts")
            ids = self.live
            if self.live_after is not None and self.list_calls > 1:
                ids = self.live_after
            return ListSessionsResultEnvelope.wrap(
                ListSessionsResult(sessions=[_session(i) for i in ids]),
                type=STS_SESSION_LIST,
                source="sts",
            )
        if envelope.type == STS_REGISTRY_RELOAD:
            self.reload_calls += 1
            if self.reload_silent:
                raise DomainRpcError("timeout", "no reply from sts")
            stamp = NodeEnv.from_env().read_stamp()
            gen = self.generation if self.generation is not None else stamp.generation
            return StsRegistryReloadResultEnvelope.wrap(
                StsRegistryReloadResult(loaded=[], generation=gen),
                type=STS_REGISTRY_RELOAD,
                source="sts",
            )
        raise AssertionError(f"unexpected type {envelope.type}")


@pytest.fixture
def env_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.setattr(environment_routes, "installer_for_apply", _write_pkg)

    async def _no_audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(environment_routes, "record_audit", _no_audit)
    return tmp_path


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
        body,
        broker=broker,
        force=force,
        owner=1,
        principal=Principal.owner(1, via="password"),
    )


async def test_get_on_a_fresh_node_is_empty(env_dir: Path) -> None:
    out = await get_environment(broker=EnvBroker())
    assert out.packages == {}
    assert out.generation == 0
    assert out.bytes == 0
    assert out.abi_ok is True


async def test_put_updates_get_and_info(env_dir: Path) -> None:
    broker = EnvBroker()
    out = await _put({"numpy": ("1.26.4", "numpy")}, broker)
    assert out.generation == 1
    assert out.loaded is True
    assert out.packages["numpy"].version == "1.26.4"
    assert broker.reload_calls == 1

    got = await get_environment(broker=EnvBroker())
    assert got.generation == 1
    assert got.packages["numpy"].dist == "numpy"

    info = await registry_info()
    assert info.env_generation == 1
    assert info.extras["numpy"].version == "1.26.4"
    assert info.extras["numpy"].dist == "numpy"
    advertised = handshake_info(data_dir=env_dir)
    assert advertised["extras"]["numpy"]["dist"] == "numpy"


async def test_put_installer_failure_does_not_reload(env_dir: Path) -> None:
    def boom(dest: Path, packages: dict[str, ApplySpec]) -> None:
        raise ApplyFailed("nope")

    environment_routes.installer_for_apply = boom
    broker = EnvBroker()
    with pytest.raises(HTTPException) as exc:
        await _put({"numpy": ("1.0", "numpy")}, broker)
    assert exc.value.status_code == 502
    assert "nope" in str(exc.value.detail)
    assert broker.reload_calls == 0
    assert NodeEnv(env_dir).read_stamp().generation == 0


async def test_put_commit_ok_sts_silent(env_dir: Path) -> None:
    broker = EnvBroker(reload_silent=True)
    out = await _put({"numpy": ("1.0", "numpy")}, broker)
    assert out.generation == 1
    assert out.loaded is False
    assert out.load_error is not None
    assert "no reply from sts" in out.load_error
    assert "restarts" in out.load_error
    assert NodeEnv(env_dir).read_stamp().generation == 1


async def test_change_with_live_session_does_not_install(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    broker = EnvBroker(live=["sess-1"])
    with pytest.raises(HTTPException) as exc:
        await _put({"numpy": ("2.0", "numpy")}, broker)
    assert exc.value.status_code == 409
    assert "sess-1" in str(exc.value.detail)
    assert calls == []
    assert NodeEnv(env_dir).read_stamp().packages["numpy"].version == "1.0"


async def test_change_when_sts_list_is_silent_is_409(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    broker = EnvBroker(list_silent=True)
    with pytest.raises(HTTPException) as exc:
        await _put({"numpy": ("2.0", "numpy")}, broker)
    assert exc.value.status_code == 409
    assert "did not answer" in str(exc.value.detail)
    assert calls == []
    assert NodeEnv(env_dir).read_stamp().generation == 1


async def test_session_appearing_during_install_aborts(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    broker = EnvBroker(live=[], live_after=["sess-late"])
    with pytest.raises(HTTPException) as exc:
        await _put({"numpy": ("2.0", "numpy")}, broker)
    assert exc.value.status_code == 409
    assert "sess-late" in str(exc.value.detail)
    stamp = NodeEnv(env_dir).read_stamp()
    assert stamp.generation == 1
    assert stamp.packages["numpy"].version == "1.0"
    assert not (env_dir / "env" / "gen-2").exists()


async def test_change_without_sessions_applies(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    out = await _put({"numpy": ("2.0", "numpy")}, EnvBroker())
    assert out.generation == 2
    assert out.packages["numpy"].version == "2.0"
    assert out.restart_required is True


async def test_force_change_sets_restart_required(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    broker = EnvBroker(live=["sess-1"])
    out = await _put({"numpy": ("2.0", "numpy")}, broker, force=True)
    assert out.generation == 2
    assert out.restart_required is True
    assert out.packages["numpy"].version == "2.0"
    assert broker.list_calls == 0


async def test_mismatched_abi_is_reported_then_healed(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    env = NodeEnv(env_dir)
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
    assert got.runtime_python != [3, 11]

    out = await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    assert out.abi_ok is True
    assert out.generation == 2


async def test_delete_names_trees_that_required_the_extra(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    store = RegistryStore(env_dir)
    store.add({"strategy.py": _NUMPY_STRAT})
    out = await delete_package(
        "numpy",
        broker=EnvBroker(),
        store=store,
        force=True,
        owner=1,
        principal=Principal.owner(1, via="password"),
    )
    assert out.generation == 2
    assert out.packages == {}
    assert [row.name for row in out.broken] == ["uses_numpy"]
    assert out.broken[0].requires == ["numpy"]


async def test_upsert_adds_without_checking_sessions(env_dir: Path) -> None:
    broker = EnvBroker(live=["sess-1"])
    out = await upsert_package(
        EnvironmentPackageBody(name="numpy", version="1.0", dist="numpy"),
        broker=broker,
        owner=1,
        principal=Principal.owner(1, via="password"),
    )
    assert out.generation == 1
    assert broker.list_calls == 0
    assert out.restart_required is False


async def test_mutation_writes_audit_with_via(
    env_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[dict[str, object]] = []

    async def capture(**kwargs: object) -> None:
        seen.append(kwargs)

    monkeypatch.setattr(environment_routes, "record_audit", capture)
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    assert seen[0]["operation"] == "environment.put"
    principal = seen[0]["principal"]
    assert getattr(principal, "via") == "password"


def test_info_accepts_legacy_flat_extras() -> None:
    info = RegistryInfoOut.model_validate(
        {
            "protocol": "mftik.registry",
            "protocol_version": 1,
            "protocol_min": 1,
            "mftik_version": "0.0.1",
            "extras": {"numpy": "2.2.1"},
        }
    )
    assert info.extras["numpy"].version == "2.2.1"
    assert info.extras["numpy"].dist == "numpy"
    assert info.env_generation == 0


@pytest.mark.parametrize("name", ["json", "logging", "mftik", "mftik_sts"])
async def test_a_name_the_node_provides_is_refused(
    env_dir: Path, name: str
) -> None:
    """The overlay is ahead of the stdlib and the SDK on ``sys.path``.

    ``gate.py`` already refuses a strategy that declares ``requires =
    ("json",)``. This is the layer that matters more: here the Owner types
    the name, and an extra installed under it would shadow the real module
    for every session in the STS process, including the running ones.
    """
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    environment_routes.installer_for_apply = record
    with pytest.raises(HTTPException) as exc:
        await _put({name: ("1.0", name)}, EnvBroker())
    assert exc.value.status_code == 400
    assert name in str(exc.value.detail)
    assert calls == [], "refused before the installer, not after"
    assert NodeEnv(env_dir).read_stamp().generation == 0


async def test_upsert_of_a_provided_name_is_refused(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    body = EnvironmentPackageBody(name="json", version="1.0", dist="json")
    with pytest.raises(HTTPException) as exc:
        await upsert_package(body, EnvBroker())
    assert exc.value.status_code == 400
    stamp = NodeEnv(env_dir).read_stamp()
    assert stamp.generation == 1
    assert set(stamp.packages) == {"numpy"}

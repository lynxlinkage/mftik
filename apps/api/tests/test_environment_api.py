"""Owner extras API — apply, then tell STS, never assume a silent broker is idle."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException
from mftik.envapply import ApplyFailed, ApplySpec
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import (
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_RELOAD,
    STS_SESSION_LIST,
    ListSessionsResult,
    ListSessionsResultEnvelope,
    SessionInfo,
    StsRegistryGenerationResult,
    StsRegistryGenerationResultEnvelope,
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
    """Answers session list, registry reload, and the read-only generation RPC."""

    def __init__(
        self,
        *,
        live: list[str] | None = None,
        live_after: list[str] | None = None,
        list_silent: bool = False,
        reload_silent: bool = False,
        generation_silent: bool = False,
        generation: int | None = None,
    ) -> None:
        self.live = list(live or [])
        self.live_after = live_after
        self.list_silent = list_silent
        self.reload_silent = reload_silent
        self.generation_silent = generation_silent
        self.generation = generation
        self.list_calls = 0
        self.reload_calls = 0
        self.generation_calls = 0

    def _generation(self) -> int:
        stamp = NodeEnv.from_env().read_stamp()
        if self.generation is not None:
            return self.generation
        return stamp.generation

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
            return StsRegistryReloadResultEnvelope.wrap(
                StsRegistryReloadResult(loaded=[], generation=self._generation()),
                type=STS_REGISTRY_RELOAD,
                source="sts",
            )
        if envelope.type == STS_REGISTRY_GENERATION:
            self.generation_calls += 1
            if self.generation_silent:
                raise DomainRpcError("timeout", "no reply from sts")
            return StsRegistryGenerationResultEnvelope.wrap(
                StsRegistryGenerationResult(generation=self._generation()),
                type=STS_REGISTRY_GENERATION,
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


async def _upsert(
    name: str,
    version: str | None,
    dist: str,
    broker: EnvBroker,
    *,
    force: bool = False,
) -> object:
    """Call the route the way FastAPI would.

    ``force`` must be passed. Called directly, the parameter default is the
    ``Query(False)`` sentinel, which is truthy — a test that omits it is
    silently exercising the force path and will not see a 409.
    """
    return await upsert_package(
        EnvironmentPackageBody(name=name, version=version, dist=dist),
        broker=broker,
        force=force,
        owner=1,
        principal=Principal.owner(1, via="password"),
    )


async def test_get_on_a_fresh_node_is_empty(env_dir: Path) -> None:
    broker = EnvBroker()
    out = await get_environment(broker=broker)
    assert out.packages == {}
    assert out.generation == 0
    assert out.bytes == 0
    assert out.abi_ok is True
    assert broker.reload_calls == 0
    assert broker.generation_calls == 1


async def test_get_does_not_reload_the_registry(env_dir: Path) -> None:
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    broker = EnvBroker(generation=0)
    got = await get_environment(broker=broker)
    assert got.generation == 1
    assert got.restart_required is True
    assert broker.reload_calls == 0
    assert broker.generation_calls == 1


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

    anon = await registry_info()
    assert anon.env_generation == 1
    assert "numpy" in anon.extras
    assert anon.extras["numpy"].version is None
    assert anon.extras["numpy"].dist is None

    keyed = await registry_info(principal=Principal.owner(1, via="password"))
    assert keyed.extras["numpy"].version == "1.26.4"
    assert keyed.extras["numpy"].dist == "numpy"
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


async def test_two_upserts_of_different_names_both_survive(env_dir: Path) -> None:
    """The merge is under the lock. A stale read-then-replace would drop one."""
    owner = Principal.owner(1, via="password")
    first = await upsert_package(
        EnvironmentPackageBody(name="numpy", version="1.0", dist="numpy"),
        broker=EnvBroker(),
        force=False,
        owner=1,
        principal=owner,
    )
    second = await upsert_package(
        EnvironmentPackageBody(name="sklearn", version="1.4", dist="scikit-learn"),
        broker=EnvBroker(),
        force=False,
        owner=1,
        principal=owner,
    )
    assert first.generation == 1
    assert second.generation == 2
    assert set(second.packages) == {"numpy", "sklearn"}
    assert NodeEnv(env_dir).extras_names() == frozenset({"numpy", "sklearn"})


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
    with pytest.raises(HTTPException) as exc:
        await _upsert("json", "1.0", "json", EnvBroker())
    assert exc.value.status_code == 400
    stamp = NodeEnv(env_dir).read_stamp()
    assert stamp.generation == 1
    assert set(stamp.packages) == {"numpy"}


def _dist_info(dest: Path, dist: str, version: str) -> None:
    info = dest / f"{dist}-{version}.dist-info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {dist}\nVersion: {version}\n"
    )


def _resolving_installer(dest: Path, packages: dict[str, ApplySpec]) -> None:
    """A stub that behaves like a resolver: it pulls a dependency in.

    ``pandas`` needs numpy, and which numpy it settles on depends on what
    else is being installed alongside it — exactly the situation the Owner
    never typed and cannot see.
    """
    _write_pkg(dest, packages)
    for name, spec in packages.items():
        _dist_info(dest, spec.dist, spec.version)
    if "pandas" in packages:
        _dist_info(dest, "numpy", "2.0" if "scipy" in packages else "1.0")


async def test_a_new_name_that_moves_a_dependency_is_disruptive(
    env_dir: Path,
) -> None:
    """Adding scipy changes no stamped name — and still swaps numpy.

    Comparing the requested names says "nothing stamped is changing", so the
    live-session gate never opens. Then the reload swings ``sys.path`` to a
    generation with a different numpy while a session is holding the old one
    in ``sys.modules``. The comparison has to be about what the resolver put
    on disk.
    """
    environment_routes.installer_for_apply = _resolving_installer
    await _put({"pandas": ("2.0", "pandas")}, EnvBroker())
    assert NodeEnv(env_dir).read_stamp().generation == 1

    with pytest.raises(HTTPException) as caught:
        await _upsert("scipy", "1.0", "scipy", EnvBroker(live=["sess-1"]))
    assert caught.value.status_code == 409
    assert "sess-1" in str(caught.value.detail)

    stamp = NodeEnv(env_dir).read_stamp()
    assert stamp.generation == 1, "the generation was aborted, not published"
    assert set(stamp.packages) == {"pandas"}


async def test_the_same_add_goes_through_with_no_live_session(
    env_dir: Path,
) -> None:
    environment_routes.installer_for_apply = _resolving_installer
    await _put({"pandas": ("2.0", "pandas")}, EnvBroker())
    out = await _upsert("scipy", "1.0", "scipy", EnvBroker())
    assert out.generation == 2
    assert set(out.packages) == {"pandas", "scipy"}
    # numpy moved 1.0 → 2.0 underneath, so the process has to be restarted
    # before anything trusts what it imported.
    assert out.restart_required is True


async def test_an_add_that_moves_nothing_is_not_disruptive(
    env_dir: Path,
) -> None:
    environment_routes.installer_for_apply = _resolving_installer
    await _put({"numpy": ("1.0", "numpy")}, EnvBroker())
    out = await _upsert("httpx", "0.27", "httpx", EnvBroker(live=["sess-1"]))
    assert out.generation == 2
    assert out.restart_required is False


async def test_get_lists_dependencies_the_owner_never_named(
    env_dir: Path,
) -> None:
    """The stamp says pandas. The volume holds numpy too, and it matters.

    Without this list the Owner sees one row, a jump in ``bytes``, and a
    deploy refusing a tree over a package that is sitting on the very
    ``sys.path`` the refusal came from.
    """
    environment_routes.installer_for_apply = _resolving_installer
    await _put({"pandas": ("2.0", "pandas")}, EnvBroker())
    out = await get_environment(broker=EnvBroker())

    rows = {row.dist: row for row in out.installed}
    assert set(rows) == {"pandas", "numpy"}
    assert rows["pandas"].approved is True
    assert rows["numpy"].approved is False
    assert rows["numpy"].version == "1.0"
    # The import name to approve it under, so the UI can offer one click at
    # the version already installed rather than asking the Owner to guess.
    assert rows["numpy"].suggested_name == "numpy"


async def test_approving_a_dependency_makes_it_declarable(env_dir: Path) -> None:
    environment_routes.installer_for_apply = _resolving_installer
    await _put({"pandas": ("2.0", "pandas")}, EnvBroker())
    assert set(NodeEnv(env_dir).read_stamp().packages) == {"pandas"}

    out = await _upsert("numpy", "1.0", "numpy", EnvBroker())
    assert set(out.packages) == {"pandas", "numpy"}
    assert {row.dist for row in out.installed if not row.approved} == set()
    # Nothing moved: approving at the version already on disk is a no-op
    # install, so no live session had to be stopped for it.
    assert out.restart_required is False


def _resolving_to_latest(dest: Path, packages: dict[str, ApplySpec]) -> None:
    _write_pkg(dest, packages)
    for spec in packages.values():
        _dist_info(dest, spec.dist, spec.version or "9.9.9")


async def test_a_package_can_be_added_without_a_version(env_dir: Path) -> None:
    """Type the name, let the resolver pick, keep an exact pin afterwards.

    Requiring the Owner to look a version up on PyPI first is friction that
    also invites picking one that fights the pins already applied.
    """
    environment_routes.installer_for_apply = _resolving_to_latest
    out = await _upsert("pandas", None, "pandas", EnvBroker())
    assert out.packages["pandas"].version == "9.9.9"
    assert NodeEnv(env_dir).read_stamp().packages["pandas"].version == "9.9.9"


async def test_the_resolved_pin_is_what_later_applies_use(env_dir: Path) -> None:
    environment_routes.installer_for_apply = _resolving_to_latest
    await _upsert("pandas", None, "pandas", EnvBroker())
    seen: list[str] = []

    def recording(dest: Path, packages: dict[str, ApplySpec]) -> None:
        seen.extend(sorted(spec.requirement() for spec in packages.values()))
        _resolving_to_latest(dest, packages)

    environment_routes.installer_for_apply = recording
    out = await _upsert("httpx", "0.27", "httpx", EnvBroker())
    assert seen == ["httpx==0.27", "pandas==9.9.9"]
    assert out.restart_required is False

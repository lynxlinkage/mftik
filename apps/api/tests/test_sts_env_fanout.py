"""Env and registry reload must reach every declared STS, not the anycast pool.

Issue #93: with ``sts`` and ``sts-2``, apply / push sent ``sts.registry.reload``
on bare ``sts``. One process answered; the other kept its in-memory stamp.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from db_harness import a_database, an_instance, an_owner
from mftik.envapply import ApplySpec
from mftik.environment import EnvStamp, NodeEnv, PackageRecord
from mftik.protocol import (
    STS_ENV_SYNC,
    STS_REGISTRY_GENERATION,
    STS_REGISTRY_RELOAD,
    StsEnvPackagePin,
    StsEnvSyncResult,
    StsEnvSyncResultEnvelope,
    StsRegistryGenerationResult,
    StsRegistryGenerationResultEnvelope,
    StsRegistryReloadResult,
    StsRegistryReloadResultEnvelope,
    Topics,
)
from mftik.registry import RegistryStore
from mftik_api.auth.principal import Principal
from mftik_api.broker_rpc import DomainRpcError
from mftik_api.routes import environment as environment_routes
from mftik_api.routes.environment import get_environment, put_environment
from mftik_api.routes.registry import add_strategy
from mftik_api.schemas import EnvironmentPutBody, EnvPackageIn, RegistryAddBody
from mftik_api.sts_fanout import extras_match, list_targets

ONE = "sts"
TWO = "sts-2"

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def _write_pkg(dest: Path, packages: dict[str, ApplySpec]) -> None:
    for name in packages:
        pkg = dest / name
        pkg.mkdir()
        (pkg / "__init__.py").write_text("ok\n")


def _name_of(subject: str) -> str:
    if subject == Topics.STS:
        return "*"
    return subject.split(".", 1)[1]


class TwoStsEnv:
    """Answers env sync / generation per named instance."""

    def __init__(
        self,
        *,
        silent: set[str] | None = None,
        trailing: set[str] | None = None,
    ) -> None:
        self.silent = silent or set()
        self.trailing = trailing or set()
        self.subjects: list[str] = []
        self.sync_calls = 0
        self.generation_calls = 0

    def _stamp(self):
        return NodeEnv.from_env().read_stamp()

    def _ok_packages(self) -> dict[str, StsEnvPackagePin]:
        stamp = self._stamp()
        return {
            name: StsEnvPackagePin(version=rec.version, dist=rec.dist)
            for name, rec in stamp.packages.items()
        }

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        self.subjects.append(subject)
        name = _name_of(subject)
        if name in self.silent:
            raise DomainRpcError("timeout", f"{name} did not answer")
        stamp = self._stamp()
        behind = name in self.trailing
        packages = {} if behind else self._ok_packages()
        generation = 0 if behind else stamp.generation
        if envelope.type == STS_ENV_SYNC:
            self.sync_calls += 1
            return StsEnvSyncResultEnvelope.wrap(
                StsEnvSyncResult(
                    loaded=[],
                    generation=generation,
                    packages=packages,
                ),
                type=STS_ENV_SYNC,
                source="sts",
            )
        if envelope.type == STS_REGISTRY_GENERATION:
            self.generation_calls += 1
            return StsRegistryGenerationResultEnvelope.wrap(
                StsRegistryGenerationResult(
                    generation=generation, packages=packages
                ),
                type=STS_REGISTRY_GENERATION,
                source="sts",
            )
        raise AssertionError(f"unexpected type {envelope.type}")


class TwoStsReload:
    """Answers registry reload per instance, optionally omitting a key."""

    def __init__(
        self,
        loaded_by: dict[str, list[str]],
        *,
        silent: set[str] | None = None,
    ) -> None:
        self.loaded_by = loaded_by
        self.silent = silent or set()
        self.subjects: list[str] = []

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        assert envelope.type == STS_REGISTRY_RELOAD
        self.subjects.append(subject)
        name = _name_of(subject)
        if name in self.silent:
            raise DomainRpcError("timeout", f"{name} did not answer")
        return StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(loaded=list(self.loaded_by.get(name, []))),
            type=STS_REGISTRY_RELOAD,
            source="sts",
        )


@pytest.fixture
def env_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.setattr(environment_routes, "installer_for_apply", _write_pkg)

    async def _no_audit(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(environment_routes, "record_audit", _no_audit)
    return tmp_path


@pytest.fixture
async def two_sts(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session, ONE, "sts")
            await an_instance(session, TWO, "sts")
            await session.commit()
        from mftik_api import sts_fanout

        monkeypatch.setattr(sts_fanout, "session_scope", database.scope)
        yield database.scope


@pytest.fixture
async def one_sts(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.maker() as session:
            await an_owner(session)
            await an_instance(session, ONE, "sts")
            await session.commit()
        from mftik_api import sts_fanout

        monkeypatch.setattr(sts_fanout, "session_scope", database.scope)
        yield database.scope


async def _put(broker: TwoStsEnv) -> object:
    return await put_environment(
        EnvironmentPutBody(
            packages={"numpy": EnvPackageIn(version="1.0", dist="numpy")}
        ),
        broker=broker,
        force=False,
        owner=1,
        principal=Principal.owner(1, via="password"),
    )


async def test_two_sts_apply_hits_unicast_not_anycast(two_sts, env_dir: Path) -> None:
    broker = TwoStsEnv()
    out = await _put(broker)
    assert out.generation == 1
    assert out.loaded is True
    assert set(broker.subjects) == {Topics.sts(ONE), Topics.sts(TWO)}
    assert Topics.STS not in broker.subjects
    assert broker.sync_calls == 2


async def test_one_sts_apply_stays_anycast(one_sts, env_dir: Path) -> None:
    broker = TwoStsEnv()
    out = await _put(broker)
    assert out.loaded is True
    assert broker.subjects == [Topics.STS]
    assert broker.sync_calls == 1


async def test_one_of_two_silent_is_restart_required(two_sts, env_dir: Path) -> None:
    broker = TwoStsEnv(silent={TWO})
    out = await _put(broker)
    assert out.generation == 1
    assert NodeEnv(env_dir).read_stamp().generation == 1
    assert out.loaded is False
    assert out.restart_required is True
    assert TWO in (out.load_error or "")
    assert "did not answer" in (out.load_error or "")


async def test_one_instance_trailing_is_restart_required(
    two_sts, env_dir: Path
) -> None:
    broker = TwoStsEnv(trailing={TWO})
    out = await _put(broker)
    assert out.generation == 1
    assert out.loaded is True
    assert out.restart_required is True

    got = await get_environment(broker=TwoStsEnv(trailing={TWO}))
    assert got.restart_required is True
    assert got.generation == 1


async def test_get_generation_fans_out(two_sts, env_dir: Path) -> None:
    await _put(TwoStsEnv())
    broker = TwoStsEnv()
    got = await get_environment(broker=broker)
    assert got.restart_required is False
    assert set(broker.subjects) == {Topics.sts(ONE), Topics.sts(TWO)}
    assert broker.generation_calls == 2
    assert broker.sync_calls == 0


async def test_registry_add_loaded_only_when_both_list_the_key(
    two_sts, tmp_path: Path
) -> None:
    store = RegistryStore(tmp_path)
    key = "private::Tiny"
    both = TwoStsReload({ONE: [key], TWO: [key]})
    added = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}),
        store=store,
        broker=both,
    )
    assert added.loaded is True
    assert set(both.subjects) == {Topics.sts(ONE), Topics.sts(TWO)}

    store2 = RegistryStore(tmp_path / "other")
    one_missing = TwoStsReload({ONE: [key], TWO: []})
    added2 = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}),
        store=store2,
        broker=one_missing,
    )
    assert added2.loaded is False
    assert "did not load it" in (added2.load_error or "")


async def test_list_targets_drops_disabled(two_sts) -> None:
    from mftik_db.repositories import InstanceRepository

    async with two_sts() as db:
        repo = InstanceRepository(db)
        row = await repo.get_by_name(TWO)
        assert row is not None
        await repo.update(row, enabled=False)

    targets = await list_targets()
    assert [t.subject for t in targets] == [Topics.STS]


def test_extras_match_uses_pins_not_generation() -> None:
    stamp = EnvStamp(
        generation=5,
        python=(3, 12),
        platform="linux",
        nbytes=1,
        packages={"numpy": PackageRecord(version="1.0", dist="numpy", source="manual")},
    )
    pins = {"numpy": StsEnvPackagePin(version="1.0", dist="numpy")}
    assert extras_match(1, pins, stamp) is True
    assert extras_match(5, {}, stamp) is True, "pre-upgrade STS: generation fallback"
    assert extras_match(0, {}, stamp) is False
    drifted = {"numpy": StsEnvPackagePin(version="2.0", dist="numpy")}
    assert extras_match(5, drifted, stamp) is False

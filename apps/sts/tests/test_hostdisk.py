"""Tree replica, pins, GC, and deployable. Component: ``tmp_path``.

One test also writes a scratch sqlite row. The probe's real interpreter
is ``test_hostdisk_probe.py``.
"""

from __future__ import annotations

import json
import sys
import sysconfig
from pathlib import Path

import pytest
from mftik.environment import EnvStamp, NodeEnv
from mftik.protocol import StsRegistrySyncRequest, StsRegistryTreeOp
from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryDigestMismatch
from mftik.registry.files import normalize_files
from mftik.registry.qualify import qualify
from mftik.registry.store import RegistryStore
from mftik_sts.controller import SessionSpec
from mftik_sts.hostdisk import (
    REASON_DIGEST_ABSENT,
    REASON_ENV_ABSENT,
    REASON_REQUIRES_MFTIK,
    REASON_STRATEGY_UNAVAILABLE,
    TreeReplica,
    deployable,
    gc_env,
    pinned_code,
    rehang_code,
)
from mftik_sts.hostdisk.checks import MFTIK_DEV_RELEASE
from mftik_sts.hostdisk.sync import (
    apply_registry_sync,
    materialize_legacy_digest,
    prepare_disk,
)

pytestmark = pytest.mark.component

_TINY = (
    "from mftik.strategy import Strategy\n"
    "class Tiny(Strategy):\n"
    "    pass\n"
)
_TINY_V2 = _TINY + "# next\n"
_FUTURE = (
    "from mftik.strategy import Strategy\n"
    "class Tiny(Strategy):\n"
    '    requires_mftik = "99.0.0"\n'
)
_NUMPY = (
    "from mftik.strategy import Strategy\n"
    "import numpy\n"
    "class Tiny(Strategy):\n"
    "    requires = ('numpy',)\n"
)


def _spec(**overrides: object) -> SessionSpec:
    raw: dict[str, object] = {
        "session_id": "abc123",
        "instance": "sts",
        "strategy": "Tiny",
    }
    raw.update(overrides)
    return SessionSpec(**raw)  # type: ignore[arg-type]


def _put(replica: TreeReplica, source: str) -> str:
    files = {"strategy.py": source}
    digest = digest_files(normalize_files(files))
    replica.put(digest, files)
    return digest


def _modules(prefix: str) -> set[str]:
    return {name for name in sys.modules if name.startswith(prefix)}


def _stamp(env: NodeEnv, generation: int) -> None:
    env.root.mkdir(parents=True, exist_ok=True)
    stamp = EnvStamp(
        generation=generation,
        python=(sys.version_info[0], sys.version_info[1]),
        platform=sysconfig.get_platform(),
        nbytes=0,
        packages={},
    )
    env.stamp_path.write_text(
        json.dumps(stamp.to_json()), encoding="utf-8"
    )


def test_a_same_name_push_leaves_the_pinned_digest_on_disk(tmp_path: Path) -> None:
    """Pushing a new version moves the index. The old tree stays, so a
    session pinned to it can still be loaded from that directory."""
    replica = TreeReplica(tmp_path)
    old = _put(replica, _TINY)
    replica.bind("Tiny", old)
    new = _put(replica, _TINY_V2)
    replica.bind("Tiny", new)
    assert new != old
    assert replica.current("Tiny") == new
    previous = replica.path_of(old)
    assert previous is not None
    assert (previous / "strategy.py").read_text(encoding="utf-8") == _TINY
    current = replica.path_of(new)
    assert current is not None
    assert (current / "strategy.py").read_text(encoding="utf-8") == _TINY_V2


def test_put_refuses_a_digest_that_is_not_the_files(tmp_path: Path) -> None:
    replica = TreeReplica(tmp_path)
    digest = _put(replica, _TINY)
    with pytest.raises(RegistryDigestMismatch):
        replica.put(digest, {"strategy.py": _TINY_V2})
    kept = replica.path_of(digest)
    assert kept is not None
    assert (kept / "strategy.py").read_text(encoding="utf-8") == _TINY


def test_gc_keeps_pinned_digests_and_pinned_env_generations(tmp_path: Path) -> None:
    """A pin survives GC. So does the index's current digest, and the
    stamp's current generation. Anything else goes."""
    replica = TreeReplica(tmp_path)
    pinned = _put(replica, _TINY)
    current = _put(replica, _TINY_V2)
    loose = _put(replica, _TINY + "# loose\n")
    replica.bind("Tiny", current)
    env = NodeEnv(tmp_path)
    for generation in (1, 2, 3):
        env.site_packages(generation).mkdir(parents=True)
    _stamp(env, 3)

    spec = _spec(strategy_digest=pinned, env_generation=1)
    other = _spec(
        session_id="built-in",
        strategy="NoopStrategy",
        strategy_digest=None,
        env_generation=None,
    )
    keep = pinned_code([spec, other])
    assert keep.digests == frozenset({pinned})
    assert keep.env_generations == frozenset({1})

    removed = replica.gc(keep.digests)
    assert loose in removed
    assert replica.path_of(pinned) is not None
    assert replica.path_of(current) is not None
    assert replica.path_of(loose) is None

    gc_env(env, keep.env_generations)
    assert env.site_packages(1).is_dir()
    assert env.site_packages(3).is_dir()
    assert not (env.root / "gen-2").exists()


def test_rehang_uses_the_digest_pinned_on_the_spec(tmp_path: Path) -> None:
    """The index has moved. The rehang still names the spec's digest."""
    replica = TreeReplica(tmp_path)
    pinned = _put(replica, _TINY)
    current = _put(replica, _TINY_V2)
    replica.bind("Tiny", current)
    spec = _spec(strategy_digest=pinned, env_generation=1)
    verdict = rehang_code(spec, replica=replica, release="0.1.0")
    assert verdict.digest == pinned
    assert verdict.digest != replica.current("Tiny")
    assert verdict.failed is False
    assert verdict.alert is False


def test_rehang_is_failed_when_requires_mftik_rejects_the_release(
    tmp_path: Path,
) -> None:
    """The declaration ``RegistryStore`` records is what the check reads.
    Incompatible means failed, and alerted. The digest is still the pin."""
    files = {"strategy.py": _FUTURE}
    added = RegistryStore(tmp_path / "api").add(files)
    replica = TreeReplica(tmp_path / "sts")
    replica.put(added.digest, files)
    replica.bind("Tiny", added.digest)
    spec = _spec(strategy_digest=added.digest)
    verdict = rehang_code(spec, replica=replica, release="0.1.0")
    assert added.requires_mftik == "99.0.0"
    assert verdict.failed is True
    assert verdict.reason == REASON_REQUIRES_MFTIK
    assert verdict.alert is True
    assert verdict.digest == added.digest
    assert verdict.digest == replica.current("Tiny")


def test_a_missing_pinned_tree_is_strategy_unavailable(tmp_path: Path) -> None:
    replica = TreeReplica(tmp_path)
    digest = "sha256:" + "ff" * 32
    verdict = rehang_code(
        _spec(strategy_digest=digest), replica=replica, release="0.1.0"
    )
    assert verdict.failed is True
    assert verdict.reason == REASON_STRATEGY_UNAVAILABLE
    assert verdict.alert is False
    assert verdict.digest == digest


def test_a_built_in_strategy_is_not_failed_for_requires_mftik(tmp_path: Path) -> None:
    verdict = rehang_code(_spec(), replica=TreeReplica(tmp_path), release="0.0.0")
    assert verdict.digest is None
    assert verdict.failed is False


def test_deployable_checks_the_disk_and_does_not_import(tmp_path: Path) -> None:
    replica = TreeReplica(tmp_path)
    digest = _put(replica, _TINY)
    env = NodeEnv(tmp_path)
    env.site_packages(2).mkdir(parents=True)
    _stamp(env, 2)
    before = _modules("_mftik_reg_")
    ready = deployable(
        _spec(strategy_digest=digest, env_generation=2),
        replica=replica,
        env=env,
    )
    assert ready.ok is True
    assert ready.reason is None
    assert _modules("_mftik_reg_") == before

    missing_digest = "sha256:" + "ee" * 32
    absent = deployable(
        _spec(strategy_digest=missing_digest), replica=replica, env=env
    )
    assert absent.ok is False
    assert absent.reason == REASON_DIGEST_ABSENT

    no_gen = deployable(
        _spec(strategy_digest=digest, env_generation=9),
        replica=replica,
        env=env,
    )
    assert no_gen.ok is False
    assert no_gen.reason == REASON_ENV_ABSENT

    needy = _put(replica, _NUMPY)
    refused = deployable(
        _spec(strategy_digest=needy, env_generation=2),
        replica=replica,
        env=env,
    )
    assert refused.ok is False
    assert refused.reason == "requires: numpy"
    assert _modules("_mftik_reg_") == before


def test_materialize_copies_one_digest_and_does_not_bind(
    tmp_path: Path,
) -> None:
    added = RegistryStore(tmp_path).add({"strategy.py": _TINY})
    replica = TreeReplica(tmp_path)
    assert replica.path_of(added.digest) is None
    assert materialize_legacy_digest(replica, added.digest)
    assert replica.path_of(added.digest) is not None
    assert replica.current(qualify(added.origin, added.type)) is None
    assert Path(added.path).is_dir()


async def test_a_spawn_guard_copies_a_name_directory_before_it_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pin whose bytes are only in the API store is runnable, and stays there."""
    from types import SimpleNamespace

    from mftik.procman import Supervisor
    from mftik_sts.controller import StsOrchestrator

    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.setenv(MFTIK_DEV_RELEASE, "1")
    added = RegistryStore(tmp_path).add({"strategy.py": _TINY})
    orch = StsOrchestrator(
        Supervisor(tmp_path / "work", plane="sts", instance="sts")
    )
    spec = SessionSpec(
        session_id="abc123",
        instance="sts",
        strategy=qualify(added.origin, added.type),
        strategy_digest=added.digest,
    )
    assert await orch._code_guard(SimpleNamespace(spec=spec)) is None  # noqa: SLF001
    assert TreeReplica(tmp_path).path_of(added.digest) is not None
    assert Path(added.path).is_dir()
    assert (
        TreeReplica(tmp_path).current(qualify(added.origin, added.type)) is None
    )


def test_prepare_disk_copies_a_legacy_tree_and_leaves_it(tmp_path: Path) -> None:
    added = RegistryStore(tmp_path).add({"strategy.py": _TINY})
    assert prepare_disk(tmp_path)
    replica = TreeReplica(tmp_path)
    assert replica.current(qualify("private", added.type)) == added.digest
    assert Path(added.path).is_dir()
    assert replica.path_of(added.digest) is not None


def _class_source(class_name: str) -> str:
    return (
        "from mftik.strategy import Strategy\n"
        f"class {class_name}(Strategy):\n"
        "    pass\n"
    )


def test_a_full_retain_on_shared_data_keeps_the_api_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The name directories are the API store. A retain sync leaves them."""
    from mftik_api.orchestrate import resolve_start_pins

    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    store = RegistryStore(tmp_path)
    public = store.add({"strategy.py": _class_source("PublicOne")}, origin="public")
    private = store.add(
        {"strategy.py": _class_source("PrivateOne")}, origin="private"
    )
    pulled = store.add({"strategy.py": _class_source("PulledOne")}, origin="peer")
    added = (public, private, pulled)
    keys = [qualify(rec.origin, rec.type) for rec in added]
    apply_registry_sync(
        StsRegistrySyncRequest(trees=[], reload=False, retain=keys),
        keep_digests=frozenset(),
        data_dir=tmp_path,
    )
    listed = {
        (rec.origin, rec.type): rec.digest
        for rec in RegistryStore(tmp_path).list_all()
    }
    for rec in added:
        assert listed[(rec.origin, rec.type)] == rec.digest
        assert Path(rec.path).is_dir()
        digest, _generation = resolve_start_pins(qualify(rec.origin, rec.type))
        assert digest == rec.digest


def test_a_partial_sync_does_not_delete_legacy_dirs(tmp_path: Path) -> None:
    added = RegistryStore(tmp_path).add({"strategy.py": _TINY})
    request = StsRegistrySyncRequest(
        trees=[
            StsRegistryTreeOp(
                op="upsert",
                origin="private",
                name=added.type,
                digest=added.digest,
                files={"strategy.py": _TINY},
            )
        ],
        reload=False,
    )
    apply_registry_sync(request, keep_digests=frozenset(), data_dir=tmp_path)
    assert Path(added.path).is_dir()


def test_an_unreadable_legacy_tree_is_left_in_place(tmp_path: Path) -> None:
    added = RegistryStore(tmp_path).add({"strategy.py": _TINY})
    junk = tmp_path / "registry" / "private" / "Junk"
    junk.mkdir()
    (junk / "readme.txt").write_text("not a strategy\n", encoding="utf-8")
    key = qualify("private", added.type)
    request = StsRegistrySyncRequest(trees=[], reload=False, retain=[key])
    apply_registry_sync(request, keep_digests=frozenset(), data_dir=tmp_path)
    assert Path(added.path).is_dir()
    assert junk.is_dir()


async def test_a_null_digest_session_rehangs_from_the_name_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live row with a null digest still loads by name after a retain sync."""
    from types import SimpleNamespace

    from db_harness import a_database, an_owner
    from mftik.procman import Supervisor
    from mftik_db.repositories.session import StsSessionRepository
    from mftik_sts.controller import StsOrchestrator
    from mftik_sts.controller.status import DbStatusStore
    from mftik_sts.hostdisk.identity import STRATEGY_DIGEST_ENV
    from mftik_sts.session_worker.process import load_strategy

    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    monkeypatch.delenv(STRATEGY_DIGEST_ENV, raising=False)
    added = RegistryStore(tmp_path).add({"strategy.py": _class_source("NameLoad")})
    key = qualify(added.origin, added.type)
    database_cm = a_database()
    database = await database_cm.__aenter__()
    try:
        async with database.scope() as session:
            await an_owner(session)
            await StsSessionRepository(session).create_live(
                session_id="abc123",
                created_by=1,
                type=key,
                instance="sts",
                strategy_digest=None,
                env_generation=None,
            )
        orch = StsOrchestrator(
            Supervisor(tmp_path / "work", plane="sts", instance="sts"),
            store=DbStatusStore(database.scope),
        )
        digests, generations = await orch.code_pins()
        assert digests == frozenset()
        assert generations == frozenset()
        apply_registry_sync(
            StsRegistrySyncRequest(trees=[], reload=False, retain=[key]),
            keep_digests=digests,
            data_dir=tmp_path,
        )
        assert Path(added.path).is_dir()
        spec = SessionSpec(
            session_id="abc123",
            instance="sts",
            strategy=key,
            strategy_digest=None,
            env_generation=None,
        )
        assert await orch._code_guard(SimpleNamespace(spec=spec)) is None  # noqa: SLF001
        assert STRATEGY_DIGEST_ENV not in orch._worker_env(spec)  # noqa: SLF001
        loaded = load_strategy(key)
        assert type(loaded).__name__ == "NameLoad"
    finally:
        await database_cm.__aexit__(None, None, None)

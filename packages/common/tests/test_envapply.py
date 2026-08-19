from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from mftik.envapply import (
    APPLY_TIMEOUT_S,
    ApplyFailed,
    ApplyInProgress,
    ApplySpec,
    EnvironmentDisruptive,
    EnvironmentMissing,
    apply_packages,
    run_uv_installer,
)
from mftik.environment import NodeEnv


def _env_root(tmp_path: Path) -> Path:
    return tmp_path / "env"


def _write_pkg(dest: Path, packages: dict[str, ApplySpec]) -> None:
    for name in packages:
        pkg = dest / name
        pkg.mkdir()
        (pkg / "__init__.py").write_text("ok\n")


def _boom(dest: Path, packages: dict[str, ApplySpec]) -> None:
    raise ApplyFailed("nope")


def test_apply_writes_generation(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    spec = ApplySpec(version="1.26.4", dist="numpy", source="manual")
    result = apply_packages(env, {"numpy": spec}, installer=_write_pkg)
    assert result.restart_required is False
    assert result.stamp.generation == 1
    root = _env_root(tmp_path)
    assert (root / "gen-1" / "site-packages" / "numpy" / "__init__.py").is_file()
    assert env.current_path.resolve() == (root / "gen-1" / "site-packages").resolve()
    assert env.extras_names() == frozenset({"numpy"})


def test_installer_failure_leaves_stamp(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    spec = ApplySpec(version="1.0", dist="numpy")
    apply_packages(env, {"numpy": spec}, installer=_write_pkg)
    before = env.read_stamp()
    with pytest.raises(ApplyFailed, match="nope"):
        apply_packages(
            env,
            {
                "numpy": spec,
                "sklearn": ApplySpec(version="1.4", dist="scikit-learn"),
            },
            installer=_boom,
        )
    after = env.read_stamp()
    assert after.generation == before.generation
    assert after.packages == before.packages
    root = _env_root(tmp_path)
    assert env.current_path.resolve() == (root / "gen-1" / "site-packages").resolve()
    assert not (root / "gen-2").exists()


def test_change_without_force_does_not_run_installer(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.26.4", dist="numpy")},
        installer=_write_pkg,
    )
    calls: list[str] = []

    def record(dest: Path, packages: dict[str, ApplySpec]) -> None:
        calls.append("ran")
        _write_pkg(dest, packages)

    with pytest.raises(EnvironmentDisruptive) as exc:
        apply_packages(
            env,
            {"numpy": ApplySpec(version="2.0.0", dist="numpy")},
            installer=record,
        )
    assert exc.value.names == ("numpy",)
    assert calls == []
    assert env.read_stamp().packages["numpy"].version == "1.26.4"


def test_remove_without_force_is_disruptive(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.26.4", dist="numpy")},
        installer=_write_pkg,
    )
    with pytest.raises(EnvironmentDisruptive):
        apply_packages(env, {}, installer=_write_pkg)
    assert env.extras_names() == frozenset({"numpy"})


def test_change_with_force_sets_restart_required(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.26.4", dist="numpy")},
        installer=_write_pkg,
    )
    result = apply_packages(
        env,
        {"numpy": ApplySpec(version="2.0.0", dist="numpy")},
        allow_disruptive=True,
        installer=_write_pkg,
    )
    assert result.restart_required is True
    assert result.stamp.generation == 2
    assert env.read_stamp().packages["numpy"].version == "2.0.0"


def test_add_is_not_disruptive(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.26.4", dist="numpy")},
        installer=_write_pkg,
    )
    result = apply_packages(
        env,
        {
            "numpy": ApplySpec(version="1.26.4", dist="numpy"),
            "sklearn": ApplySpec(version="1.4.0", dist="scikit-learn"),
        },
        installer=_write_pkg,
    )
    assert result.restart_required is False
    assert env.extras_names() == frozenset({"numpy", "sklearn"})


def test_second_apply_drops_predecessor(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    env.ensure_current()
    root = _env_root(tmp_path)
    assert (root / "gen-0").is_dir()
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_write_pkg,
    )
    apply_packages(
        env,
        {
            "numpy": ApplySpec(version="1.0", dist="numpy"),
            "sklearn": ApplySpec(version="1.4", dist="scikit-learn"),
        },
        installer=_write_pkg,
    )
    assert env.read_stamp().generation == 2
    assert (root / "gen-1").is_dir()
    assert (root / "gen-2").is_dir()
    assert not (root / "gen-0").exists()


def test_stamp_versions_come_from_target_metadata(tmp_path: Path) -> None:
    def plant(dest: Path, packages: dict[str, ApplySpec]) -> None:
        _write_pkg(dest, packages)
        info = dest / "numpy-9.9.9.dist-info"
        info.mkdir()
        (info / "METADATA").write_text(
            "Metadata-Version: 2.1\nName: numpy\nVersion: 9.9.9\n",
            encoding="utf-8",
        )

    env = NodeEnv(tmp_path)
    result = apply_packages(
        env,
        {"numpy": ApplySpec(version="1.26.4", dist="numpy")},
        installer=plant,
    )
    assert result.stamp.packages["numpy"].version == "9.9.9"


def test_before_commit_abort_leaves_stamp(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_write_pkg,
    )
    before = env.read_stamp()

    def boom() -> None:
        raise RuntimeError("session appeared")

    with pytest.raises(RuntimeError, match="session appeared"):
        apply_packages(
            env,
            {
                "numpy": ApplySpec(version="1.0", dist="numpy"),
                "sklearn": ApplySpec(version="1.4", dist="scikit-learn"),
            },
            installer=_write_pkg,
            before_commit=boom,
        )
    after = env.read_stamp()
    assert after.generation == before.generation
    assert after.packages == before.packages
    assert not (_env_root(tmp_path) / "gen-2").exists()


def test_sequential_upserts_keep_both_names(tmp_path: Path) -> None:
    """Two adds that both started from ``{numpy}`` must not drop one of them.

    That is the two-tab race: each read the stamp, then the later commit
    used to write a full replacement set built from that stale read.
    Merge now happens after the flock, so the second add sees the first.
    """
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_write_pkg,
    )
    with ApplyInProgress(
        env,
        upsert={"sklearn": ApplySpec(version="1.4", dist="scikit-learn")},
        installer=_write_pkg,
    ) as first:
        first.commit()
    with ApplyInProgress(
        env,
        upsert={"torch": ApplySpec(version="2.0", dist="torch")},
        installer=_write_pkg,
    ) as second:
        second.commit()
    assert env.extras_names() == frozenset({"numpy", "sklearn", "torch"})
    assert env.read_stamp().generation == 3


def test_remove_of_a_name_the_lock_no_longer_has(tmp_path: Path) -> None:
    env = NodeEnv(tmp_path)
    apply_packages(
        env,
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=_write_pkg,
    )
    apply_packages(env, {}, allow_disruptive=True, installer=_write_pkg)
    with pytest.raises(EnvironmentMissing, match="numpy"):
        with ApplyInProgress(env, remove={"numpy"}, installer=_write_pkg):
            pass


def test_uv_installer_pins_and_binary_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[list[str]] = []
    timeouts: list[object] = []

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        seen.append(cmd)
        timeouts.append(kwargs.get("timeout"))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("mftik.envapply.subprocess.run", fake_run)
    monkeypatch.setenv("UV_INDEX_URL", "https://index.example/simple")
    dest = tmp_path / "site-packages"
    dest.mkdir()
    run_uv_installer(
        dest,
        {"numpy": ApplySpec(version="1.26.4", dist="numpy")},
    )
    cmd = seen[0]
    assert "--only-binary" in cmd
    assert ":all:" in cmd
    assert "numpy==1.26.4" in cmd
    assert "--index-url" in cmd
    assert "https://index.example/simple" in cmd
    assert timeouts == [APPLY_TIMEOUT_S]


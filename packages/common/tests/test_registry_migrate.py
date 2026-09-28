"""Offline rename of registry trees onto their class name."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from mftik.cli.app import main
from mftik.registry import migrate
from mftik.registry.errors import RegistryError
from mftik.registry.migrate import migrate_registry


def _plant(root: Path, dirname: str, cls: str) -> None:
    dest = root / dirname
    dest.mkdir(parents=True)
    (dest / "strategy.py").write_text(
        "from mftik.strategy import Strategy\n"
        f"\nclass {cls}(Strategy):\n"
        "    pass\n"
    )


def test_short_directories_move_through_a_temp_name_and_pulled_goes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    public = data / "registry" / "public"
    _plant(private, "tiny", "Tiny")
    _plant(public, "other", "Other")
    pulled = data / "registry" / "pulled" / "node1" / "tiny"
    pulled.mkdir(parents=True)
    (pulled / "strategy.py").write_text("x = 1\n")

    calls: list[tuple[str, str]] = []
    real = Path.rename

    def spy(self: Path, target: Path) -> Path:
        calls.append((self.name, Path(target).name))
        return real(self, target)

    monkeypatch.setattr(Path, "rename", spy)
    result = migrate_registry(data)

    assert any(name.startswith(".tmp-migrate-") for pair in calls for name in pair)
    assert os.listdir(private) == ["Tiny"]
    assert os.listdir(public) == ["Other"]
    assert "class Tiny" in (private / "Tiny" / "strategy.py").read_text()
    assert not (data / "registry" / "pulled" / "node1").exists()
    assert result.removed_pulled == ("node1",)
    assert len(result.renamed) == 2


def test_a_second_run_changes_nothing(tmp_path: Path) -> None:
    data = tmp_path / "data"
    _plant(data / "registry" / "private", "Tiny", "Tiny")

    result = migrate_registry(data)

    assert result.renamed == ()
    assert result.removed_pulled == ()
    assert os.listdir(data / "registry" / "private") == ["Tiny"]


def test_duplicate_class_names_refuse_the_whole_batch(tmp_path: Path) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "tiny", "Tiny")
    _plant(private, "other", "Tiny")
    pulled = data / "registry" / "pulled" / "node1"
    pulled.mkdir(parents=True)

    with pytest.raises(RegistryError, match="collide"):
        migrate_registry(data)

    assert {p.name for p in private.iterdir()} == {"tiny", "other"}
    assert pulled.is_dir()


def test_casefold_targets_refuse_the_whole_batch(tmp_path: Path) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "left", "Tiny")
    _plant(private, "right", "tiny")

    with pytest.raises(RegistryError, match="collide"):
        migrate_registry(data)

    assert {p.name for p in private.iterdir()} == {"left", "right"}


def test_a_failed_landing_puts_every_tree_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The second phase fails on the filesystem. Nothing stays parked."""
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "tiny", "Tiny")
    _plant(private, "other", "Other")
    pulled = data / "registry" / "pulled" / "node1"
    pulled.mkdir(parents=True)

    real = Path.rename

    def refuse_class_names(self: Path, target: Path) -> Path:
        # Only the landing renames. The rollback moves trees back onto their
        # old names and has to be allowed to finish.
        if Path(target).name in {"Tiny", "Other"}:
            raise OSError(13, "permission denied")
        return real(self, target)

    monkeypatch.setattr(Path, "rename", refuse_class_names)
    with pytest.raises(OSError):
        migrate_registry(data)

    assert sorted(os.listdir(private)) == ["other", "tiny"]
    assert "class Tiny" in (private / "tiny" / "strategy.py").read_text()
    assert pulled.is_dir()


def test_a_refusal_in_the_second_phase_puts_every_tree_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A class name that appeared mid-run refuses the batch, not half of it."""
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "tiny", "Tiny")
    _plant(private, "other", "Other")

    monkeypatch.setattr(migrate, "_taken_by", lambda root, name: "Tiny")
    with pytest.raises(RegistryError, match="while the migration was moving"):
        migrate_registry(data)

    assert sorted(os.listdir(private)) == ["other", "tiny"]


def test_a_tree_that_already_landed_goes_back_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first tree lands, the second cannot. Both end up where they were."""
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "aaa", "Aaa")
    _plant(private, "bbb", "Bbb")

    real = Path.rename

    def refuse_the_second(self: Path, target: Path) -> Path:
        if Path(target).name == "Bbb":
            raise OSError(13, "permission denied")
        return real(self, target)

    monkeypatch.setattr(Path, "rename", refuse_the_second)
    with pytest.raises(OSError):
        migrate_registry(data)

    assert sorted(os.listdir(private)) == ["aaa", "bbb"]
    assert "class Aaa" in (private / "aaa" / "strategy.py").read_text()


def test_a_run_killed_mid_rename_is_recovered_by_the_next_one(
    tmp_path: Path
) -> None:
    """A leftover temp directory is not an empty registry."""
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, f"{migrate.TMP_PREFIX}4242-tiny", "Tiny")
    _plant(private, "other", "Other")

    result = migrate_registry(data)

    assert sorted(os.listdir(private)) == ["Other", "Tiny"]
    assert [new for _old, new in result.recovered] == [str(private / "tiny")]
    assert (str(private / "tiny"), str(private / "Tiny")) in result.renamed


def test_a_leftover_temp_directory_whose_name_is_taken_is_refused(
    tmp_path: Path
) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, f"{migrate.TMP_PREFIX}4242-tiny", "Tiny")
    _plant(private, "tiny", "Other")

    with pytest.raises(RegistryError, match="interrupted migration"):
        migrate_registry(data)

    assert sorted(os.listdir(private)) == [
        f"{migrate.TMP_PREFIX}4242-tiny",
        "tiny",
    ]


def test_a_leftover_temp_directory_that_does_not_say_its_name_is_refused(
    tmp_path: Path
) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, f"{migrate.TMP_PREFIX}4242", "Tiny")

    with pytest.raises(RegistryError, match="does not say what it was called"):
        migrate_registry(data)


def test_the_command_reports_a_recovery_rather_than_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, f"{migrate.TMP_PREFIX}4242-tiny", "Tiny")

    assert main(["registry-migrate", "--data", str(data)]) == 0
    out = capsys.readouterr().out
    assert "already uses class names" not in out
    assert "left by an interrupted run" in out
    assert os.listdir(private) == ["Tiny"]


def test_the_command_is_idempotent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data = tmp_path / "data"
    _plant(data / "registry" / "private", "tiny", "Tiny")

    assert main(["registry-migrate", "--data", str(data)]) == 0
    assert "renamed" in capsys.readouterr().out
    assert main(["registry-migrate", "--data", str(data)]) == 0
    assert "already uses class names" in capsys.readouterr().out
    assert os.listdir(data / "registry" / "private") == ["Tiny"]

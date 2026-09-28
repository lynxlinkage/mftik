"""Offline rename of registry trees onto their class name."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from mftik.cli.app import main
from mftik.registry import RegistryStore, migrate, qualify
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


def test_short_directories_move_through_a_temp_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    public = data / "registry" / "public"
    _plant(private, "tiny", "Tiny")
    _plant(public, "other", "Other")

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
    assert len(result.renamed) == 2


def test_a_second_run_changes_nothing(tmp_path: Path) -> None:
    data = tmp_path / "data"
    _plant(data / "registry" / "private", "Tiny", "Tiny")

    result = migrate_registry(data)

    assert result.renamed == ()
    assert result.dropped_pulled == ()
    assert os.listdir(data / "registry" / "private") == ["Tiny"]


def test_a_pulled_copy_is_renamed_and_resolves_under_its_remote(
    tmp_path: Path
) -> None:
    """Renamed, not deleted.

    The boot scan is the only thing that restores an interrupted session, so
    a session that was running ``node1::Tiny`` needs that key present when
    STS starts — not after somebody runs ``connect`` again.
    """
    data = tmp_path / "data"
    pulled = data / "registry" / "pulled"
    _plant(pulled / "node1", "tiny", "Tiny")
    _plant(pulled / "node2", "tiny", "Tiny")

    result = migrate_registry(data)

    assert result.dropped_pulled == ()
    assert os.listdir(pulled / "node1") == ["Tiny"]
    assert os.listdir(pulled / "node2") == ["Tiny"]
    store = RegistryStore(data)
    assert sorted(
        qualify(rec.origin, rec.type) for rec in store.list_pulled()
    ) == ["node1::Tiny", "node2::Tiny"]


def test_a_rerun_after_connect_leaves_pulled_copies_alone(
    tmp_path: Path
) -> None:
    """Copies already named by their class are what ``connect`` writes now."""
    data = tmp_path / "data"
    pulled = data / "registry" / "pulled"
    _plant(pulled / "node1", "Tiny", "Tiny")
    _plant(data / "registry" / "private", "Own", "Own")

    result = migrate_registry(data)

    assert (result.renamed, result.dropped_pulled, result.recovered) == ((), (), ())
    assert os.listdir(pulled / "node1") == ["Tiny"]
    assert "class Tiny" in (pulled / "node1" / "Tiny" / "strategy.py").read_text()


def test_a_pulled_copy_nothing_can_name_is_dropped(tmp_path: Path) -> None:
    """It cannot be renamed, no listing shows it, and it holds a name.

    ``connect`` fetching ``Tiny`` later would collide with the directory
    called ``tiny``, so leaving it is what would cost the copy for good.
    """
    data = tmp_path / "data"
    pulled = data / "registry" / "pulled"
    broken = pulled / "node1" / "tiny"
    broken.mkdir(parents=True)
    (broken / "strategy.py").write_text("x = 1\n")
    _plant(data / "registry" / "private", "own", "Own")

    result = migrate_registry(data)

    assert result.dropped_pulled == (str(broken),)
    assert not broken.exists()
    assert os.listdir(data / "registry" / "private") == ["Own"]


def test_an_unreadable_own_tree_still_refuses_the_batch(tmp_path: Path) -> None:
    """This node's own trees are not copies. Nothing here is disposable."""
    data = tmp_path / "data"
    private = data / "registry" / "private"
    broken = private / "tiny"
    broken.mkdir(parents=True)
    (broken / "strategy.py").write_text("x = 1\n")
    _plant(private, "other", "Other")

    with pytest.raises(RegistryError):
        migrate_registry(data)

    assert sorted(os.listdir(private)) == ["other", "tiny"]


def test_a_collision_between_two_copies_of_one_peer_refuses_the_batch(
    tmp_path: Path
) -> None:
    """The peer's registry is what has to change. Disconnect is the way past."""
    data = tmp_path / "data"
    pulled = data / "registry" / "pulled"
    _plant(pulled / "node1", "one", "Tiny")
    _plant(pulled / "node1", "two", "Tiny")
    _plant(data / "registry" / "private", "own", "Own")

    with pytest.raises(RegistryError, match="collide"):
        migrate_registry(data)

    assert sorted(os.listdir(pulled / "node1")) == ["one", "two"]
    assert os.listdir(data / "registry" / "private") == ["own"]


def test_a_failure_rolls_back_pulled_copies_and_drops_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One rename fails. Own trees, pulled copies and the drop list all hold."""
    data = tmp_path / "data"
    private = data / "registry" / "private"
    pulled = data / "registry" / "pulled"
    _plant(private, "own", "Own")
    _plant(pulled / "node1", "tiny", "Tiny")
    broken = pulled / "node1" / "junk"
    broken.mkdir(parents=True)
    (broken / "strategy.py").write_text("x = 1\n")

    real = Path.rename

    def refuse_the_pulled_landing(self: Path, target: Path) -> Path:
        dest = Path(target)
        if dest.name == "Tiny" and dest.parent.name == "node1":
            raise OSError(13, "permission denied")
        return real(self, target)

    monkeypatch.setattr(Path, "rename", refuse_the_pulled_landing)
    with pytest.raises(OSError):
        migrate_registry(data)

    assert os.listdir(private) == ["own"]
    assert sorted(os.listdir(pulled / "node1")) == ["junk", "tiny"]
    assert (broken / "strategy.py").is_file()


def test_a_pulled_copy_left_parked_is_recovered(tmp_path: Path) -> None:
    data = tmp_path / "data"
    node1 = data / "registry" / "pulled" / "node1"
    _plant(node1, f"{migrate.TMP_PREFIX}4242-tiny", "Tiny")

    result = migrate_registry(data)

    assert os.listdir(node1) == ["Tiny"]
    assert [new for _old, new in result.recovered] == [str(node1 / "tiny")]


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


def test_a_cycle_that_fails_half_way_goes_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``Aaa`` holds class ``Bbb`` and ``Bbb`` holds class ``Aaa``.

    Each one's target is the other's current name, which is the case the
    temp names exist for — and the case a one-pass rollback would put a tree
    back onto a name the other tree is standing on.
    """
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "Aaa", "Bbb")
    _plant(private, "Bbb", "Aaa")

    real = Path.rename
    landings: list[str] = []

    def refuse_the_second_landing(self: Path, target: Path) -> Path:
        dest = Path(target)
        if not dest.name.startswith(migrate.TMP_PREFIX):
            landings.append(dest.name)
            if len(landings) == 2:
                raise OSError(13, "permission denied")
        return real(self, target)

    monkeypatch.setattr(Path, "rename", refuse_the_second_landing)
    with pytest.raises(OSError):
        migrate_registry(data)

    assert sorted(os.listdir(private)) == ["Aaa", "Bbb"]
    assert "class Bbb" in (private / "Aaa" / "strategy.py").read_text()
    assert "class Aaa" in (private / "Bbb" / "strategy.py").read_text()


def test_a_cycle_completes_when_nothing_fails(tmp_path: Path) -> None:
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "Aaa", "Bbb")
    _plant(private, "Bbb", "Aaa")

    migrate_registry(data)

    assert "class Aaa" in (private / "Aaa" / "strategy.py").read_text()
    assert "class Bbb" in (private / "Bbb" / "strategy.py").read_text()


def test_a_case_only_rename_that_fails_goes_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``tiny`` to ``Tiny`` is the rename the temp name is mandatory for.

    The rollback has to put it back under the case it came in with, which on
    a case-insensitive volume is the same directory answering to both names.
    """
    data = tmp_path / "data"
    private = data / "registry" / "private"
    _plant(private, "tiny", "Tiny")

    real = Path.rename

    def refuse_the_landing(self: Path, target: Path) -> Path:
        if Path(target).name == "Tiny":
            raise OSError(13, "permission denied")
        return real(self, target)

    monkeypatch.setattr(Path, "rename", refuse_the_landing)
    with pytest.raises(OSError):
        migrate_registry(data)

    assert os.listdir(private) == ["tiny"]
    assert "class Tiny" in (private / "tiny" / "strategy.py").read_text()


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

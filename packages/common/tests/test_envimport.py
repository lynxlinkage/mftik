"""Preview a peer's extras without touching the stamp."""

from __future__ import annotations

from mftik.envimport import confirm_blockers, preview_import
from mftik.environment import EnvStamp, PackageRecord


def _stamp(**packages: tuple[str, str]) -> EnvStamp:
    return EnvStamp(
        generation=1 if packages else 0,
        python=(3, 12),
        platform="test",
        nbytes=0,
        packages={
            name: PackageRecord(version=version, dist=dist, source="manual")
            for name, (version, dist) in packages.items()
        },
    )


def test_object_extra_against_empty_local_is_added() -> None:
    preview = preview_import(
        EnvStamp.empty(),
        {"numpy": {"version": "2.2.1", "dist": "numpy"}},
    )
    assert [row.name for row in preview.added] == ["numpy"]
    assert preview.added[0].dist == "numpy"
    assert preview.added[0].guessed is False
    assert preview.conflicts == ()
    assert confirm_blockers(preview) == []


def test_sklearn_object_keeps_the_pypi_dist() -> None:
    preview = preview_import(
        EnvStamp.empty(),
        {"sklearn": {"version": "1.6.1", "dist": "scikit-learn"}},
    )
    row = preview.added[0]
    assert row.name == "sklearn"
    # The import name is the stamp key; the dist is what gets installed.
    # Losing this distinction is how a confirm asks an index for "sklearn".
    assert (row.dist, row.version) == ("scikit-learn", "1.6.1")
    assert row.guessed is False
    assert confirm_blockers(preview) == []


def test_legacy_flat_extra_marks_dist_guessed() -> None:
    preview = preview_import(EnvStamp.empty(), {"sklearn": "1.6.1"})
    assert preview.added[0].guessed is True
    assert preview.added[0].dist == "sklearn"
    assert preview.guessed_names == ("sklearn",)
    blockers = confirm_blockers(preview)
    assert any("guessed" in item for item in blockers)


def test_dist_override_clears_guessed() -> None:
    preview = preview_import(
        EnvStamp.empty(),
        {"sklearn": "1.6.1"},
        dist_overrides={"sklearn": "scikit-learn"},
    )
    assert preview.added[0].guessed is False
    assert preview.added[0].dist == "scikit-learn"
    assert confirm_blockers(preview) == []


def test_same_name_same_version_is_kept() -> None:
    preview = preview_import(
        _stamp(numpy=("2.2.1", "numpy")),
        {"numpy": {"version": "2.2.1", "dist": "numpy"}},
    )
    assert [row.name for row in preview.kept] == ["numpy"]
    # Only ``added`` rows are sent to apply, so a kept row never reaches the
    # installer and the local record keeps the ``source`` it was stamped with.
    assert preview.added == ()


def test_pin_clash_is_a_conflict() -> None:
    preview = preview_import(
        _stamp(numpy=("2.2.1", "numpy")),
        {"numpy": {"version": "1.26.4", "dist": "numpy"}},
    )
    assert preview.conflicts[0].local_version == "2.2.1"
    assert preview.conflicts[0].version == "1.26.4"
    assert "numpy" in confirm_blockers(preview)[0]


def test_names_without_pins_are_not_a_dist_problem() -> None:
    """An anonymous ``/info`` publishes ``{name: {}}`` — no version at all.

    Folding that into ``guessed`` sent the Owner to set a ``dist``, which
    clears the blocker and then fails the installer with "no version pin".
    Nothing they can type here supplies the version; a key from the peer is
    the only way to see it.
    """
    preview = preview_import(EnvStamp.empty(), {"numpy": {}, "sklearn": {}})
    assert preview.unpinned_names == ("numpy", "sklearn")
    assert preview.guessed_names == ()
    blockers = confirm_blockers(preview)
    assert len(blockers) == 2
    assert all("registry key" in item for item in blockers)
    assert not any("dist" in item for item in blockers)


def test_a_dist_override_cannot_unblock_an_unpinned_row() -> None:
    preview = preview_import(
        EnvStamp.empty(),
        {"sklearn": {}},
        dist_overrides={"sklearn": "scikit-learn"},
    )
    assert preview.added[0].pinned is False
    assert confirm_blockers(preview) != []


def test_an_unpinned_name_this_node_already_has_is_kept() -> None:
    """Not a conflict: the peer never published a version to conflict with."""
    preview = preview_import(_stamp(numpy=("2.2.1", "numpy")), {"numpy": {}})
    assert [row.name for row in preview.kept] == ["numpy"]
    assert preview.conflicts == ()
    assert preview.added == ()
    assert confirm_blockers(preview) == []


def test_a_pinned_object_is_still_pinned() -> None:
    preview = preview_import(
        EnvStamp.empty(), {"numpy": {"version": "2.2.1", "dist": "numpy"}}
    )
    assert preview.added[0].pinned is True
    assert preview.unpinned_names == ()

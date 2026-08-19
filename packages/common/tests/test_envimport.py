"""Preview a peer's extras without touching the stamp."""

from __future__ import annotations

from mftik.envimport import confirm_blockers, preview_import, union_specs
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
    assert preview.added[0].name == "sklearn"
    assert preview.added[0].dist == "scikit-learn"
    specs = union_specs(EnvStamp.empty(), preview, source="peer:http://peer")
    assert specs["sklearn"].requirement() == "scikit-learn==1.6.1"
    assert specs["sklearn"].source == "peer:http://peer"


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
    assert preview.added == ()
    specs = union_specs(
        _stamp(numpy=("2.2.1", "numpy")), preview, source="peer:x"
    )
    assert specs["numpy"].source == "manual"


def test_pin_clash_is_a_conflict() -> None:
    preview = preview_import(
        _stamp(numpy=("2.2.1", "numpy")),
        {"numpy": {"version": "1.26.4", "dist": "numpy"}},
    )
    assert preview.conflicts[0].local_version == "2.2.1"
    assert preview.conflicts[0].version == "1.26.4"
    assert "numpy" in confirm_blockers(preview)[0]

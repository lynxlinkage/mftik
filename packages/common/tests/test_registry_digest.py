"""Digest is the identity of the ``.py`` files passed in."""

from __future__ import annotations

from mftik.registry.digest import digest_files


def test_same_source_same_digest() -> None:
    files = {"strategy.py": b"x = 1\n"}
    assert digest_files(files) == digest_files(dict(files))


def test_path_and_body_both_count() -> None:
    a = digest_files({"a.py": b"x = 1\n"})
    b = digest_files({"b.py": b"x = 1\n"})
    c = digest_files({"a.py": b"x = 2\n"})
    assert a != b
    assert a != c
    assert a.startswith("sha256:")
    assert len(a) == len("sha256:") + 64

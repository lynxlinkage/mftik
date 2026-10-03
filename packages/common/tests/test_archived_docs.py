"""Archived design docs stay out of the live tree (B1-01, §10).

``docs/`` may only hold the files §10 names. Everything else lives in
``docs/archive/``, and a tracked file outside that directory must not
cite the old ``docs/<name>.md`` path. ``docs/archive/<name>.md`` does
not match: the name has to sit directly under ``docs/``.

One URL is left on purpose. ``packages/common/README.md`` links at the
CLI document on ``main`` (``blob/main`` plus that filename). The file
is still there. B1-03 / B10 rewrite the link when ``main`` moves.
Any other hit fails.
"""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

#: Stems moved by B1-01. ``README.md`` was already in the archive; its
#: old path was the repository root, not ``docs/README.md``.
_ARCHIVED_STEMS = (
    "Alert",
    "Artifact",
    "AuditIdentity",
    "Auth",
    "BitgetUta",
    "Broker",
    "BrokerPatterns",
    "BrokerProvisioning",
    "CLI",
    "Deribit",
    "EventLoop",
    "Instances",
    "JetStreamRemoval",
    "MdExpiry",
    "MdHandover",
    "MdOpenInterest",
    "MdVenueSubscriptions",
    "RedisRemoval",
    "StrategyEnvironment",
    "StsPause",
    "StsSessionList",
)

#: Bytes of ``docs/<stem>.md``. Built here so this file does not itself
#: contain a live path the scan would report.
_NEEDLES = tuple(
    (stem, ("docs/" + stem + ".md").encode()) for stem in _ARCHIVED_STEMS
)

#: §10, including the two files later tickets add. A name outside this
#: set at the ``docs/`` root is a file that should have been archived.
_ROOT_MARKDOWN = {
    "ARCHITECTURE.md",
    "ARCHITECTURE_CHANGE_PLAN.md",
    "TESTING.md",
    "REFACTOR_TICKETS.md",
    "Deployment.md",
}

_SKIP_DIRS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".svelte-kit",
    "dist",
    "build",
}
_SKIP_SUFFIXES = {
    ".png",
    ".ico",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".lock",
    ".woff",
    ".woff2",
    ".pdf",
    ".zip",
}

_MAIN_BLOB = "blob/main/"


def _allowed_main_blob(rel: str, line: str, stem: str) -> bool:
    """The published README still links at the CLI doc on ``main``.

    The link text and the URL both name that file. Allow the line only
    when the URL is the ``main`` blob, so a second citation in the same
    file still fails.
    """
    if rel != "packages/common/README.md" or stem != "CLI":
        return False
    return (_MAIN_BLOB + "docs/" + "CLI.md") in line


def test_docs_root_keeps_only_section_10_files() -> None:
    names = {path.name for path in (ROOT / "docs").glob("*.md")}
    extra = sorted(names - _ROOT_MARKDOWN)
    assert extra == [], (
        "docs/ root holds files §10 does not list: " + ", ".join(extra)
    )
    # The two later files (ARCHITECTURE.md, TESTING.md) may land in
    # either order. These three are already on the branch.
    required = {
        "ARCHITECTURE_CHANGE_PLAN.md",
        "REFACTOR_TICKETS.md",
        "Deployment.md",
    }
    missing = sorted(required - names)
    assert missing == [], "docs/ root is missing: " + ", ".join(missing)


def test_archive_index_names_every_archived_file() -> None:
    archive = ROOT / "docs" / "archive"
    index = (archive / "INDEX.md").read_text(encoding="utf-8")
    missing = [
        path.name
        for path in sorted(archive.glob("*.md"))
        if path.name != "INDEX.md" and f"| `{path.name}` |" not in index
    ]
    assert missing == [], "INDEX.md has no row for: " + ", ".join(missing)


def test_no_live_file_cites_an_archived_doc_at_the_old_path() -> None:
    # ``os`` rather than ``Path``: a thousand ``Path.read_bytes`` calls
    # sits on the 50 ms unit cap, and this check has to stay under it.
    root = os.fspath(ROOT)
    hits: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in _SKIP_DIRS]
        for name in filenames:
            if os.path.splitext(name)[1].lower() in _SKIP_SUFFIXES:
                continue
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            if rel.startswith("docs/archive/"):
                continue
            try:
                with open(full, "rb") as handle:
                    data = handle.read()
            except OSError:
                continue
            if b"docs/" not in data:
                continue
            matched = [stem for stem, needle in _NEEDLES if needle in data]
            if not matched:
                continue
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                continue
            for lineno, line in enumerate(text.splitlines(), 1):
                for stem in matched:
                    token = "docs/" + stem + ".md"
                    if token not in line or _allowed_main_blob(rel, line, stem):
                        continue
                    hits.append(f"{rel}:{lineno}: {token}")
    assert hits == [], (
        "these still point at docs/<archived>.md; use docs/archive/:\n  "
        + "\n  ".join(hits)
    )

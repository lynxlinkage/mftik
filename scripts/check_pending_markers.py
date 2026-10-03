#!/usr/bin/env python3
"""Fail when ``pending Yi Te`` appears under ``apps/`` or ``packages/``.

``docs/`` is not scanned. The plan and the tickets quote the phrase.
A 「待決」 marker that cites ``#286`` fails the same way.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SKIP_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "node_modules",
}
_SKIP_SUFFIX = {".pyc", ".pyo", ".so", ".png", ".jpg", ".gif", ".webp", ".whl"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def scan(root: Path) -> list[str]:
    """Paths under ``apps/`` and ``packages/`` that still carry a marker."""
    hits: list[str] = []
    for name in ("apps", "packages"):
        base = root / name
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            if any(part in _SKIP_DIRS for part in path.parts):
                continue
            if path.suffix in _SKIP_SUFFIX:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            rel = path.relative_to(root)
            for lineno, line in enumerate(text.splitlines(), 1):
                if "pending yi te" in line.casefold():
                    hits.append(f"{rel}:{lineno}: pending Yi Te")
                if "待決" in line and "#286" in line:
                    hits.append(f"{rel}:{lineno}: 待決 #286")
    return hits


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv if argv is None else argv)
    if args and args[0].endswith(".py"):
        args = args[1:]
    if len(args) > 1:
        print("usage: check_pending_markers.py [ROOT]", file=sys.stderr)
        return 2
    root = Path(args[0]) if args else repo_root()
    hits = scan(root)
    if not hits:
        return 0
    print(
        "pending Yi Te must not appear under apps/ or packages/",
        file=sys.stderr,
    )
    for hit in hits:
        print(hit, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

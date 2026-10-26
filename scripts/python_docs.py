"""Shared readers for the Python user-doc checks.

Fenced blocks in ``docs/python`` are example excerpts. Inline code is where a
public name has to appear. Both checks import this module; it is not a third
CI entry.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_FENCE = re.compile(r"^( {0,3})(`{3,}|~{3,})(.*)$")
_INFO = re.compile(r"^([A-Za-z0-9_+-]+)[ \t]+(\S+):(\d+)-(\d+)$")


class DocError(Exception):
    """A docs tree failed a check. The message is the CI log."""


@dataclass(frozen=True)
class Fence:
    """One fenced block that cites ``examples/python``."""

    path: Path
    info_path: str
    start: int
    end: int
    body: tuple[str, ...]
    line: int


def markdown_files(root: Path) -> list[Path]:
    docs = root / "docs" / "python"
    if not docs.is_dir():
        raise DocError("docs/python is missing")
    files = sorted(path for path in docs.rglob("*.md") if path.is_file())
    if not files:
        raise DocError("docs/python has no Markdown files")
    return files


def fences(text: str, source: Path) -> list[Fence]:
    """Every fence in ``text``. A fence without a source citation is an error."""
    lines = text.splitlines()
    found: list[Fence] = []
    index = 0
    while index < len(lines):
        matched = _FENCE.match(lines[index])
        if matched is None:
            index += 1
            continue
        marker = matched.group(2)
        info = matched.group(3).strip()
        opening = index + 1
        index += 1
        body: list[str] = []
        while index < len(lines) and not lines[index].lstrip().startswith(marker):
            body.append(lines[index])
            index += 1
        if index >= len(lines):
            raise DocError(f"{source}:{opening}: code block is not closed")
        if lines[index].strip() != marker:
            raise DocError(f"{source}:{index + 1}: closing fence has extra text")
        index += 1
        cited = _INFO.fullmatch(info)
        if cited is None:
            raise DocError(
                f"{source}:{opening}: code block must cite examples/python "
                "as '<language> <path>:<start>-<end>'"
            )
        start = int(cited.group(3))
        end = int(cited.group(4))
        if start < 1 or end < start:
            raise DocError(f"{source}:{opening}: line range {start}-{end} is empty")
        found.append(
            Fence(
                path=source,
                info_path=cited.group(2),
                start=start,
                end=end,
                body=tuple(body),
                line=opening,
            )
        )
    return found


def code_text(text: str) -> str:
    """Inline code spans, plus the body of every fence.

    The fence's info string is not code: a path must not satisfy the API check.
    """
    lines = text.splitlines()
    chunks: list[str] = []
    index = 0
    while index < len(lines):
        matched = _FENCE.match(lines[index])
        if matched is None:
            chunks.append(_inline(lines[index]))
            index += 1
            continue
        marker = matched.group(2)
        index += 1
        while index < len(lines) and not lines[index].lstrip().startswith(marker):
            chunks.append(lines[index])
            index += 1
        if index < len(lines):
            index += 1
    return "\n".join(chunks)


def _inline(line: str) -> str:
    spans: list[str] = []
    index = 0
    while index < len(line):
        start = line.find("`", index)
        if start < 0:
            break
        width = 1
        while start + width < len(line) and line[start + width] == "`":
            width += 1
        end = line.find("`" * width, start + width)
        if end < 0:
            break
        spans.append(line[start + width : end])
        index = end + width
    return "\n".join(spans)


def contains_name(code: str, name: str) -> bool:
    return re.search(rf"(?<!\w){re.escape(name)}(?!\w)", code) is not None

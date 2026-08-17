"""Printing. Columns that line up, and errors that say what to do next."""

from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence


def table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> str:
    """Left-aligned columns, each as wide as its widest cell.

    Two spaces between columns rather than a box: the output of one of these
    commands is routinely piped into ``grep`` and ``awk``, and a border is
    something both of those then have to be told to ignore.
    """
    body = [[str(cell) for cell in row] for row in rows]
    if not body:
        return "  ".join(headers)
    widths = [len(h) for h in headers]
    for row in body:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))
    lines = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip()]
    for row in body:
        lines.append(
            "  ".join(
                cell.ljust(widths[i]) if i < len(widths) else cell
                for i, cell in enumerate(row)
            ).rstrip()
        )
    return "\n".join(lines)


def fail(message: str) -> None:
    """An error, on stderr, without a traceback.

    A stack trace is the right answer for a bug in this tool and the wrong
    one for a typo'd URL, and almost everything reaching here is the second.
    """
    print(f"mftik: {message}", file=sys.stderr)

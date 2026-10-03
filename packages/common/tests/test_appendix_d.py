"""Appendix D lists the same numbers the code defines (B3-09).

Parsing the plan and importing each symbol happens at collection.
The test itself only compares, so it stays inside the unit call budget.
"""

from __future__ import annotations

import ast
import importlib
import re
from collections.abc import Mapping
from pathlib import Path

import httpx
from mftik.procman import RestartIntensity

_PLAN = (
    Path(__file__).resolve().parents[3] / "docs" / "ARCHITECTURE_CHANGE_PLAN.md"
)
_LOCATION = re.compile(r"[\w./-]+\.py:[A-Za-z_][A-Za-z0-9_]*\Z")


def _cells(line: str) -> list[str]:
    body = line.strip()
    if body.startswith("|"):
        body = body[1:]
    if body.endswith("|"):
        body = body[:-1]
    return [cell.strip() for cell in body.split("|")]


def _strip_code(cell: str) -> str:
    text = cell.strip()
    if len(text) >= 2 and text.startswith("`") and text.endswith("`"):
        return text[1:-1].strip()
    return text


def _appendix(text: str) -> str:
    start = text.index("## 附錄 D")
    rest = text[start + len("## 附錄 D") :]
    nxt = rest.find("\n## ")
    if nxt != -1:
        rest = rest[:nxt]
    return rest


def _module_name(path: str) -> str:
    parts = Path(path).parts
    src = parts.index("src")
    body = list(parts[src + 1 :])
    body[-1] = body[-1].removesuffix(".py")
    return ".".join(body)


def _matches(obj: object, cell: str) -> bool:
    expected = ast.literal_eval(_strip_code(cell))
    if isinstance(obj, httpx.Limits):
        if not isinstance(expected, dict):
            return False
        return (
            obj.max_connections == expected["max_connections"]
            and obj.max_keepalive_connections == expected["max_keepalive_connections"]
            and obj.keepalive_expiry == expected["keepalive_expiry"]
        )
    if isinstance(obj, RestartIntensity):
        if not isinstance(expected, dict):
            return False
        return (
            obj.max_restarts == expected["max_restarts"]
            and obj.window_s == expected["window_s"]
            and obj.min_backoff_s == expected["min_backoff_s"]
        )
    if isinstance(obj, Mapping):
        return dict(obj) == expected
    return obj == expected


def _rows() -> list[tuple[str, str, object, str]]:
    loaded: list[tuple[str, str, object, str]] = []
    for line in _appendix(_PLAN.read_text(encoding="utf-8")).splitlines():
        if not line.startswith("|"):
            continue
        cells = _cells(line)
        if len(cells) != 5:
            continue
        if all(set(cell) <= set("-: ") and cell for cell in cells):
            continue
        location = _strip_code(cells[2])
        if _LOCATION.fullmatch(location) is None:
            continue
        path, symbol = location.rsplit(":", 1)
        module = importlib.import_module(_module_name(path))
        loaded.append((location, symbol, getattr(module, symbol), cells[1]))
    return loaded


_ROWS = _rows()


def test_appendix_d_matches_the_code() -> None:
    """Every located row imports to the value the table prints."""
    mismatches = [
        f"{location} table={cell!r} code={obj!r}"
        for location, _symbol, obj, cell in _ROWS
        if not _matches(obj, cell)
    ]
    assert mismatches == []
    symbols = {symbol for _location, symbol, _obj, _cell in _ROWS}
    assert {
        "MUST_DELIVER_CAPACITY",
        "ALL_QUEUE_CAPACITY",
        "MARK_RETENTION",
        "ACCOUNT_HB_TIMEOUT_S",
        "FETCH_RESTART_MAX",
        "BACKOFF_RATIO",
    } <= symbols
    assert len(_ROWS) >= 40

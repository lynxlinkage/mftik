"""Write-time Matcher spec checks. Compile uses the ``regex`` package."""

from __future__ import annotations

from typing import Any

import regex
from mftik_db.models.alert import AlertMatcherKind

PATTERN_MAX = 512
_KINDS = frozenset(k.value for k in AlertMatcherKind)
_EXTRACT_AS = frozenset({"float", "int", "str"})
_EXTRACT_OPS = frozenset({">", ">=", "<", "<=", "==", "!="})


class InvalidMatcherSpec(ValueError):
    """A Matcher body the API will not store."""


def compile_matcher_spec(kind: str, spec: dict[str, Any]) -> None:
    """Refuse a bad kind or pattern at write, before the row exists.

    Search itself is ALT-5. This only proves the pattern compiles and
    the spec has the fields that kind needs.
    """
    if kind not in _KINDS:
        raise InvalidMatcherSpec(
            f"kind must be one of {sorted(_KINDS)}, not {kind!r}"
        )
    if kind == AlertMatcherKind.LEVEL.value:
        levels = spec.get("levels")
        if not isinstance(levels, list) or not levels:
            raise InvalidMatcherSpec("level matcher needs a non-empty levels list")
        if not all(isinstance(level, str) and level for level in levels):
            raise InvalidMatcherSpec("levels must be non-empty strings")
        return

    pattern = spec.get("pattern")
    if not isinstance(pattern, str) or not pattern:
        raise InvalidMatcherSpec("pattern is required")
    if len(pattern) > PATTERN_MAX:
        raise InvalidMatcherSpec(f"pattern longer than {PATTERN_MAX}")
    try:
        regex.compile(pattern)
    except regex.error as exc:
        raise InvalidMatcherSpec(f"invalid pattern: {exc}") from exc

    if kind == AlertMatcherKind.EXTRACT.value:
        group = spec.get("group", 1)
        if not isinstance(group, int) or group < 1:
            raise InvalidMatcherSpec("extract group must be an int >= 1")
        as_ = spec.get("as", "str")
        if as_ not in _EXTRACT_AS:
            raise InvalidMatcherSpec(
                f"extract as must be one of {sorted(_EXTRACT_AS)}"
            )
        op = spec.get("op")
        if op not in _EXTRACT_OPS:
            raise InvalidMatcherSpec(
                f"extract op must be one of {sorted(_EXTRACT_OPS)}"
            )
        if "value" not in spec:
            raise InvalidMatcherSpec("extract value is required")
        value = spec["value"]
        if as_ in {"float", "int"}:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise InvalidMatcherSpec("extract value must be a number")
        elif not isinstance(value, str):
            raise InvalidMatcherSpec("extract value must be a string")

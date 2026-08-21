"""Judge a line against Matcher candidates. Search never runs on the loop."""

from __future__ import annotations

import asyncio
import logging
import operator
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import regex

if TYPE_CHECKING:
    from mftik_api.alert_match import MatcherRec, MatchRuntime

logger = logging.getLogger("mftik_api.alert_eval")

_EXEC = ThreadPoolExecutor(max_workers=1, thread_name_prefix="alert-match")
SEARCH_TIMEOUT = 0.01
IMPLEMENTED_KINDS = frozenset({"level", "regex", "extract"})
_OPS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}
_compiled: dict[str, regex.Pattern[str]] = {}
#: How many times this process submitted a line to the matcher thread.
executor_submissions = 0


@dataclass
class Hit:
    matcher: MatcherRec
    captures: dict[str, Any] = field(default_factory=dict)


def _timeout_disable_after() -> int:
    return max(1, int(os.getenv("ALERT_TIMEOUT_DISABLE_AFTER", "3")))


def compiled(pattern: str) -> regex.Pattern[str]:
    cached = _compiled.get(pattern)
    if cached is None:
        cached = regex.compile(pattern)
        _compiled[pattern] = cached
    return cached


def warm_patterns(matchers: list[MatcherRec]) -> None:
    for matcher in matchers:
        if matcher.kind not in {"regex", "extract"}:
            continue
        pattern = matcher.spec.get("pattern")
        if isinstance(pattern, str) and pattern:
            try:
                compiled(pattern)
            except regex.error:
                logger.warning("matcher %s pattern will not compile", matcher.id)


def _canon_level(level: str) -> str:
    name = level.lower()
    return "warn" if name == "warning" else name


def match_level(level: str, spec: dict[str, Any]) -> bool:
    allowed = {_canon_level(str(item)) for item in spec.get("levels") or []}
    return _canon_level(level) in allowed


def _coerce(raw: str, as_: str) -> Any:
    if as_ == "float":
        return float(raw)
    if as_ == "int":
        return int(raw)
    return str(raw)


def _eval_all(
    message: str, candidates: list[MatcherRec]
) -> list[tuple[int, dict[str, Any] | None, bool]]:
    """One call, every regex/extract on this line. Runs on ``_EXEC``."""
    out: list[tuple[int, dict[str, Any] | None, bool]] = []
    for matcher in candidates:
        pattern = matcher.spec.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            out.append((matcher.id, None, False))
            continue
        try:
            found = compiled(pattern).search(
                message, timeout=SEARCH_TIMEOUT, concurrent=True
            )
        except TimeoutError:
            out.append((matcher.id, None, True))
            continue
        except regex.error:
            out.append((matcher.id, None, False))
            continue
        if found is None:
            out.append((matcher.id, None, False))
            continue
        if matcher.kind == "regex":
            out.append((matcher.id, {}, False))
            continue
        captures = _extract_captures(matcher, found)
        out.append((matcher.id, captures, False))
    return out


def _extract_captures(
    matcher: MatcherRec, found: regex.Match[str]
) -> dict[str, Any] | None:
    spec = matcher.spec
    group = int(spec.get("group", 1))
    try:
        raw = found.group(group)
    except IndexError:
        return None
    if raw is None:
        return None
    as_ = str(spec.get("as", "str"))
    try:
        value = _coerce(raw, as_)
    except (TypeError, ValueError):
        return None
    op = _OPS.get(str(spec.get("op")))
    if op is None:
        return None
    try:
        if not op(value, spec["value"]):
            return None
    except TypeError:
        return None
    return {str(group): value}


async def evaluate(
    line: dict[str, Any],
    candidates: list[MatcherRec],
    runtime: MatchRuntime | None = None,
) -> list[Hit]:
    hits: list[Hit] = []
    threaded: list[MatcherRec] = []
    disabled = runtime.disabled if runtime is not None else {}
    for matcher in candidates:
        if matcher.id in disabled:
            continue
        if matcher.kind == "level":
            if match_level(str(line.get("level") or ""), matcher.spec):
                hits.append(Hit(matcher))
            continue
        if matcher.kind in {"regex", "extract"}:
            threaded.append(matcher)
            continue
        logger.info(
            "skipping unknown matcher kind=%s id=%s", matcher.kind, matcher.id
        )
    if not threaded:
        return hits

    global executor_submissions
    executor_submissions += 1
    loop = asyncio.get_running_loop()
    judged = await loop.run_in_executor(_EXEC, _eval_all, line["message"], threaded)
    by_id = {matcher.id: matcher for matcher in threaded}
    limit = _timeout_disable_after()
    for matcher_id, captures, timed_out in judged:
        matcher = by_id[matcher_id]
        if timed_out:
            _note_timeout(runtime, matcher, limit)
            continue
        if runtime is not None:
            runtime.timeout_streak[matcher_id] = 0
        if captures is None:
            continue
        hits.append(Hit(matcher, captures))
    return hits


def _note_timeout(
    runtime: MatchRuntime | None, matcher: MatcherRec, limit: int
) -> None:
    logger.warning("matcher %s timed out kind=%s", matcher.id, matcher.kind)
    if runtime is None:
        return
    streak = runtime.timeout_streak.get(matcher.id, 0) + 1
    runtime.timeout_streak[matcher.id] = streak
    if streak >= limit and matcher.id not in runtime.disabled:
        runtime.disabled[matcher.id] = "timeout"
        logger.warning(
            "matcher %s disabled after %d timeouts", matcher.id, streak
        )

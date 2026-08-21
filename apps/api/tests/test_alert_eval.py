"""level / regex / extract. Search is off the event loop."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from mftik_api import alert_eval
from mftik_api.alert_eval import Hit, evaluate, match_level
from mftik_api.alert_match import MatcherRec, MatchRuntime

SIGNAL = '"risk value = {%f}", 0.995'
EXTRACT = {
    "pattern": r'risk value = \{\%f\}", ([\d.]+)',
    "group": 1,
    "as": "float",
    "op": ">",
    "value": 0.99,
}


def _m(kind: str, spec: dict, matcher_id: int = 1) -> MatcherRec:
    return MatcherRec(id=matcher_id, name=kind, kind=kind, spec=spec)


def test_level_is_case_insensitive_membership() -> None:
    spec = {"levels": ["warn", "error"]}
    assert match_level("WARN", spec)
    assert match_level("error", spec)
    assert not match_level("info", spec)


async def test_regex_matches_the_signal_sample() -> None:
    hits = await evaluate(
        {"level": "info", "message": SIGNAL},
        [_m("regex", {"pattern": "risk value"})],
        MatchRuntime(),
    )
    assert len(hits) == 1


async def test_extract_compares_the_capture() -> None:
    runtime = MatchRuntime()
    matcher = _m("extract", EXTRACT)
    high = await evaluate(
        {"level": "info", "message": SIGNAL}, [matcher], runtime
    )
    low = await evaluate(
        {"level": "info", "message": '"risk value = {%f}", 0.50'},
        [matcher],
        runtime,
    )
    assert len(high) == 1
    assert high[0].captures == {"1": 0.995}
    assert low == []


async def test_extract_non_numeric_is_not_a_match() -> None:
    matcher = _m(
        "extract",
        {**EXTRACT, "pattern": r'risk value = \{\%f\}", (\w+)'},
    )
    hits = await evaluate(
        {"level": "info", "message": '"risk value = {%f}", notanumber'},
        [matcher],
        MatchRuntime(),
    )
    assert hits == []


async def test_three_regex_matchers_dispatch_once() -> None:
    before = alert_eval.executor_submissions
    matchers = [
        _m("regex", {"pattern": "risk"}, matcher_id=i) for i in (1, 2, 3)
    ]
    hits = await evaluate(
        {"level": "info", "message": SIGNAL}, matchers, MatchRuntime()
    )
    assert len(hits) == 3
    assert alert_eval.executor_submissions == before + 1


async def test_search_does_not_block_the_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(alert_eval, "SEARCH_TIMEOUT", 0.3)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    task = asyncio.create_task(ticker())
    await evaluate(
        {"level": "info", "message": "a" * 28 + "x"},
        [_m("regex", {"pattern": r"(a+)+$"})],
        MatchRuntime(),
    )
    task.cancel()
    assert ticks > 1


async def test_timeout_disables_and_graph_poll_re_enables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ALERT_TIMEOUT_DISABLE_AFTER", "1")

    def timed_out(message: str, candidates):  # noqa: ANN001
        return [
            (c.id, None, True) if c.id == 7 else (c.id, {}, False)
            for c in candidates
        ]

    monkeypatch.setattr(alert_eval, "_eval_all", timed_out)
    runtime = MatchRuntime()
    bad = _m("regex", {"pattern": "x"}, matcher_id=7)
    good = _m("regex", {"pattern": "hello"}, matcher_id=8)
    line = {"level": "info", "message": "hello"}
    first = await evaluate(line, [bad, good], runtime)
    assert [h.matcher.id for h in first] == [8]
    assert runtime.disabled[7] == "timeout"
    second = await evaluate(line, [bad, good], runtime)
    assert [h.matcher.id for h in second] == [8]
    runtime.clear_disabled()
    # The next line still evaluates the other Matcher; after a poll, both do.
    third = await evaluate(
        {"level": "info", "message": "hello"}, [good], runtime
    )
    assert len(third) == 1


def test_module_does_not_use_re_or_to_thread() -> None:
    src = Path(alert_eval.__file__).read_text()
    assert "import re\n" not in src
    assert "from re " not in src
    assert "to_thread" not in src
    assert "eval(spec" not in src
    assert " eval(" not in src


def test_hit_type_is_exported() -> None:
    assert Hit is alert_eval.Hit

"""A subscription pattern has to mean the same thing to both subscribers.

`log.*` matched every log channel for as long as the broker was Redis, which
globs the whole channel name and lets one `*` swallow the separators with it.
A subscriber that matches per segment reads the same string as a two-segment
subject and delivers nothing — so under NATS the log persister and the alert
matcher would have gone quiet with every publisher still publishing, nothing
raised, and nothing in any log to say the pattern had stopped matching.

`log.*.*` means "three segments, last two anything" to both. That is the only
form worth writing, and the rule that produces it is mechanical: one `*` per
segment, never a bare `*` doing the work of a separator too.

Checked against every `*_pattern` on `Topics` rather than the two in use, so
the next one is covered before it is subscribed to. The patterns themselves
live on `Topics` for the same reason — see the note above `log_pattern`.
"""

from __future__ import annotations

import pytest
from mftik.protocol import Topics

PATTERNS = sorted(
    name
    for name in vars(Topics)
    if name.endswith("_pattern") and callable(getattr(Topics, name))
)


def test_there_are_patterns_to_check() -> None:
    """Renaming the convention must not turn this file into a no-op."""
    assert PATTERNS, "no *_pattern helpers found on Topics"


@pytest.mark.parametrize("name", PATTERNS)
def test_a_wildcard_is_a_whole_segment(name: str) -> None:
    pattern = getattr(Topics, name)()
    offenders = [
        segment
        for segment in pattern.split(".")
        if "*" in segment and segment != "*"
    ]

    assert not offenders, (
        f"Topics.{name}() is {pattern!r}: {offenders} mix a wildcard with "
        "literal text, so what it matches depends on whether the subscriber "
        "globs the whole name or matches segment by segment"
    )


@pytest.mark.parametrize("name", PATTERNS)
def test_a_pattern_is_not_open_ended(name: str) -> None:
    """A trailing `*` that has to match a variable number of segments.

    There is no string that says "two segments or more" to both readings —
    Redis has no `>` and NATS' `*` will not span one — so a pattern that
    needs it is a design that has to change rather than a string to fix.
    """
    pattern = getattr(Topics, name)()

    assert ">" not in pattern, (
        f"Topics.{name}() is {pattern!r}: `>` is a NATS wildcard and a "
        "literal character to Redis"
    )

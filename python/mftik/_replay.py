"""``mftik.replay``: run a strategy again over its event log (E07-D020–E07-D024)."""

from __future__ import annotations

import os

from mftik._mftik import ReplayReport, _run_replay
from mftik.strategy import Strategy

__all__ = ["replay"]

_MAX_INCARNATION = 2**32 - 1


def replay(
    strategy_cls: type[Strategy],
    dir: str | os.PathLike[str],
    *,
    incarnation: int | None = None,
    volume_dir: str | os.PathLike[str] | None = None,
    input_dir: str | os.PathLike[str] | None = None,
) -> ReplayReport:
    """Run ``strategy_cls()`` again over one incarnation of the log in ``dir``.

    ``dir`` is the session's ``eventlog`` directory, under its volume.
    ``incarnation`` defaults to the newest one there. ``volume_dir`` and
    ``input_dir`` are what ``Context.volume_dir`` and ``Context.input_dir``
    use during the replay; unset, they are empty. Differences are on the
    report. ``mftik.ReplayError`` means the log could not be replayed at all.
    """
    if not isinstance(strategy_cls, type) or not issubclass(strategy_cls, Strategy):
        raise TypeError("strategy_cls must be a subclass of mftik.Strategy")
    path = _path("dir", dir)
    number = _incarnation(incarnation)
    volume = "" if volume_dir is None else _path("volume_dir", volume_dir)
    inputs = "" if input_dir is None else _path("input_dir", input_dir)
    strategy = strategy_cls()
    if not isinstance(strategy, Strategy):
        raise TypeError("strategy_cls() did not return a mftik.Strategy")
    return _run_replay(strategy, path, number, volume, inputs)


def _path(name: str, value: object) -> str:
    if not isinstance(value, str | os.PathLike):
        raise TypeError(f"{name} must be str or os.PathLike[str]")
    path = os.fspath(value)
    if not isinstance(path, str):
        raise TypeError(f"{name} must be str or os.PathLike[str]")
    return path


def _incarnation(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("incarnation must be int or None")
    if not 1 <= value <= _MAX_INCARNATION:
        raise ValueError(f"incarnation must be between 1 and {_MAX_INCARNATION}")
    return value

"""Exit codes shared by the session worker and the STS controller.

The shim's ``<id>.exit.json`` is the only crash fact that survives a
controller restart, so the cause rides on the exit code (B5-06). The
worker chooses the code. The controller maps it. Neither side imports
the other: this module is the shared vocabulary.

``HOOK_BLOCKED`` is reserved for B5-04. Nothing in the worker produces
it yet.
"""

from __future__ import annotations

import signal

#: The process finished. A strategy that returned, or a stop whose
#: ``on_stop`` completed. No cleanup.
CLEAN = 0

#: A strategy hook raised after ``on_ready`` returned, and ``on_stop``
#: was attempted. Class A.
STRATEGY_EXCEPTION = 75

#: A general hook held the strategy loop past the hard limit. Class B.
#: Defined so the controller already understands the code. B5-04 is what
#: makes the worker exit with it.
HOOK_BLOCKED = 76

_STOP_SIGNALS = frozenset({signal.SIGKILL, signal.SIGTERM})


def cause_for_exit(
    *,
    exit_code: int | None,
    signal_no: int | None,
    stopped_by_controller: bool,
) -> str | None:
    """The :class:`~mftik_sts.controller.CrashCause` value, or ``None``.

    ``None`` is a clean exit (code 0 and no signal). Cleanup does not run.

    A ``SIGKILL`` or ``SIGTERM`` this controller's stop produced, or any
    other non-zero code on that stop, is ``stop_stuck`` (class B). Any
    other signal, a missing code, or any other non-zero code is
    ``process_death`` (class C). ``strategy_exception`` and the reserved
    ``hook_blocked`` are the two codes that name themselves.
    """
    if exit_code == CLEAN and signal_no is None:
        return None
    if stopped_by_controller and (
        signal_no in _STOP_SIGNALS or (exit_code is not None and exit_code != CLEAN)
    ):
        return "stop_stuck"
    if signal_no is None and exit_code == STRATEGY_EXCEPTION:
        return "strategy_exception"
    if signal_no is None and exit_code == HOOK_BLOCKED:
        return "hook_blocked"
    return "process_death"

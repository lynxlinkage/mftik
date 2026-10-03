"""Numbers for the MD fetch worker's supervision (Appendix D).

The restart curve is the current stand-in. F42 replaces it; B3-08
(#365) owns that change. This module does not add ``max_backoff_s``,
jitter, ``stable_s`` or ``alert_after``.
"""

from __future__ import annotations

#: Cold start of the fetch worker (import, NATS, first ready beat). No
#: earlier fetch-specific timeout exists. Sized so the roll test's spawn
#: finishes with margin under the 10s integration cap.
#: Default; adjust from measurement (Appendix D).
FETCH_START_TIMEOUT_S = 8.0

#: A ready worker whose heartbeat counter is quiet this long is CRASHED.
#: The worker beats several times inside the window
#: (:data:`FETCH_HEARTBEAT_PERIOD_S`). This is the fetch worker, not the
#: MD connection heartbeat F42 changes (B8-02, #239).
#: Default; adjust from measurement (Appendix D).
FETCH_HB_TIMEOUT_S = 2.0

#: SIGTERM, then SIGKILL. The shim's measured graceful stop is under a
#: second (§4.2); this leaves room for an in-flight read to finish.
FETCH_STOP_GRACE_S = 2.0

#: How often the fetch worker writes a status-pipe snapshot. Well under
#: :data:`FETCH_HB_TIMEOUT_S`, so a live loop is not declared dead.
#: Default; adjust from measurement (Appendix D).
FETCH_HEARTBEAT_PERIOD_S = 0.25

#: Restarts allowed inside :data:`FETCH_RESTART_WINDOW_S`. Current value.
#: F42 changes this curve to no FATAL (``max_restarts`` unset). B3-08
#: (#365) owns that change.
FETCH_RESTART_MAX = 5

#: Window for :data:`FETCH_RESTART_MAX`. Current value. F42's stable
#: window is 600 seconds of RUNNING before the attempt count resets.
#: B3-08 (#365) owns that change.
FETCH_RESTART_WINDOW_S = 600.0

#: Floor of the backoff curve. Current value, and the same 1 second F42
#: starts from. The 60 second cap and ±20% jitter are B3-08 (#365).
FETCH_MIN_BACKOFF_S = 1.0

#: How often the MD process looks at the fetch slot. Death is noticed
#: within one period after the supervisor has classified it. Not a
#: heartbeat and not a wire interval.
#: Default; adjust from measurement (Appendix D).
FETCH_RECONCILE_PERIOD_S = 0.2

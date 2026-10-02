"""Provisional numbers for the MD fetch worker's supervision.

The plan describes MD restart as exponential backoff plus an intensity
(§4.3) and does not give the values. Each constant below is a stand-in.

provisional, pending Yi Te (#286)
"""

from __future__ import annotations

#: provisional, pending Yi Te (#286)
#: Cold start of the fetch worker (import, NATS, first ready beat). No
#: earlier fetch-specific timeout exists. Sized so the roll test's spawn
#: finishes with margin under the 10s integration cap.
FETCH_START_TIMEOUT_S = 8.0

#: provisional, pending Yi Te (#286)
#: A ready worker whose heartbeat counter is quiet this long is CRASHED.
#: The worker beats several times inside the window
#: (:data:`FETCH_HEARTBEAT_PERIOD_S`).
FETCH_HB_TIMEOUT_S = 2.0

#: provisional, pending Yi Te (#286)
#: SIGTERM, then SIGKILL. The shim's measured graceful stop is under a
#: second (§4.2); this leaves room for an in-flight read to finish.
FETCH_STOP_GRACE_S = 2.0

#: provisional, pending Yi Te (#286)
#: How often the fetch worker writes a status-pipe snapshot. Well under
#: :data:`FETCH_HB_TIMEOUT_S`, so a live loop is not declared dead.
FETCH_HEARTBEAT_PERIOD_S = 0.25

#: provisional, pending Yi Te (#286)
#: Restarts allowed inside :data:`FETCH_RESTART_WINDOW_S`. The only
#: intensity in the plan is STS's F11 figure (5); MD has none of its own.
FETCH_RESTART_MAX = 5

#: provisional, pending Yi Te (#286)
#: Window for :data:`FETCH_RESTART_MAX`. Same stand-in as F11's 600s.
FETCH_RESTART_WINDOW_S = 600.0

#: provisional, pending Yi Te (#286)
#: Floor of the backoff curve. F11's STS floor, used because §4.3 does
#: not give MD one. Attempt 1 waits this long.
FETCH_MIN_BACKOFF_S = 1.0

#: provisional, pending Yi Te (#286)
#: How often the MD process looks at the fetch slot. Death is noticed
#: within one period after the supervisor has classified it. Not a
#: heartbeat and not a wire interval.
FETCH_RECONCILE_PERIOD_S = 0.2

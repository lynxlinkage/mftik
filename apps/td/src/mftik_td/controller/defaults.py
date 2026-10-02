"""Provisional timings for a TD account worker.

The plan names a heartbeat and a restart bound and does not give the
numbers (issue #286). Each value lives once, here. Nothing in this
module reads the environment.
"""

from __future__ import annotations

# provisional, pending Yi Te (#286)
ACCOUNT_START_TIMEOUT_S = 8.0
# provisional, pending Yi Te (#286)
ACCOUNT_HB_TIMEOUT_S = 3.0
# provisional, pending Yi Te (#286)
ACCOUNT_STOP_GRACE_S = 2.0
# provisional, pending Yi Te (#286)
#: How often the TD process reconciles. The plan does not name a period.
#: A refused spawn is retried on the next pass, so the loop needs one.
ACCOUNT_RECONCILE_PERIOD_S = 5.0
# provisional, pending Yi Te (#286)
ACCOUNT_MAX_RESTARTS = 5
# provisional, pending Yi Te (#286)
ACCOUNT_RESTART_WINDOW_S = 60.0
# provisional, pending Yi Te (#286)
ACCOUNT_MIN_BACKOFF_S = 1.0

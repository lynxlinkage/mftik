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
# provisional, pending Yi Te (#286)
#: How long a drain-replace waits for calls already inside the order
#: handler. The same bound as ``WAIT_TIMEOUT_S`` on ``cancel_session``.
#: A shorter wait would stop the worker under an order that is still
#: being booked.
DRAIN_TIMEOUT_S = 30.0
# provisional, pending Yi Te (#286)
#: How long a quiesced worker waits for the process to be stopped.
#:
#: The controller's reply-to-``STOP`` gap is the stop grace (2 s) plus
#: the time to deliver the reply. This sits well above that. If nothing
#: stops the process, the worker resumes so resting orders can still
#: be cancelled. A lost drain reply and a controller restart both land
#: here: nobody else clears the quiesce.
QUIESCE_LEASE_S = 10.0

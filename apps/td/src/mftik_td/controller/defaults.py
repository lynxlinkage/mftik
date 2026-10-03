"""Timings for a TD account worker (Appendix D).

The restart curve and :data:`ACCOUNT_HB_TIMEOUT_S` keep their current
values. F42 changes them. B3-08 (#365) owns the curve. B6-09 (#367)
owns the 10 second heartbeat timeout and switches this plane onto
``INFRA_RESTART``. Each value lives once, here. Nothing in this module
reads the environment.
"""

from __future__ import annotations

#: Default; adjust from measurement (Appendix D).
ACCOUNT_START_TIMEOUT_S = 8.0
#: Current value. F42 changes the TD account heartbeat timeout to 10
#: seconds. B6-09 (#367) owns that change.
ACCOUNT_HB_TIMEOUT_S = 3.0
#: Default; adjust from measurement (Appendix D). The shim's measured
#: graceful stop is under a second (§4.2).
ACCOUNT_STOP_GRACE_S = 2.0
#: How often the TD process reconciles. A refused spawn is retried on
#: the next pass, so the loop needs one.
#: Default; adjust from measurement (Appendix D).
ACCOUNT_RECONCILE_PERIOD_S = 5.0
#: Current value. F42 changes this curve to no FATAL. B3-08 (#365) owns
#: that change. B6-09 (#367) switches the account worker onto
#: ``INFRA_RESTART``.
ACCOUNT_MAX_RESTARTS = 5
#: Current value, 60 seconds. F42's stable window is 600 seconds of
#: RUNNING. B3-08 (#365) owns that change.
ACCOUNT_RESTART_WINDOW_S = 60.0
#: Floor of the backoff curve. Current value, and the same 1 second F42
#: starts from. The cap and jitter are B3-08 (#365).
ACCOUNT_MIN_BACKOFF_S = 1.0
#: How long a drain-replace waits for calls already inside the order
#: handler. The same bound as ``WAIT_TIMEOUT_S`` on ``cancel_session``.
#: A shorter wait would stop the worker under an order that is still
#: being booked.
#: Default; adjust from measurement (Appendix D).
DRAIN_TIMEOUT_S = 30.0
#: How long a quiesced worker waits for the process to be stopped.
#:
#: The controller's reply-to-``STOP`` gap is the stop grace (2 s) plus
#: the time to deliver the reply. This sits well above that. If nothing
#: stops the process, the worker resumes so resting orders can still
#: be cancelled. A lost drain reply and a controller restart both land
#: here: nobody else clears the quiesce.
#: Default; adjust from measurement (Appendix D).
QUIESCE_LEASE_S = 10.0

"""Numbers the session worker uses (Appendix D).

Each one is defined once, here. The ack timeout is not: order entry
reuses :data:`mftik.strategy.oms.ORDER_ACK_TIMEOUT_S`.
"""

from __future__ import annotations

#: Shim heartbeat period, in seconds. Well under the controller's
#: ``SESSION_HB_TIMEOUT_S`` (3s), so one delayed beat is not a dead worker.
#: Default; adjust from measurement (Appendix D).
HEARTBEAT_PERIOD_S = 1.0

#: Bound of one ``all`` feed queue (B5-01, #296). ``latest`` and
#: ``kline`` conflate and do not use it. Overflow drops the oldest.
#: Default; adjust from measurement (Appendix D).
ALL_QUEUE_CAPACITY = 1024

#: Bound of the shared must-deliver queue: TD, ``feed_end``, RPC replies
#: and the availability notices (#296). Overflow fails the session
#: instead of dropping. It does not follow :data:`ALL_QUEUE_CAPACITY`:
#: a market-data flood must not be able to fail the session at the same
#: depth that only drops a trade print.
MUST_DELIVER_CAPACITY = 8192

#: What :func:`~mftik_sts.session_worker.process.amain` passes as
#: ``all_capacity``. It follows :data:`ALL_QUEUE_CAPACITY`, not
#: :data:`MUST_DELIVER_CAPACITY`: this is the temporary buffer for one
#: ``all`` feed, and that feed drops its oldest. Must-deliver overflow
#: fails the session and uses its own limit.
#: Default; adjust from measurement (Appendix D).
TEMP_BUFFER_CAPACITY = ALL_QUEUE_CAPACITY

#: Minimum gap between logged drop warnings for one feed. The drop count
#: still moves on every drop, and :meth:`Delivery.warnings` keeps one
#: line per drop up to :data:`WARNING_RETENTION`. The plan says the log
#: is rate-limited and does not name the window.
#: Default; adjust from measurement (Appendix D).
DROP_WARN_INTERVAL_S = 1.0

#: How many disposition marks :class:`Delivery` keeps. A long session
#: marks every event; without a cap that dict grows until the process
#: runs out of memory. Older marks are forgotten. B5-02 persists the
#: event log and can drop a mark once it has been written. This cap is
#: separate from the queue bounds (#360).
#: Default; adjust from measurement (Appendix D).
MARK_RETENTION = 1024

#: How many drop-warning lines :meth:`Delivery.warnings` retains.
#: Oldest first, then forgotten. The drop count is not capped.
#: Default; adjust from measurement (Appendix D).
WARNING_RETENTION = 1024

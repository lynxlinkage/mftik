"""Numbers the session worker uses until Yi Te names them (#286).

Each one is defined once, here. They are provisional. The ack timeout is
not: order entry reuses :data:`mftik.strategy.oms.ORDER_ACK_TIMEOUT_S`.
"""

from __future__ import annotations

#: Shim heartbeat period, in seconds. Well under the controller's
#: ``SESSION_HB_TIMEOUT_S`` (3s), so one delayed beat is not a dead worker.
HEARTBEAT_PERIOD_S = 1.0

#: Bound of one ``all`` feed queue (B5-01, provisional #286). ``latest``
#: and ``kline`` conflate and do not use it.
ALL_QUEUE_CAPACITY = 1024

#: Bound of the shared must-deliver queue: TD, ``feed_end``, RPC replies
#: and the availability notices. Same number as one ``all`` feed, also
#: provisional (#286). Overflow fails the session instead of dropping.
MUST_DELIVER_CAPACITY = ALL_QUEUE_CAPACITY

#: What :class:`~mftik_sts.session_worker.ingress.Ingress` passes as
#: :class:`~mftik_sts.session_worker.delivery.Delivery` ``capacity``.
#: That one argument is both bounds above, which are defined equal.
TEMP_BUFFER_CAPACITY = ALL_QUEUE_CAPACITY

#: Minimum gap between logged drop warnings for one feed. The drop count
#: still moves on every drop, and :meth:`Delivery.warnings` keeps one
#: line per drop. The plan says the log is rate-limited and does not
#: name the window. Provisional (#286).
DROP_WARN_INTERVAL_S = 1.0

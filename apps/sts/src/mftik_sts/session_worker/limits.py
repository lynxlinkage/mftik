"""Numbers B4-03 uses until Yi Te names them (#286).

Each one is defined once, here. They are provisional. The ack timeout is
not: order entry reuses :data:`mftik.strategy.oms.ORDER_ACK_TIMEOUT_S`.
"""

from __future__ import annotations

#: Shim heartbeat period, in seconds. Well under the controller's
#: ``SESSION_HB_TIMEOUT_S`` (3s), so one delayed beat is not a dead worker.
HEARTBEAT_PERIOD_S = 1.0

#: Bound of the temporary ingress buffer the process passes as
#: ``Ingress`` capacity. Overflow drops the oldest market-data event
#: and logs the burst. TD is never dropped; a buffer that is over
#: capacity and holds nothing but TD fails the session. That MD-only
#: policy stands until B5-01, when ``offer`` delegates to :class:`Delivery`.
TEMP_BUFFER_CAPACITY = 1024

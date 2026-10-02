"""The six stages of one session (§5.3).

The number is the order. A later stage is a larger value, and the
ingress walks them once: nothing in this process moves the phase
backwards, and nothing in this process starts a second walk (I2).
"""

from __future__ import annotations

from enum import IntEnum


class Phase(IntEnum):
    """Stages 0 to 6. The names are the plan's rows.

    * ``BOOT`` (0, 啟動) — ingress only. Receive connection, subscribe
      ``sts.ctl.{session_id}``, heartbeat the shim. No strategy thread.
    * ``LOAD`` (1, 載入) — strategy thread exists. Ingress subscribes the
      MD feeds and starts digesting them. The strategy thread opens its
      send connection and imports the strategy.
    * ``ON_START`` (2) — ``on_start`` is running. The ingress keeps
      receiving and does not deliver anything.
    * ``READY`` (3, 就緒) — ``on_start`` has returned. Ingress subscribes
      TD and recon runs. ``on_ready`` runs here. Still no delivery.
    * ``RUNNING`` (4) — ``on_ready`` has returned. Hooks run, and events
      are delivered under the delivery table.
    * ``STOPPING`` (5) — stop has been handed to the strategy. The
      ingress keeps receiving acks and fills until ``on_stop`` returns.
    * ``TEARDOWN`` (6, 收尾) — the strategy thread has finished. The
      ingress writes the last status, flushes the event log, drains
      NATS and exits.
    """

    BOOT = 0
    LOAD = 1
    ON_START = 2
    READY = 3
    RUNNING = 4
    STOPPING = 5
    TEARDOWN = 6

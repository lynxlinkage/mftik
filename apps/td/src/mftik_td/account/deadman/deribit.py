"""Deribit's slot. Cancel-on-disconnect is not used (F37).

Deribit's own COD cancels on every reconnect, which is the opposite of
a dead-man's switch. Deribit cannot place orders yet. This slot makes
no venue call: no COD, no order, no countdown refresh against Deribit.
The class exists so a Deribit ``api_id`` still resolves to a slot.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class DeribitDeadMan(DeadMansSwitch):
    """Deribit's slot. Not COD, and no venue call."""

    venue = "Deribit"

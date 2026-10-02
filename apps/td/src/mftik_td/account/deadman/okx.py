"""OKX countdown cancel (F37).

B6-07 arms this. This module does not call OKX.
"""

from __future__ import annotations

from mftik_td.account.deadman.base import DeadMansSwitch


class OkxDeadMan(DeadMansSwitch):
    """OKX's slot."""

    venue = "Okx"

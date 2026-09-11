"""Global ``cid_slot`` sequence — one increment per new STS session."""

from __future__ import annotations

from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column

from mftik_db.models.base import Base

#: Matches ``mftik.strategy.client_order_id.SLOT_SPACE``. Kept here so the
#: database package does not import the strategy SDK.
SLOT_SPACE = 65536


class CidSlotSeq(Base):
    """A single-row counter. ``nextval % 65536`` is the slot.

    Not a process-local integer: two STS instances create sessions at once,
    and ``owns()`` compares the slot packed into ``client_order_id``. A
    counter that starts at 0 on every process gives two live sessions the
    same slot.
    """

    __tablename__ = "cid_slot_seq"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

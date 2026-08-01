"""Shared API dependencies — broker lifecycle and defaults."""

from __future__ import annotations

import os
from typing import Annotated

from fastapi import Depends, Request
from mft.broker import Broker

DEFAULT_USER_ID = int(os.getenv("MFT_DEFAULT_USER_ID", "1"))


def get_broker(request: Request) -> Broker:
    broker: Broker | None = getattr(request.app.state, "broker", None)
    if broker is None:
        raise RuntimeError("broker not initialized")
    return broker


BrokerDep = Annotated[Broker, Depends(get_broker)]

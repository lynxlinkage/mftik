"""Shared API dependencies — broker lifecycle and defaults."""

from __future__ import annotations

import os
from typing import Annotated

from fastapi import Depends, Request
from mftik.broker import Broker
from mftik.registry import RegistryStore

#: The Owner, for code that has no request to read a principal from.
#:
#: This used to be the identity of every request. It is now the identity of
#: none of them while ``MFTIK_AUTH_ENABLED`` is on — handlers take ``OwnerId``,
#: which resolves the principal the gate built. What is left is the seed
#: default and the stand-in for a handler called outside a request.
DEFAULT_USER_ID = int(os.getenv("MFTIK_DEFAULT_USER_ID", "1"))


def get_broker(request: Request) -> Broker:
    broker: Broker | None = getattr(request.app.state, "broker", None)
    if broker is None:
        raise RuntimeError("broker not initialized")
    return broker


BrokerDep = Annotated[Broker, Depends(get_broker)]


def get_registry_store() -> RegistryStore:
    return RegistryStore.from_env()


RegistryStoreDep = Annotated[RegistryStore, Depends(get_registry_store)]


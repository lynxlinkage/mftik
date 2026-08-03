"""Thin helpers for API → domain request-reply over the broker."""

from __future__ import annotations

from typing import Any

from mft.broker import Broker
from mft.broker.errors import RequestTimeoutError
from mft.protocol import (
    MD_ERROR,
    STS_ERROR,
    TD_ERROR,
    RpcError,
    UntypedEnvelope,
)
from pydantic import BaseModel

# Domains the API orchestrates by default. SYM passes its own set.
_DEFAULT_ERROR_TYPES = frozenset({STS_ERROR, TD_ERROR, MD_ERROR})


class DomainRpcError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


async def request_domain[T: BaseModel](
    broker: Broker,
    subject: str,
    envelope: UntypedEnvelope | Any,
    *,
    result_type: type[T],
    error_types: frozenset[str] | None = None,
    timeout: float = 5.0,
) -> T:
    """Send a control-plane request and parse a typed success payload."""
    errors = error_types or _DEFAULT_ERROR_TYPES
    try:
        reply = await broker.request(subject, envelope, timeout=timeout)
    except RequestTimeoutError as exc:
        raise DomainRpcError("timeout", str(exc)) from exc

    if reply.type in errors:
        err = RpcError.model_validate(reply.payload)
        raise DomainRpcError(err.code, err.message)

    return result_type.model_validate(reply.payload)

"""Sym RPC — list / venues / refresh on ``Topics.SYM``."""

from __future__ import annotations

import logging

from mft.broker import IncomingRequest
from mft.protocol import (
    SYM_ERROR,
    SYM_LIST,
    SYM_REFRESH,
    SYM_VENUES,
    RpcError,
    RpcErrorEnvelope,
    SymListRequest,
    SymListResult,
    SymListResultEnvelope,
    SymRefreshRequest,
    SymRefreshResult,
    SymRefreshResultEnvelope,
    SymVenuesResult,
    SymVenuesResultEnvelope,
)

from mft_sym.plane import SymbolPlane

logger = logging.getLogger(__name__)

SOURCE = "sym"


async def handle_list(req: IncomingRequest, *, plane: SymbolPlane) -> None:
    try:
        payload = SymListRequest.model_validate(req.envelope.payload or {})
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    try:
        symbols = await plane.list_symbols(
            universal_ticker=payload.universal_ticker,
            venue=payload.venue,
            category=payload.category,
            symbol=payload.symbol,
            active_only=payload.active_only,
        )
    except Exception as exc:
        logger.exception("sym.list failed")
        await _error(req, "list_failed", str(exc))
        return
    await req.reply(
        SymListResultEnvelope.wrap(
            SymListResult(symbols=symbols), type=SYM_LIST, source=SOURCE
        )
    )


async def handle_venues(req: IncomingRequest, *, plane: SymbolPlane) -> None:
    try:
        counts = await plane.counts()
    except Exception as exc:
        logger.exception("sym.venues failed")
        await _error(req, "venues_failed", str(exc))
        return
    await req.reply(
        SymVenuesResultEnvelope.wrap(
            SymVenuesResult(venues=plane.venues, counts=counts),
            type=SYM_VENUES,
            source=SOURCE,
        )
    )


async def handle_refresh(req: IncomingRequest, *, plane: SymbolPlane) -> None:
    try:
        payload = SymRefreshRequest.model_validate(req.envelope.payload or {})
    except Exception as exc:
        await _error(req, "invalid_payload", str(exc))
        return
    result = await plane.refresh(payload.venue)
    await req.reply(
        SymRefreshResultEnvelope.wrap(
            SymRefreshResult(**result), type=SYM_REFRESH, source=SOURCE
        )
    )


async def dispatch(req: IncomingRequest, *, plane: SymbolPlane) -> None:
    handler = _HANDLERS.get(req.envelope.type)
    if handler is None:
        await _error(
            req, "unknown_type", f"unsupported sym request {req.envelope.type!r}"
        )
        return
    await handler(req, plane=plane)


async def _error(req: IncomingRequest, code: str, message: str) -> None:
    await req.reply(
        RpcErrorEnvelope.wrap(
            RpcError(code=code, message=message),
            type=SYM_ERROR,
            source=SOURCE,
        )
    )


_HANDLERS = {
    SYM_LIST: handle_list,
    SYM_VENUES: handle_venues,
    SYM_REFRESH: handle_refresh,
}

__all__ = ["dispatch", "handle_list", "handle_refresh", "handle_venues"]

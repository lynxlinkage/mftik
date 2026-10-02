"""Decode one inbound event and call the strategy hook.

The ingress does not do this (I4). Both the session worker and the
older :class:`mftik_sts.session.session.StsSession` shell call these
functions, so a fill wakes ``wait_cids`` the same way in either place.

``swallow`` is the shell's behaviour: a hook that raises is recorded
and the session continues, which is what the event-log tests cover.
The worker passes ``False``. A strategy exception there is class A —
``on_stop``, then a non-zero exit — and the caller does that.
"""

from __future__ import annotations

import logging
from typing import Any

from mftik.exchange.models import (
    AggTrade,
    Balance,
    BestQuote,
    FeedEnd,
    Fill,
    FundingRate,
    Greeks,
    Kline,
    Liquidation,
    OpenInterest,
    Order,
    OrderBook,
    Ticker,
    Trade,
)
from mftik.exchange.oms import Position
from mftik.protocol import (
    MD_AGG_TRADE,
    MD_BEST_QUOTE,
    MD_FEED_END,
    MD_FUNDING_RATE,
    MD_GREEKS,
    MD_KLINE,
    MD_LIQUIDATION,
    MD_OPEN_INTEREST,
    MD_ORDERBOOK,
    MD_TICKER,
    MD_TRADE,
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    TD_POSITION_UPDATE,
    CancelReject,
    OrderReject,
    UntypedEnvelope,
)
from mftik.strategy import Strategy
from mftik.strategy.eventlog import EventLog
from pydantic import BaseModel

logger = logging.getLogger(__name__)

#: MD message type → (strategy hook, payload model).
MD_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    MD_TICKER: ("on_ticker", Ticker),
    MD_ORDERBOOK: ("on_order_book", OrderBook),
    MD_KLINE: ("on_kline", Kline),
    MD_TRADE: ("on_trade", Trade),
    MD_AGG_TRADE: ("on_agg_trade", AggTrade),
    MD_BEST_QUOTE: ("on_best_quote", BestQuote),
    MD_LIQUIDATION: ("on_liquidation", Liquidation),
    MD_FUNDING_RATE: ("on_funding_rate", FundingRate),
    MD_OPEN_INTEREST: ("on_open_interest", OpenInterest),
    MD_GREEKS: ("on_greeks", Greeks),
    MD_FEED_END: ("on_feed_end", FeedEnd),
}

#: TD global message type → (strategy hook, payload model).
TD_GLOBAL_HANDLERS: dict[str, tuple[str, type[BaseModel]]] = {
    TD_ORDER_UPDATE: ("on_order_update", Order),
    TD_FILL: ("on_fill", Fill),
    TD_ORDER_REJECT: ("on_order_reject", OrderReject),
    TD_CANCEL_REJECT: ("on_cancel_reject", CancelReject),
    TD_BALANCE_UPDATE: ("on_balance_update", Balance),
    TD_POSITION_UPDATE: ("on_position_update", Position),
}

#: Fan-out types that can change a watched cid's readiness. After the
#: strategy hook returns, these wake :meth:`StrategyOms.wait_cids`.
_OMS_WAIT_TYPES = frozenset(
    {TD_ORDER_UPDATE, TD_FILL, TD_ORDER_REJECT, TD_CANCEL_REJECT}
)


async def dispatch_md(
    strategy: Strategy,
    event_log: EventLog,
    env: UntypedEnvelope,
    *,
    swallow: bool = True,
) -> None:
    """Hand one market-data envelope to its hook.

    An unknown type is recorded and ignored. A payload that does not
    validate is recorded and ignored. A hook that raises is recorded,
    and re-raised when ``swallow`` is false.
    """
    entry = MD_HANDLERS.get(env.type)
    if entry is None:
        _record_in(event_log, "unhandled", env)
        return
    name, model = entry
    _record_in(event_log, "md", env, hook=name)
    try:
        payload = model.model_validate(env.payload)
    except Exception as exc:
        event_log.record(
            "error",
            "payload_invalid",
            dir="self",
            hook=name,
            type=env.type,
            env_id=env.id,
            error=repr(exc),
        )
        logger.exception("invalid md payload type=%s", env.type)
        return
    try:
        await getattr(strategy, name)(payload)
    except Exception as exc:
        _record_hook_failed(event_log, name, env, exc)
        logger.exception("strategy %s failed type=%s", name, env.type)
        if not swallow:
            raise


async def dispatch_td(
    strategy: Strategy,
    event_log: EventLog,
    api_id: int,
    env: UntypedEnvelope,
    *,
    swallow: bool = True,
) -> bool:
    """Hand one TD global envelope to its hook.

    ``False`` means the type has no hook. The shell logs that on its
    own channel; the worker records it here only when ``swallow`` is
    false, because that path has no second log. Inflight tracking stays
    session-owned: the hook must not be the sole writer, and
    ``wait_cids`` is signalled even when the hook raises.
    """
    entry = TD_GLOBAL_HANDLERS.get(env.type)
    if entry is None:
        if not swallow:
            _record_in(event_log, "unhandled", env, api_id=api_id)
        return False
    name, model = entry
    _record_in(event_log, "td", env, hook=name, api_id=api_id)
    try:
        payload = model.model_validate(env.payload)
    except Exception as exc:
        event_log.record(
            "error",
            "payload_invalid",
            dir="self",
            hook=name,
            type=env.type,
            env_id=env.id,
            api_id=api_id,
            error=repr(exc),
        )
        logger.exception(
            "invalid td global payload api_id=%s type=%s", api_id, env.type
        )
        return True
    if name == "on_order_update":
        strategy.oms.note_order(payload)
    elif name == "on_order_reject":
        strategy.oms.note_reject(
            getattr(payload, "error_code", None),
            getattr(payload, "client_order_id", None),
        )
    elif name == "on_cancel_reject":
        strategy.oms.note_gone(getattr(payload, "client_order_id", None))
    try:
        await getattr(strategy, name)(api_id, payload)
    except Exception as exc:
        _record_hook_failed(event_log, name, env, exc, api_id=api_id)
        logger.exception(
            "strategy %s failed api_id=%s type=%s", name, api_id, env.type
        )
        if not swallow:
            raise
    finally:
        if env.type in _OMS_WAIT_TYPES:
            strategy.oms.signal(
                api_id,
                getattr(payload, "client_order_id", None),
                payload if isinstance(payload, Order) else None,
            )
    return True


def _record_in(
    event_log: EventLog, kind: str, env: UntypedEnvelope, **fields: Any
) -> None:
    event_log.record(
        kind,
        env.type,
        env_id=env.id,
        sent_ts=env.ts,
        source=env.source,
        payload=env.payload,
        **fields,
    )


def _record_hook_failed(
    event_log: EventLog,
    hook: str,
    env: UntypedEnvelope,
    exc: BaseException,
    **fields: Any,
) -> None:
    event_log.record(
        "error",
        "hook_failed",
        dir="self",
        hook=hook,
        type=env.type,
        env_id=env.id,
        error=repr(exc),
        **fields,
    )

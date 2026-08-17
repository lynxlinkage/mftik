"""Identity of one market-data message, for overlapping MD publishers.

Two MD processes that subscribed to the same feed will both see the venue's
print and both call :meth:`Dispatcher.publish`. The second must not reach
STS or the tape. The key is always ``topic.UniversalTicker`` plus an id
taken from the payload — a bare venue symbol would collide across markets.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import Topics

#: How long a seen id is remembered. Sized for an update overlap, not for
#: forever: a print that legitimately repeats after a minute is a new event.
SEEN_TTL_S = 60

KLINE_PREFIX = "kline_"


def event_id(topic: str, payload: Mapping[str, Any]) -> str | None:
    """The part of ``payload`` that makes this message itself.

    ``None`` means we cannot tell copies from two distinct events, so the
    caller must publish rather than guess.
    """
    if topic in ("trade", "aggtrade"):
        tid = payload.get("trade_id")
        return str(tid) if tid not in (None, "") else None
    if topic == "liquidation":
        for key in ("trade_id", "id"):
            value = payload.get(key)
            if value not in (None, ""):
                return str(value)
        side = payload.get("side", "")
        price = payload.get("price", "")
        qty = payload.get("qty", "")
        ts = payload.get("ts", "")
        if not (side or price or qty):
            return None
        return f"{side}|{price}|{qty}|{ts}"
    if topic.startswith(KLINE_PREFIX):
        interval = payload.get("interval") or topic[len(KLINE_PREFIX) :]
        open_time = payload.get("open_time", "")
        closed = bool(payload.get("closed", False))
        if open_time in (None, ""):
            return None
        return f"{interval}|{open_time}|{int(closed)}"
    if topic == "orderbook":
        for key in ("last_update_id", "u", "update_id"):
            value = payload.get(key)
            if value not in (None, "", 0):
                return str(value)
        return _book_fingerprint(payload)
    if topic in ("ticker", "bestquote"):
        bid = payload.get("bid", "")
        ask = payload.get("ask", "")
        if bid in (None, "") and ask in (None, ""):
            return None
        bid_qty = payload.get("bid_qty", "")
        ask_qty = payload.get("ask_qty", "")
        return f"{bid}|{ask}|{bid_qty}|{ask_qty}"
    return None


def seen_key(key_prefix: str, topic: str, ticker: UniversalTicker, ident: str) -> str:
    """``{prefix}:md:seen:{topic}.{UniversalTicker}:{id}``."""
    feed = Topics.md_feed(topic, ticker)
    return f"{key_prefix}:md:seen:{feed}:{ident}"


def _book_fingerprint(payload: Mapping[str, Any]) -> str | None:
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    top_bid = _level_fp(bids[0] if bids else None)
    top_ask = _level_fp(asks[0] if asks else None)
    if not top_bid and not top_ask:
        return None
    return f"{top_bid}|{top_ask}"


def _level_fp(level: Any) -> str:
    if level is None:
        return ""
    if isinstance(level, Mapping):
        return f"{level.get('price', '')}|{level.get('qty', '')}"
    return str(level)

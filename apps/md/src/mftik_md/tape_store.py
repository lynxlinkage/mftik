"""Regional Redis tape — the prints one MD recorded, and what they cover.

This is the only ``src`` module allowed to import ``redis``. The broker is
the IPC layer; the tape is regional disk sitting next to the process that
pumps the feed. STS never opens it. A warm-up crosses one core RTT to the
MD that holds the feed, and that MD reads this store.

Standalone Redis, AOF plus a volume, one process per region. Not a cluster
and not a second ``BrokerTransport``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

#: How many measured gaps one feed's coverage will carry before the tape
#: stops being described as one series. Same bound the broker used to keep.
TAPE_MAX_GAPS = 32

#: Reserved stream field for a stamp that could not be the entry id
#: (clock went backwards, or the same millisecond overflowed). Stripped
#: on read so a caller never sees it.
_RECORDED_MS_FIELD = "_recorded_ms"


def encode_tape_gaps(gaps: list[tuple[int, int]]) -> str:
    """Render measured gaps as ``start-end`` pairs, oldest first."""
    return ",".join(f"{start}-{end}" for start, end in gaps)


def decode_tape_gaps(raw: str | None) -> list[tuple[int, int]]:
    """Parse :func:`encode_tape_gaps`. Unreadable entries are skipped."""
    if not raw:
        return []
    gaps: list[tuple[int, int]] = []
    for chunk in raw.split(","):
        head, _, tail = chunk.partition("-")
        try:
            gaps.append((int(head), int(tail)))
        except ValueError:
            logger.warning("tape coverage has an unreadable gap: %r", chunk)
    return gaps


def _now_ms() -> int:
    return int(time.time() * 1000)


def _int_or_none(raw: str | None) -> int | None:
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _stream_key(feed: str) -> str:
    return f"tape:{feed}"


def _coverage_key(feed: str) -> str:
    return f"tapecov:{feed}"


def _entry_ms(entry_id: str, fields: Mapping[str, str]) -> int:
    stamped = fields.get(_RECORDED_MS_FIELD)
    if stamped:
        parsed = _int_or_none(stamped)
        if parsed is not None:
            return parsed
    head, _, _seq = entry_id.partition("-")
    return int(head) if head.isdigit() else 0


def _public_fields(fields: Mapping[str, str]) -> dict[str, str]:
    return {k: str(v) for k, v in fields.items() if k != _RECORDED_MS_FIELD}


class TapeStore:
    """One region's tape: a Redis stream per feed and a coverage hash."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    @classmethod
    def from_url(cls, url: str) -> TapeStore:
        return cls(Redis.from_url(url, decode_responses=True))

    async def ping(self) -> None:
        await self._redis.ping()

    async def aclose(self) -> None:
        await self._redis.aclose()

    async def append(
        self,
        feed: str,
        fields: Mapping[str, str],
        *,
        maxlen: int,
        ttl_seconds: int,
        recorded_ms: int | None = None,
    ) -> None:
        """Append one print. ``recorded_ms`` is the recorder's clock."""
        ms = _now_ms() if recorded_ms is None else recorded_ms
        key = _stream_key(feed)
        payload = {str(k): str(v) for k, v in fields.items()}
        entry_id = await self._next_id(key, ms)
        if not entry_id.startswith(f"{ms}-"):
            payload[_RECORDED_MS_FIELD] = str(ms)
        await self._redis.xadd(
            key,
            payload,
            id=entry_id,
            maxlen=max(1, maxlen),
            approximate=False,
        )
        await self._redis.expire(key, max(1, ttl_seconds))

    async def tail(
        self, feed: str, *, count: int
    ) -> list[tuple[int, dict[str, str]]]:
        """Newest ``count`` records, oldest → newest, as ``(ms, fields)``."""
        if count <= 0:
            return []
        rows = await self._redis.xrevrange(_stream_key(feed), count=count)
        out: list[tuple[int, dict[str, str]]] = []
        for entry_id, fields in reversed(rows):
            out.append((_entry_ms(str(entry_id), fields), _public_fields(fields)))
        return out

    async def tail_page(
        self,
        feed: str,
        *,
        limit: int,
        before: str | None = None,
        chunk: int,
    ) -> tuple[list[tuple[str, int, dict[str, str]]], bool]:
        """One newest-first page inside the last ``limit`` records.

        Each item is ``(stream_id, recorded_ms, fields)``. ``before`` is
        an exclusive upper bound (the previous page's oldest id). The
        boolean is whether an older page may still exist inside ``limit``.
        """
        if limit <= 0 or chunk <= 0:
            return [], False
        max_id = "+" if not before else f"({before}"
        rows = await self._redis.xrevrange(
            _stream_key(feed), max=max_id, min="-", count=min(chunk, limit)
        )
        more = len(rows) == min(chunk, limit)
        page = [
            (str(entry_id), _entry_ms(str(entry_id), fields), _public_fields(fields))
            for entry_id, fields in reversed(rows)
        ]
        return page, more and bool(rows)

    async def trim_before(self, feed: str, *, min_id_ms: int) -> int:
        """Drop records stamped before ``min_id_ms``. How many went."""
        key = _stream_key(feed)
        before = await self._redis.xlen(key)
        if not before:
            return 0
        await self._redis.xtrim(key, minid=f"{min_id_ms}-0", approximate=False)
        after = await self._redis.xlen(key)
        return max(0, int(before) - int(after))

    async def coverage(self, feed: str) -> dict[str, str]:
        raw: dict[Any, Any] = await self._redis.hgetall(_coverage_key(feed))
        return {str(k): str(v) for k, v in raw.items()}

    async def mark_recording(
        self, feed: str, *, since_ms: int, ttl_seconds: int
    ) -> None:
        """Stamp that ``feed`` began recording at ``since_ms``.

        A clean stop leaves ``stopped_ms``; the hole is then measured and
        continuity is kept. No stamp is SIGKILL / OOM: the hole has no
        length and continuity restarts here.
        """
        prior = await self.coverage(feed)
        stopped_ms = _int_or_none(prior.get("stopped_ms"))
        prior_since = _int_or_none(prior.get("continuous_since_ms"))
        gaps = decode_tape_gaps(prior.get("gaps"))

        measured = (
            stopped_ms is not None
            and prior_since is not None
            and stopped_ms <= since_ms
        )
        if measured:
            assert prior_since is not None and stopped_ms is not None
            gaps = [*gaps, (stopped_ms, since_ms)]
            continuous_since = prior_since
            if len(gaps) > TAPE_MAX_GAPS:
                gaps = []
                continuous_since = since_ms
        else:
            gaps = []
            continuous_since = since_ms

        await self._put_coverage(
            feed,
            {
                "continuous_since_ms": str(continuous_since),
                "recording": "1",
                "stopped_ms": "",
                "gaps": encode_tape_gaps(gaps),
            },
            ttl_seconds=ttl_seconds,
        )

    async def mark_stopped(self, feed: str, *, at_ms: int, ttl_seconds: int) -> None:
        """Stamp that ``feed`` stopped recording at ``at_ms``."""
        prior = await self.coverage(feed)
        prior["recording"] = "0"
        prior["stopped_ms"] = str(at_ms)
        await self._put_coverage(feed, prior, ttl_seconds=ttl_seconds)

    async def _put_coverage(
        self, feed: str, fields: Mapping[str, str], *, ttl_seconds: int
    ) -> None:
        key = _coverage_key(feed)
        if not fields:
            return
        await self._redis.hset(key, mapping=dict(fields))
        await self._redis.expire(key, max(1, ttl_seconds))

    async def _next_id(self, key: str, ms: int) -> str:
        """A stream id at ``ms``, or just after the newest id if ``ms`` is old."""
        newest = await self._redis.xrevrange(key, count=1)
        if not newest:
            return f"{ms}-0"
        last_id = str(newest[0][0])
        last_ms_s, _, last_seq_s = last_id.partition("-")
        try:
            last_ms = int(last_ms_s)
            last_seq = int(last_seq_s or "0")
        except ValueError:
            return f"{ms}-0"
        if ms > last_ms:
            return f"{ms}-0"
        if ms == last_ms:
            return f"{ms}-{last_seq + 1}"
        # Clock went backwards. Redis refuses an id below the newest, so
        # we take the next sequence on the last millisecond and keep the
        # real stamp in :data:`_RECORDED_MS_FIELD`.
        return f"{last_ms}-{last_seq + 1}"

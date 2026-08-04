"""Per-session liveness keys — how a dead STS process gets noticed.

A session row is closed by the process that owns it. That covers every
ending the process gets to observe, including a shutdown cut short. It does
not cover the process disappearing outright — SIGKILL, OOM, the machine
going away — where nothing runs and the row stays ``live`` forever with no
owner left to correct it: a session the UI shows as running that nobody can
stop.

So every live session holds a short-lived Redis key, refreshed on its
heartbeat. The key is what lets any STS process tell "another process owns
this session" from "nobody does". That distinction cannot be made from
process-local state: several STS processes serve the same RPC subject as
competing consumers, so a process finding a live row it does not own has no
way of knowing, by itself, whether a peer is running it.
"""

from __future__ import annotations

from typing import Any

#: How long a key outlives its last refresh. Generous next to the default
#: 1s heartbeat: the cost of waiting is a stale row lingering a little
#: longer, while the cost of expiring early is declaring a healthy session
#: dead and marking a running strategy failed.
ALIVE_TTL_SECONDS = 30


def alive_key(key_prefix: str, session_id: str) -> str:
    return f"{key_prefix}:sts:alive:{session_id}"


async def mark_alive(
    broker: Any, session_id: str, *, ttl: int = ALIVE_TTL_SECONDS
) -> None:
    """Claim / renew ownership of ``session_id``."""
    await broker.redis.set(
        alive_key(broker.config.key_prefix, session_id), "1", ex=ttl
    )


async def clear_alive(broker: Any, session_id: str) -> None:
    """Release ownership. Safe to call for a session that never claimed one."""
    await broker.redis.delete(alive_key(broker.config.key_prefix, session_id))


async def is_alive(broker: Any, session_id: str) -> bool:
    return bool(
        await broker.redis.exists(
            alive_key(broker.config.key_prefix, session_id)
        )
    )

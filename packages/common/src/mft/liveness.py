"""Per-session liveness keys — how a process that died is noticed.

A session row is closed by the process that owns it. That covers every
ending the process gets to observe, including a shutdown cut short. It does
not cover the process disappearing outright — SIGKILL, OOM, the machine
going away — where nothing runs and the row stays ``live`` forever with no
owner left to correct it: a session the UI shows as running that nobody can
stop.

So every live session holds a short-lived Redis key, refreshed while its
owner is running. The key is what lets a process tell "a peer owns this
session" from "nobody does". That distinction cannot be made from
process-local state: several processes of a plane serve the same RPC
subject as competing consumers, so one finding a live row it does not own
has no way of knowing, by itself, whether a peer is running it.

Shared rather than STS-local because the question outlives STS: td and md
rows are keyed by the STS session they were attached for, and they face the
same live-row-with-no-owner problem. What they must not do is answer it by
reading STS's key. A session id is one thing; *which plane still holds it*
is another, and an md attach can die while the strategy it served keeps
running. Hence ``domain``: one key per (plane, session), each written and
read by the plane that owns the row it guards.
"""

from __future__ import annotations

from typing import Any

#: How long a key outlives its last refresh. Generous next to the default
#: 1s heartbeat: the cost of waiting is a stale row lingering a little
#: longer, while the cost of expiring early is declaring a healthy session
#: dead and marking a running strategy failed.
ALIVE_TTL_SECONDS = 30


def alive_key(key_prefix: str, session_id: str, *, domain: str) -> str:
    return f"{key_prefix}:{domain}:alive:{session_id}"


async def mark_alive(
    broker: Any,
    session_id: str,
    *,
    domain: str,
    ttl: int = ALIVE_TTL_SECONDS,
) -> None:
    """Claim / renew this plane's ownership of ``session_id``."""
    await broker.redis.set(
        alive_key(broker.config.key_prefix, session_id, domain=domain),
        "1",
        ex=ttl,
    )


async def claim_alive(
    broker: Any,
    session_id: str,
    *,
    domain: str,
    ttl: int = ALIVE_TTL_SECONDS,
) -> bool:
    """Take ownership of ``session_id`` only if nobody else holds it.

    ``SET NX`` rather than :func:`mark_alive`'s plain ``SET``, because this
    asks a question instead of stating a fact: several STS processes rebuild
    on boot at once, and without the atomic test they would each restore the
    same session and run two of it.

    Returns whether the claim was taken.
    """
    return bool(
        await broker.redis.set(
            alive_key(broker.config.key_prefix, session_id, domain=domain),
            "1",
            ex=ttl,
            nx=True,
        )
    )


async def clear_alive(broker: Any, session_id: str, *, domain: str) -> None:
    """Release ownership. Safe to call for a session that never claimed one."""
    await broker.redis.delete(
        alive_key(broker.config.key_prefix, session_id, domain=domain)
    )


async def is_alive(broker: Any, session_id: str, *, domain: str) -> bool:
    return bool(
        await broker.redis.exists(
            alive_key(broker.config.key_prefix, session_id, domain=domain)
        )
    )

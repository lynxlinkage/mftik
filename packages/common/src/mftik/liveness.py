"""Per-session liveness keys — how a process that died is noticed.

A session row is closed by the process that owns it. That covers every
ending the process gets to observe, including a shutdown cut short. It does
not cover the process disappearing outright — SIGKILL, OOM, the machine
going away — where nothing runs and the row stays ``live`` forever with no
owner left to correct it: a session the UI shows as running that nobody can
stop.

So every live session holds a short-lived lease, refreshed while its owner is
running. The lease is what lets a process tell "a peer owns this session"
from "nobody does". That distinction cannot be made from process-local state:
several processes of a plane serve the same RPC subject as competing
consumers, so one finding a live row it does not own has no way of knowing,
by itself, whether a peer is running it.

Shared rather than STS-local because the question outlives STS: td and md
rows are keyed by the STS session they were attached for, and they face the
same live-row-with-no-owner problem. What they must not do is answer it by
reading STS's lease. A session id is one thing; *which plane still holds it*
is another, and an md attach can die while the strategy it served keeps
running. Hence ``domain``: one lease per (plane, session), each written and
read by the plane that owns the row it guards.

What this module is, then, is the *policy*: which lease answers which
question, and what a lapse means for the row behind it. The transport is
:class:`~mftik.broker.Broker`'s lease primitives, and nothing here reaches
past them — the keys these names become, their expiry, and the races in the
conditional writes all belong to the broker.
"""

from __future__ import annotations

from mftik.broker import Broker

#: How long a lease outlives its last refresh. Generous next to the default
#: 1s heartbeat: the cost of waiting is a stale row lingering a little
#: longer, while the cost of expiring early is declaring a healthy session
#: dead and marking a running strategy failed.
ALIVE_TTL_SECONDS = 30


def alive_name(session_id: str, *, domain: str) -> str:
    return f"{domain}:alive:{session_id}"


async def mark_alive(
    broker: Broker,
    session_id: str,
    *,
    domain: str,
    ttl: int = ALIVE_TTL_SECONDS,
) -> None:
    """Claim / renew this plane's ownership of ``session_id``."""
    await broker.lease_put(alive_name(session_id, domain=domain), ttl=ttl)


async def claim_alive(
    broker: Broker,
    session_id: str,
    *,
    domain: str,
    ttl: int = ALIVE_TTL_SECONDS,
) -> bool:
    """Take ownership of ``session_id`` only if nobody else holds it.

    ``lease_take`` rather than :func:`mark_alive`'s unconditional write,
    because this asks a question instead of stating a fact: several STS
    processes rebuild on boot at once, and without the atomic test they would
    each restore the same session and run two of it.

    Returns whether the claim was taken.
    """
    return await broker.lease_take(
        alive_name(session_id, domain=domain), ttl=ttl
    )


async def clear_alive(broker: Broker, session_id: str, *, domain: str) -> None:
    """Release ownership. Safe to call for a session that never claimed one."""
    await broker.lease_drop(alive_name(session_id, domain=domain))


async def is_alive(broker: Broker, session_id: str, *, domain: str) -> bool:
    return await broker.lease_held(alive_name(session_id, domain=domain))


# --- resource ownership ----------------------------------------------------
#
# The leases above answer "is anyone running this session". These answer a
# different question: "may I be the one running this resource at all". A
# session is claimed by whoever is asked to run it; an account is claimed
# against *rivals*, because two processes holding one credential is two
# order books, two ledgers and one venue that believes both.

#: How long an ownership claim outlives its last refresh. Short enough that a
#: process which died does not hold an account out of service for long, and
#: many refreshes wide so a broker blip never costs a live account its claim.
OWNER_TTL_SECONDS = 30


def owner_name(resource: str, *, domain: str) -> str:
    return f"{domain}:owner:{resource}"


async def claim_owner(
    broker: Broker,
    resource: str,
    *,
    domain: str,
    owner: str,
    ttl: int = OWNER_TTL_SECONDS,
) -> str | None:
    """Take exclusive ownership of ``resource``.

    ``None`` when the claim is now ours. Otherwise the id of whoever holds it,
    so a refusal can name them rather than saying only that it failed.

    ``owner`` must identify the *process*, not the instance. Two processes
    configured with the same ``MFTIK_INSTANCE`` is precisely the mistake this
    guards against, and a claim keyed by instance name would hand it straight
    to the second one.
    """
    name = owner_name(resource, domain=domain)
    if await broker.lease_take(name, ttl=ttl, owner=owner):
        return None
    held = await broker.lease_owner(name)
    # Lapsed between the two calls: nobody holds it, but we do not either.
    # The caller retries or refuses; inventing ownership here would be the one
    # outcome this function exists to prevent.
    return held if held is not None else "unknown"


async def hold_owner(
    broker: Broker,
    resource: str,
    *,
    domain: str,
    owner: str,
    ttl: int = OWNER_TTL_SECONDS,
) -> bool:
    """Refresh a claim we still hold. ``False`` means it is no longer ours.

    A lost claim sends the caller back through :func:`claim_owner` rather than
    re-creating one here, which is what leaves a rival a moment to say no. The
    race inside the refresh, and which way it is lost, is
    :meth:`~mftik.broker.Broker.lease_hold`'s.
    """
    return await broker.lease_hold(
        owner_name(resource, domain=domain), owner=owner, ttl=ttl
    )


async def release_owner(
    broker: Broker, resource: str, *, domain: str, owner: str
) -> None:
    """Give up a claim, if it is still ours to give up."""
    await broker.lease_release(
        owner_name(resource, domain=domain), owner=owner
    )

"""Which instance of a plane this process is.

A node can run several processes of one plane — ``td-jp-1``, ``md-jp-2``,
``sts-tw`` — and the control plane addresses them by name. The name is read
here, from the environment, and nowhere else: it is set in a compose file on
the host, which the API has never read and cannot write. That is why an
instance cannot be renamed through the UI — a row edited there would not reach
the process that answers to it. See ``docs/Instances.md``.

The default is the plane's own name, so a deployment that has never heard of
this is already an instance called ``td`` / ``md`` / ``sts`` — the three
migration 0031 declares. Nothing has to be configured for the single-process
case to keep working.
"""

from __future__ import annotations

import os
import re
from enum import StrEnum

#: An instance name is one subject segment — ``health.{domain}.{name}``
#: — and the value of ``MFTIK_INSTANCE``. Dots split the subject, spaces and
#: uppercase make a row no compose file will ever match, and there is no
#: rename. ``*`` is already the unpinned-md sentinel.
_INSTANCE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")


def validate_instance_name(name: str) -> str:
    """Return a legal instance name, or raise ``ValueError``.

    Strips surrounding whitespace so a form field and an env value are
    judged the same way. Does not lowercase: silently folding ``TD-JP-1``
    into ``td-jp-1`` would be a rename, and there is no rename.
    """
    name = name.strip()
    if not name:
        raise ValueError("name is required")
    if _INSTANCE_NAME.fullmatch(name) is None:
        raise ValueError(
            f"instance name {name!r} must be lowercase letters, digits "
            f"and hyphens, starting with a letter — it is a subject "
            f"segment and there is no rename"
        )
    return name


#: The environment variable a deployment sets to name one process.
INSTANCE_ENV = "MFTIK_INSTANCE"

#: Planes that may have more than one process. ``sym`` is off the hot path
#: behind ``SymbolClient``'s cache and ``paper`` exists to be one shared book,
#: so neither is instanced and neither reads this.
INSTANCED_PLANES = frozenset({"td", "md", "sts"})


def instance_name(plane: str) -> str:
    """This process's instance name, defaulting to ``plane``.

    Whitespace is stripped and an empty value is treated as unset, so an
    ``MFTIK_INSTANCE=`` left in a compose file reads as "the default" rather
    than as an instance whose name is the empty string — which would serve a
    subject ending in a dot and match no declared row.

    A value that is set but illegal refuses to start, which is the same rule
    ``/instances`` applies to a declaration and has to be, because these two
    names are compared for equality and neither side can rewrite the other.
    Coming up anyway is the worse failure: ``MFTIK_INSTANCE=MD-JP-1`` would
    serve ``health.md.MD-JP-1``, a name no declaration can hold, and read as
    *down* on Home forever with the process healthy. Raising here says which
    variable while somebody is still looking at the deploy.
    """
    raw = os.getenv(INSTANCE_ENV, "")
    name = raw.strip()
    if not name:
        return plane
    try:
        return validate_instance_name(name)
    except ValueError as exc:
        raise ValueError(f"{INSTANCE_ENV}={raw!r}: {exc}") from exc


#: The environment variable that sets an instance's role.
ROLE_ENV = "MFTIK_ROLE"


class Role(StrEnum):
    """How much of its plane's work one instance will answer for.

    One ordered role rather than two booleans, and the reason is that
    ``standby`` has to gate the *unicast* subject too. ``docs/MdHandover.md``
    needs a warming MD to answer no attach at all, and blue and green are both
    ``md-jp-1`` — a green that gated only the anycast subject would still take
    a named attach for feeds it does not have. Two booleans would also admit a
    meaningless fourth state and put a two-way interaction at every serve site.
    """

    #: Serves nothing and runs no reaper. Its links keep running: this gates
    #: ``run_rpc``, never the dispatcher, so an instance being replaced stops
    #: taking new work while finishing what it holds.
    STANDBY = "standby"
    #: Answers only work addressed to it by name.
    NAMED = "named"
    #: Also takes from the plane's shared pool. The default, so that adding
    #: unicast subjects changes nothing observable.
    ACTIVE = "active"

    @property
    def serves_unicast(self) -> bool:
        return self is not Role.STANDBY

    @property
    def serves_anycast(self) -> bool:
        return self is Role.ACTIVE

    @property
    def runs_reaper(self) -> bool:
        """A standby instance has nothing to reap and a peer that does."""
        return self is not Role.STANDBY


#: Planes for which ``standby`` is a legitimate configuration.
#:
#: MD alone, and ``docs/MdHandover.md`` says why: a strategy session holds
#: positions and places orders, so two copies of STS is two copies deciding to
#: trade, and TD is the same argument about an account. Neither is ever
#: blue/greened, which is the only thing ``standby`` is for. A TD sitting in
#: standby is not dangerous, merely useless — it answers nothing — and a
#: configuration that silently does nothing is worth refusing at boot.
STANDBY_PLANES = frozenset({"md"})


def instance_role(plane: str) -> Role:
    """This process's role, defaulting to :attr:`Role.ACTIVE`.

    Raises ``ValueError`` for a value this plane cannot hold, rather than
    quietly correcting it. Every caller is a process starting up, so the
    exception is a boot failure with a sentence in it — which is what someone
    who set the variable needs, and strictly better than a plane that comes up
    and answers nothing.
    """
    raw = os.getenv(ROLE_ENV, "").strip().lower()
    if not raw:
        return Role.ACTIVE
    try:
        role = Role(raw)
    except ValueError:
        raise ValueError(
            f"{ROLE_ENV}={raw!r} is not a role; "
            f"expected one of {sorted(r.value for r in Role)}"
        ) from None
    if role is Role.STANDBY and plane not in STANDBY_PLANES:
        raise ValueError(
            f"{ROLE_ENV}=standby is not available to {plane} — only "
            f"{sorted(STANDBY_PLANES)} is ever replaced while running, and a "
            f"standby {plane} would answer nothing at all"
        )
    return role


def control_subjects(plane: str, instance: str, role: Role) -> list[str]:
    """Which control-plane subjects this process should serve.

    Ordered named-first so a log line reads as the instance's own subject
    followed by whatever pool it also draws from.
    """
    from mftik.protocol import Topics

    named = {"td": Topics.td, "sts": Topics.sts, "md": Topics.md}
    #: No ``td``. Everything that reaches TD carries an ``api_id``, and an
    #: ``api_id`` names the one instance allowed to use that credential — so
    #: there is no such thing as unaddressed TD work for a pool to take. A TD
    #: configured ``active`` therefore serves exactly what a ``named`` one
    #: does; the value is accepted rather than refused because a fleet-wide
    #: ``MFTIK_ROLE=active`` is a reasonable thing to write and means something
    #: unambiguous here.
    anycast = {"sts": Topics.STS, "md": Topics.MD}
    if plane not in named:
        raise ValueError(
            f"{plane} is not an instanced plane; expected one of "
            f"{sorted(INSTANCED_PLANES)}"
        )

    subjects: list[str] = []
    if role.serves_unicast:
        subjects.append(named[plane](instance))
    if role.serves_anycast and plane in anycast:
        subjects.append(anycast[plane])
    return subjects

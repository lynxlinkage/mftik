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
    """
    raw = os.getenv(INSTANCE_ENV, "")
    name = raw.strip()
    return name or plane

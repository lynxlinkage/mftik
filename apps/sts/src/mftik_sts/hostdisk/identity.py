"""Environment names the session worker reads for its code identity.

The controller puts these on ``WorkerSpec.env`` on top of
:func:`mftik_sts.controller.env.forwarded_env`. They are not part of
that forwarded set: the worker's existing code does not read them, and
the forwarded list stays the names it already knew.

This module does not import a strategy tree.
"""

from __future__ import annotations

#: Digest the API pinned at start. The worker loads ``trees/<digest>/``.
STRATEGY_DIGEST_ENV = "MFTIK_STRATEGY_DIGEST"

#: Extras generation the API pinned at start, as a decimal integer.
ENV_GENERATION_ENV = "MFTIK_ENV_GENERATION"

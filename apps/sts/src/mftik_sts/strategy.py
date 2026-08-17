"""Where :class:`Strategy` used to live, and where strategies still find it.

The class moved to :mod:`mftik.strategy` so it ships in the package a strategy
author installs, rather than only inside this app. This module stays because
the old path is written down in places a rename cannot reach: every strategy
tree already on disk imports it, including the copies pulled from peers, and
``mftik.registry.gate`` recognises a subclass by the module it was imported
from. Both spellings are accepted there; this is what keeps the older one
true.

New strategies should import from :mod:`mftik.strategy`.
"""

from mftik.strategy import Strategy

__all__ = ["Strategy"]

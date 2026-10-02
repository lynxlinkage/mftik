"""Load the strategy tree a session is pinned to.

The session worker calls :func:`load_pinned` when
``MFTIK_STRATEGY_DIGEST`` is set. That package does not import
:mod:`mftik.registry` or :mod:`mftik.environment` (its surface test),
and this module does not import :mod:`mftik_sts.hostdisk` or
:mod:`mftik_db`. The controller already chose the digest. This only
opens that directory and, when ``MFTIK_ENV_GENERATION`` is set, puts
that generation's ``site-packages`` on ``sys.path``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from mftik.strategy import Strategy

#: Same strings as :mod:`mftik_sts.hostdisk.identity` and the worker.
#: Inlined so importing this module does not load the controller.
STRATEGY_DIGEST_ENV = "MFTIK_STRATEGY_DIGEST"
ENV_GENERATION_ENV = "MFTIK_ENV_GENERATION"


def load_pinned(digest: str) -> Strategy:
    """Instantiate the class in ``registry/trees/<digest>``.

    A missing tree is ``KeyError``, which the worker reports as an
    init failure. This does not call
    :func:`mftik_sts.runtime_env.refresh`.
    """
    from mftik.environment import NodeEnv
    from mftik.registry.files import read_tree
    from mftik.registry.gate import check_files
    from mftik.registry.inspect import pick_class
    from mftik.registry.load import load_class
    from mftik.registry.store import DATA_ENV, DEFAULT_DATA_DIR

    raw = os.environ.get(DATA_ENV, "").strip() or DEFAULT_DATA_DIR
    data = Path(raw)
    generation = os.environ.get(ENV_GENERATION_ENV, "").strip()
    if generation:
        try:
            number = int(generation)
        except ValueError as exc:
            raise KeyError(f"env generation {generation!r} is not an int") from exc
        site = NodeEnv(data).site_packages(number)
        if site.is_dir():
            entry = str(site)
            if entry not in sys.path:
                sys.path.insert(0, entry)
    tree = data / "registry" / "trees" / digest
    if not tree.is_dir():
        raise KeyError(f"strategy digest {digest} is not on this disk")
    chosen = pick_class(check_files(read_tree(tree)))
    cls = load_class(
        tree,
        type_name=chosen.type,
        source="digest",
        name=chosen.type,
        digest=digest,
    )
    instance = cls()
    if not isinstance(instance, Strategy):
        raise KeyError(f"{chosen.type} is not a Strategy")
    return instance

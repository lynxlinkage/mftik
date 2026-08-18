"""Identity of a strategy is the hash of its Python source.

Two trees with the same ``.py`` files are the same strategy even if they
live under different names on disk, or ship different ``strategy.yml``
templates.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

DIGEST_PREFIX = "sha256:"


def digest_files(files: Mapping[str, bytes]) -> str:
    """Return ``sha256:<hex>`` over ``.py`` files, paths in sorted order.

    Each path is hashed with its contents so swapping two files' bodies is a
    different identity, not a different zip layout. Non-Python sidecars
    (the optional ``strategy.yml``) ride along in the tree but do not
    change identity: a peer that still drops the template must still agree
    on the digest.
    """
    hasher = hashlib.sha256()
    for path in sorted(p for p in files if p.endswith(".py")):
        body = files[path]
        hasher.update(path.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(hashlib.sha256(body).digest())
    return DIGEST_PREFIX + hasher.hexdigest()

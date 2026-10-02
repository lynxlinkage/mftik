"""STS host disk: digest-addressed trees, pins, and the import probe.

This is the layer §3.4 names ``mftik_sts.hostdisk``. It is the shape of
the registry replica and the extras generations on an STS volume
(§5.7, F39). Writing them from ``sts.registry.sync`` / ``sts.env.sync``,
and serving artifacts and event-log reads, is B5-10 and B5-11. The
controller handlers for those subjects raise
``NotImplementedError("IF-16")``.

**State authority (§3.3).**

* The strategy-tree catalogue (which names exist, which digest is
  current) is the API's registry store. This layer holds the STS copy:
  ``registry/trees/<digest>/`` and the name → digest index. The
  controller is the only writer, and only from the API's fan-out and
  the boot catch-up. A session worker reads a tree to load it.
* The extras catalogue is the API's ``env/applied.json``. This layer
  holds ``env/gen-{N}`` on the STS volume. The same writer rule.
* The pin ``(strategy_digest, env_generation)`` is the API's, stored on
  ``sts_sessions``. This layer reads it off a :class:`SessionSpec`. It
  does not choose the pin and it does not write the row.
* Artifacts stay on the artifact volume. Operator writes go through the
  controller later (F40, B5-11). This package does not open
  :class:`mftik.strategy.artifacts.ArtifactStore` and does not define a
  digest prefix of its own.
* Where the non-terminal specs are read from — the ``sts_sessions`` rows
  or ``supervisor.json`` — is not decided. :func:`pinned_code` is a pure
  function of the specs it is given.

**Invariants.**

* A new digest does not overwrite a tree that is already stored. The
  index moves; a pinned older digest stays until nothing pins it and it
  is not the index's current version.
* :func:`gc_env` keeps pinned generations and the stamp's current
  generation. It calls :meth:`NodeEnv._prune_generations`.
* :func:`deployable` and :func:`rehang_code` do not import the tree.
  :func:`probe` imports it in a child interpreter.
  :func:`mftik.registry.load.load_class` and
  :func:`mftik.registry.protocol.handshake_info` run only in that child.
* A rehang uses ``spec.strategy_digest``, not the index. An incompatible
  ``requires_mftik`` is failed and alerted. A missing pinned tree is
  ``strategy_unavailable``.
* ``api.registry.catchup`` is not a handler on this controller. The API
  serves that subject. The client signature is
  :func:`mftik_sts.controller.catch_up_registry`.
"""

from mftik_sts.hostdisk._ticket import TICKET
from mftik_sts.hostdisk.checks import (
    REASON_DIGEST_ABSENT,
    REASON_ENV_ABSENT,
    REASON_REQUIRES_MFTIK,
    REASON_STRATEGY_UNAVAILABLE,
    Deployability,
    RehangCode,
    deployable,
    rehang_code,
    release_accepts,
)
from mftik_sts.hostdisk.pin import PinnedCode, gc_env, pinned_code
from mftik_sts.hostdisk.probe import ProbeResult, probe
from mftik_sts.hostdisk.replica import TreeReplica

__all__ = [
    "REASON_DIGEST_ABSENT",
    "REASON_ENV_ABSENT",
    "REASON_REQUIRES_MFTIK",
    "REASON_STRATEGY_UNAVAILABLE",
    "TICKET",
    "Deployability",
    "PinnedCode",
    "ProbeResult",
    "RehangCode",
    "TreeReplica",
    "deployable",
    "gc_env",
    "pinned_code",
    "probe",
    "rehang_code",
    "release_accepts",
]

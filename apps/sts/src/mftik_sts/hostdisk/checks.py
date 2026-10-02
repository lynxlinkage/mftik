"""Deployability and the rehang's code identity, without importing a tree.

Both reads go through :func:`mftik.registry.gate.check_files`. The
``requires_mftik`` string is the one :class:`mftik.registry.store.RegistryStore`
stores on :attr:`mftik.registry.store.AddedStrategy.requires_mftik`,
including the ``0.1.0`` fallback the store uses when the class omits it.
``load_class`` is not called here. The probe subprocess is the only
importer.
"""

from __future__ import annotations

from dataclasses import dataclass

from mftik.environment import NodeEnv
from mftik.registry.errors import RegistryError
from mftik.registry.files import read_tree
from mftik.registry.gate import StrategyClass, check_files
from mftik.registry.inspect import pick_class

from mftik_sts.controller.types import SessionSpec
from mftik_sts.hostdisk.replica import TreeReplica

#: ``RegistryStore.add`` writes this when the class has no ``requires_mftik``.
_DEFAULT_REQUIRES_MFTIK = "0.1.0"

#: The pinned tree is not on this disk and cannot be fetched back (F39).
REASON_STRATEGY_UNAVAILABLE = "strategy_unavailable"
#: The tree's ``requires_mftik`` does not accept the release that would spawn.
REASON_REQUIRES_MFTIK = "requires_mftik"
REASON_DIGEST_ABSENT = "digest not on this disk"
REASON_ENV_ABSENT = "env generation not on this disk"


@dataclass(frozen=True, slots=True)
class Deployability:
    """Whether ``spec`` can be started on this disk, without importing it.

    ``ok`` is false when the pinned digest or env generation is missing,
    or when the tree's ``requires`` is not a subset of the applied extras.
    ``reason`` is ``None`` when ``ok`` is true.
    """

    ok: bool
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.ok and self.reason is not None:
            raise ValueError("a deployable spec has no reason")
        if not self.ok and not self.reason:
            raise ValueError("a refusal names a reason")


@dataclass(frozen=True, slots=True)
class RehangCode:
    """What a rehang spawns, and whether that rehang is failed.

    ``digest`` is the spec's pin. It is never the index's current digest
    for the strategy name. ``failed`` is the record the orchestrator
    writes; this function does not write the row (B5-10). ``alert`` is
    true for an incompatible ``requires_mftik`` (F39). A missing tree is
    ``strategy_unavailable`` and does not alert: the plan names the alert
    on the version check only.
    """

    digest: str | None
    failed: bool
    reason: str | None
    alert: bool


def deployable(
    spec: SessionSpec, *, replica: TreeReplica, env: NodeEnv
) -> Deployability:
    """Digest and generation on this disk, and ``requires`` against extras.

    A built-in strategy (``strategy_digest is None``) has no tree to find.
    A spec with no ``env_generation`` has no generation directory to find.
    Extras are :meth:`NodeEnv.extras_names` — the applied stamp, which is
    the only extras list on the volume. This does not import the tree.
    """
    if not isinstance(spec, SessionSpec):
        raise TypeError("deployable reads a SessionSpec")
    if spec.strategy_digest is not None:
        path = replica.path_of(spec.strategy_digest)
        if path is None:
            return Deployability(ok=False, reason=REASON_DIGEST_ABSENT)
        try:
            chosen = pick_class(check_files(read_tree(path)))
        except RegistryError as exc:
            return Deployability(ok=False, reason=str(exc))
        missing = [name for name in chosen.requires if name not in env.extras_names()]
        if missing:
            return Deployability(
                ok=False, reason="requires: " + ", ".join(missing)
            )
    if spec.env_generation is not None and not env.site_packages(
        spec.env_generation
    ).is_dir():
        return Deployability(ok=False, reason=REASON_ENV_ABSENT)
    return Deployability(ok=True)


def rehang_code(
    spec: SessionSpec, *, replica: TreeReplica, release: str
) -> RehangCode:
    """The digest a rehang uses, and whether that rehang is failed.

    The digest is ``spec.strategy_digest``, including when the index has
    since moved to another digest of the same name. A built-in strategy
    is not failed for ``requires_mftik``: its code is ``release``.

    ``requires_mftik`` is compared as a numeric minimum
    (:func:`release_accepts`). The plan says an incompatible tree is
    failed and alerted; it does not name the predicate. A floor is what
    lets a strategy that asked for 0.1.0 keep running after the release
    moves forward, and fail when the release is still behind what the
    tree declared.
    """
    if not isinstance(spec, SessionSpec):
        raise TypeError("rehang_code reads a SessionSpec")
    if not isinstance(release, str) or release == "":
        raise ValueError("release must be a non-empty version string")
    digest = spec.strategy_digest
    if digest is None:
        return RehangCode(digest=None, failed=False, reason=None, alert=False)
    path = replica.path_of(digest)
    if path is None:
        return RehangCode(
            digest=digest,
            failed=True,
            reason=REASON_STRATEGY_UNAVAILABLE,
            alert=False,
        )
    try:
        chosen = pick_class(check_files(read_tree(path)))
    except RegistryError as exc:
        return RehangCode(digest=digest, failed=True, reason=str(exc), alert=True)
    if not release_accepts(_requires_mftik(chosen), release):
        return RehangCode(
            digest=digest,
            failed=True,
            reason=REASON_REQUIRES_MFTIK,
            alert=True,
        )
    return RehangCode(digest=digest, failed=False, reason=None, alert=False)


def release_accepts(requires_mftik: str, release: str) -> bool:
    """True when ``release`` is at least the declared minimum.

    Both sides are dotted integers (``0.2.0``). Anything else is
    incompatible, so a rehang fails closed rather than spawning a tree
    whose declaration this process cannot read. Equal counts as
    compatible.
    """
    need = _version(requires_mftik)
    have = _version(release)
    if need is None or have is None:
        return False
    return have >= need


def _requires_mftik(chosen: StrategyClass) -> str:
    """The string ``RegistryStore.add`` stores on ``AddedStrategy``."""
    return chosen.requires_mftik or _DEFAULT_REQUIRES_MFTIK


def _version(value: str) -> tuple[int, ...] | None:
    if not isinstance(value, str) or value == "":
        return None
    parts = value.split(".")
    numbers: list[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        numbers.append(int(part))
    return tuple(numbers)

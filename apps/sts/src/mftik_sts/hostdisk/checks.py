"""Deployability and the rehang's code identity, without importing a tree.

Both reads go through :func:`mftik.registry.gate.check_files`. The
``requires_mftik`` string is the one :class:`mftik.registry.store.RegistryStore`
stores on :attr:`mftik.registry.store.AddedStrategy.requires_mftik`,
including the ``0.1.0`` fallback the store uses when the class omits it.
``load_class`` is not called here. The probe subprocess is the only
importer.
"""

from __future__ import annotations

import os
import re
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

#: Set to ``1`` when this tree is a source checkout whose distribution
#: version is ``0.0.0``. Otherwise a source-tree release refuses every
#: ``requires_mftik``, including ``0.0.0``.
MFTIK_DEV_RELEASE = "MFTIK_DEV_RELEASE"

_PRE_RANK = {"a": 0, "b": 1, "rc": 2}
_PRE_ALIAS = {
    "a": "a",
    "alpha": "a",
    "b": "b",
    "beta": "b",
    "rc": "rc",
    "c": "rc",
    "preview": "rc",
}
_VERSION = re.compile(
    r"^(?:(?P<epoch>[0-9]+)!)?"
    r"(?P<release>[0-9]+(?:\.[0-9]+)*)"
    r"(?:\.?(?P<pre_l>a|alpha|b|beta|rc|c|preview)(?P<pre_n>[0-9]*))?"
    r"(?:\.post(?P<post>[0-9]+))?"
    r"(?:\.dev(?P<dev>[0-9]+))?"
    r"(?:\+(?P<local>[0-9A-Za-z.]+))?"
    r"$",
    re.IGNORECASE,
)

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

    ``requires_mftik`` is compared as a PEP 440 minimum
    (:func:`release_accepts`) against the release the caller passes.
    The plan says an incompatible tree is failed and alerted; it does
    not name the predicate. A floor is what lets a strategy that asked
    for 0.1.0 keep running after the release moves forward, and fail
    when the release is still behind what the tree declared.
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


def installed_release() -> str:
    """The installed ``mftik`` distribution version.

    Missing package metadata is ``0.0.0``, the hatch default for a
    source tree. That value is not a release: :func:`release_accepts`
    refuses it unless :data:`MFTIK_DEV_RELEASE` is ``1`` or the caller
    passes ``allow_dev=True``.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        found = version("mftik")
    except PackageNotFoundError:
        return "0.0.0"
    if not isinstance(found, str) or found == "":
        return "0.0.0"
    return found


def release_accepts(
    requires_mftik: str, release: str, *, allow_dev: bool | None = None
) -> bool:
    """True when ``release`` is at least the declared minimum.

    Both sides are PEP 440 versions. A leading ``v`` is ignored. Trailing
    zeros do not matter (``0.2`` and ``0.2.0`` are the same release).
    A final release is newer than ``rc``, which is newer than ``b``,
    which is newer than ``a``. A release with no ``.dev`` is newer than
    the same release with one. An epoch (``1!``) fails closed. Anything
    this parser cannot read fails closed.

    A source-tree release — ``0``, ``0.0``, ``0.0.0``, or the same with
    a leading ``v``, and no pre, post, or dev — accepts every
    ``requires_mftik`` when ``allow_dev`` is true, and accepts nothing
    when it is false, including a requirement of ``0.0.0``. ``allow_dev``
    left unset reads :data:`MFTIK_DEV_RELEASE`: only the value ``1``
    turns it on.
    """
    if allow_dev is None:
        allow_dev = os.environ.get(MFTIK_DEV_RELEASE) == "1"
    have = _parse_version(release)
    if have is not None and have.source_tree:
        return allow_dev
    if have is None or have.epoch is not None:
        return False
    need = _parse_version(requires_mftik)
    if need is None or need.epoch is not None:
        return False
    return _version_key(have) >= _version_key(need)


def _requires_mftik(chosen: StrategyClass) -> str:
    """The string ``RegistryStore.add`` stores on ``AddedStrategy``."""
    return chosen.requires_mftik or _DEFAULT_REQUIRES_MFTIK


@dataclass(frozen=True, slots=True)
class _ParsedVersion:
    epoch: int | None
    release: tuple[int, ...]
    pre: tuple[int, int] | None
    post: int | None
    dev: int | None

    @property
    def source_tree(self) -> bool:
        """``0.0.0`` with no epoch, pre, post, or dev."""
        return (
            self.epoch is None
            and self.pre is None
            and self.post is None
            and self.dev is None
            and self.release == (0,)
        )


def _parse_version(value: str) -> _ParsedVersion | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text[:1] in {"v", "V"}:
        text = text[1:]
    match = _VERSION.fullmatch(text)
    if match is None:
        return None
    epoch = match.group("epoch")
    parts = [int(part) for part in match.group("release").split(".")]
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    pre_l = match.group("pre_l")
    pre: tuple[int, int] | None = None
    if pre_l is not None:
        label = _PRE_ALIAS[pre_l.lower()]
        number = match.group("pre_n")
        pre = (_PRE_RANK[label], int(number) if number else 0)
    post = match.group("post")
    dev = match.group("dev")
    return _ParsedVersion(
        epoch=None if epoch is None else int(epoch),
        release=tuple(parts),
        pre=pre,
        post=None if post is None else int(post),
        dev=None if dev is None else int(dev),
    )


def _version_key(
    parsed: _ParsedVersion,
) -> tuple[tuple[int, ...], tuple[int, int], int, tuple[int, int]]:
    """Order final > rc > b > a, and a release with no ``.dev`` above one with it."""
    pre = (3, 0) if parsed.pre is None else parsed.pre
    dev = (1, 0) if parsed.dev is None else (0, parsed.dev)
    post = 0 if parsed.post is None else parsed.post
    return (parsed.release, pre, post, dev)

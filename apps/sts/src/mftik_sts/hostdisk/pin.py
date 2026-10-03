"""Which digests and env generations this instance must keep (F39).

The keep set is a pure function of the non-terminal :class:`SessionSpec`
values the caller passes. Where those specs are read — the
``sts_sessions`` rows or ``supervisor.json`` — is not decided here.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass

from mftik.environment import NodeEnv

from mftik_sts.controller.types import SessionSpec


@dataclass(frozen=True, slots=True)
class PinnedCode:
    """Digests and extras generations a set of specs pins.

    ``None`` pins are omitted. A built-in strategy has no digest (F39).
    """

    digests: frozenset[str]
    env_generations: frozenset[int]


def pinned_code(specs: Iterable[SessionSpec]) -> PinnedCode:
    """The keep set for these specs.

    The caller passes the non-terminal specs of this instance. This
    function does not open the database and does not read
    ``supervisor.json``. A terminal session's pin is not in the iterable,
    so it is not kept.
    """
    digests: set[str] = set()
    generations: set[int] = set()
    for spec in specs:
        if not isinstance(spec, SessionSpec):
            raise TypeError("pinned_code reads SessionSpec values")
        if spec.strategy_digest is not None:
            digests.add(spec.strategy_digest)
        if spec.env_generation is not None:
            generations.add(spec.env_generation)
    return PinnedCode(frozenset(digests), frozenset(generations))


def gc_env(env: NodeEnv, keep: Collection[int]) -> None:
    """Delete ``gen-*`` directories that are neither pinned nor current.

    ``keep`` is the generations :func:`pinned_code` returned. The stamp's
    current generation is kept as well, the same rule as the registry
    index (§5.7). The deletion is :meth:`NodeEnv._prune_generations`,
    which also keeps whatever ``pinned-generations.json`` names.
    :meth:`NodeEnv.commit` prunes through that same method, so a
    generation written into the pin file before the commit survives.
    """
    if isinstance(keep, str) or not isinstance(keep, Collection):
        raise TypeError("keep must be a collection of generations")
    for generation in keep:
        if type(generation) is not int:
            raise TypeError("keep must be a collection of generations")
    retained = set(keep)
    retained.add(env.read_stamp().generation)
    env._prune_generations(keep=retained)

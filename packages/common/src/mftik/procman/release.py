"""Which release this process is, and the S-2 pin file.

``WorkerSpec.code_ref`` is the release of the controller that spawned the
worker (§4.5). :func:`current_release` is that string. Planes pass it
when they spawn. This module is the only place that reads
``STRATEGON_RELEASE_VERSION``.

Strategon names a release by the tag ``s7n.py apply`` is given
(``v0.9.5``). The image's ``MFTIK_DIST_VERSION`` is the PEP 440 form of
that tag (``0.9.5``). The two spellings are not the same string.
``mftik workers --stale`` (B8-06) compares with :func:`current_release`
on both sides, so a tag and a distribution version of one build are not
treated as two releases.

The pin file is the fallback in §4.5 S-2. Strategon's release GC keeps a
release whose rootfs is still some process's ``/proc/<pid>/root``, and
that GC says it does not read a pin file. The check that the agent can
see ``/proc/<pid>/root`` across user namespaces is still manual.
:func:`pinned_releases_path` returns a path only when
``STRATEGON_RELEASE_VERSION`` is set, so nothing is written while the
``/proc`` scan is what GC trusts. A plane passes the result as
``Supervisor(..., pin_path=...)``. ``None`` writes nothing.
"""

from __future__ import annotations

import importlib.metadata
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path

from mftik.procman.errors import ProcmanError
from mftik.procman.state import ALIVE_PHASES, WorkerPhase

#: ``STRATEGON_RELEASE_VERSION``. Not read anywhere else in this package.
_RELEASE_ENV = "STRATEGON_RELEASE_VERSION"

#: Strategon work directory when the caller does not pass one.
_WORK_DIR_ENV = "WORK_DIR"

#: ``<work dir>/.strategon/pinned-releases``, the path strategon#60 proposed.
_PIN_DIR = ".strategon"
_PIN_NAME = "pinned-releases"

#: Phases whose process may still be on a rootfs.
#:
#: :data:`~mftik.procman.state.ALIVE_PHASES` plus ``LOST``. A ``LOST`` slot
#: can still be running: the shim is gone and the worker was not reaped
#: (S-2). A reattached worker that is still up is ``STARTING`` or
#: ``RUNNING``, which are already in the alive set. ``FAILED``,
#: ``CRASHED``, ``STOPPED``, ``BACKOFF`` and ``FATAL`` are past the exit.
#: That rootfs is not in use, so the pin does not keep it.
PINNED_PHASES: frozenset[WorkerPhase] = frozenset(
    ALIVE_PHASES | {WorkerPhase.LOST}
)


def _single_line(value: str) -> str | None:
    """A version that can occupy one pin-file line, or ``None``."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or "\n" in text or "\r" in text:
        return None
    return text


def _strategon_release() -> str | None:
    """The release Strategon named, or ``None`` when it did not.

    Unset and whitespace are ``None``: there is no release to report, and
    the pin file stays dormant. A value that is not one line is refused.
    A pin file of more than one version per line would not be a list.
    """
    raw = os.environ.get(_RELEASE_ENV)
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    if "\n" in text or "\r" in text:
        raise ProcmanError(f"{_RELEASE_ENV} must be a single line")
    return text


def current_release() -> str:
    """The release this process is. Never an empty string.

    ``STRATEGON_RELEASE_VERSION`` when that variable is set and, after
    stripping, a single non-empty line. Otherwise the installed ``mftik``
    distribution version (:mod:`importlib.metadata`).

    The two sources spell the version differently. Strategon passes the
    tag (``v0.9.5``). The distribution version is the PEP 440 form of
    that tag (``0.9.5``). :func:`current_release` is the only reader of
    the variable. ``mftik workers --stale`` (B8-06) has to call this
    function for the worker's ``code_ref`` and for the latest release.
    Comparing a tag to a distribution version would mark every worker
    stale, or none of them.

    Raises:
        ProcmanError: the variable is set to more than one line, or it
            is unset and the installed version cannot be read.
    """
    named = _strategon_release()
    if named is not None:
        return named
    try:
        installed = importlib.metadata.version("mftik")
    except importlib.metadata.PackageNotFoundError as exc:
        raise ProcmanError(
            f"{_RELEASE_ENV} is unset and mftik is not installed"
        ) from exc
    text = _single_line(installed)
    if text is None:
        raise ProcmanError("installed mftik version is empty")
    return text


def _work_dir(work_dir: Path | None) -> Path:
    if work_dir is not None:
        return Path(work_dir)
    raw = os.environ.get(_WORK_DIR_ENV)
    if raw is not None and raw.strip():
        return Path(raw.strip())
    return Path.cwd()


def pinned_releases_path(work_dir: Path | None = None) -> Path | None:
    """The S-2 pin file, or ``None`` while Strategon scans ``/proc``.

    ``<work dir>/.strategon/pinned-releases`` when
    ``STRATEGON_RELEASE_VERSION`` is set and non-empty. ``work_dir`` is
    that directory; omitted, it is ``WORK_DIR`` or the current directory.
    A plane passes the result to ``Supervisor(..., pin_path=...)`` and
    does not read the variable itself. ``None`` means do not write a
    file: GC is still using the ``/proc`` scan, which does not consult
    a pin file.
    """
    if _strategon_release() is None:
        return None
    return _work_dir(work_dir) / _PIN_DIR / _PIN_NAME


def pinned_releases(
    own: str,
    held: Iterable[tuple[str, WorkerPhase]],
) -> tuple[str, ...]:
    """Versions to pin, one each, sorted.

    ``own`` is this controller's release and is always present. The file
    therefore never shrinks below that release, including when every
    worker has been released. ``held`` is ``(code_ref, phase)`` for slots
    this controller still holds. A phase in :data:`PINNED_PHASES`
    contributes its ``code_ref``. A slot that has been released is not in
    ``held``, so its release is absent unless ``own`` or another held
    slot still names it.

    A ``code_ref`` that is empty or more than one line is skipped. It
    cannot be a pin-file line. ``own`` has to be one non-empty line.

    Raises:
        ProcmanError: ``own`` is empty or is not a single line.
    """
    own_line = _single_line(own)
    if own_line is None:
        raise ProcmanError(
            "the controller release must be a single non-empty line"
        )
    versions = {own_line}
    for code_ref, phase in held:
        try:
            named = WorkerPhase(phase)
        except ValueError:
            continue
        if named not in PINNED_PHASES:
            continue
        line = _single_line(code_ref)
        if line is not None:
            versions.add(line)
    return tuple(sorted(versions))


def write_pinned_releases(path: Path, versions: Iterable[str]) -> None:
    """Replace ``path`` with one version per line, sorted and unique.

    Temp file, then :func:`os.replace`, in the same directory, the same
    way ``supervisor.json`` is written. The caller passes the lines
    :func:`pinned_releases` just built from the snapshot that is about
    to be written to ``supervisor.json``. An empty ``versions`` is
    refused: that would shrink the file below the controller's own
    release.

    Raises:
        ProcmanError: ``versions`` has no single-line release.
    """
    lines: list[str] = []
    seen: set[str] = set()
    for value in versions:
        line = _single_line(value)
        if line is None or line in seen:
            continue
        seen.add(line)
        lines.append(line)
    if not lines:
        raise ProcmanError("refusing to write a pin file with no release")
    lines.sort()
    body = "".join(f"{line}\n" for line in lines).encode()
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(
        prefix="pinned-releases.", suffix=".tmp", dir=destination.parent
    )
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, destination)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

"""Preview a peer's advertised extras before they can write this node's stamp.

Connect compares names. Import is the Owner action that would add the missing
ones. The preview is a diff; confirm is apply.

Two kinds of row cannot be installed, and they are not the same kind of
problem, so they do not share a message. A **guessed dist** is a peer on the
legacy flat handshake: there is a version, and the PyPI name was assumed from
the import name — the Owner can supply the right one and confirm.
An **unpinned** row is a peer that published names without pins, which is what
an authenticated ``/info`` withholds from an anonymous caller: no version
exists to install, and no override the Owner can type here supplies one. That
one needs a registry key from the peer, and saying "set dist" would send them
somewhere that cannot help.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from mftik.environment import EnvStamp

ADDED = "added"
KEPT = "kept"
CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class PeerExtra:
    name: str
    version: str
    dist: str
    guessed: bool
    #: The peer gave a version. False when it published the name only.
    pinned: bool = True


@dataclass(frozen=True, slots=True)
class ImportRow:
    name: str
    version: str
    dist: str
    status: Literal["added", "kept", "conflict"]
    guessed: bool = False
    pinned: bool = True
    local_version: str | None = None
    local_dist: str | None = None


@dataclass(frozen=True, slots=True)
class ImportPreview:
    rows: tuple[ImportRow, ...]

    @property
    def added(self) -> tuple[ImportRow, ...]:
        return tuple(row for row in self.rows if row.status == ADDED)

    @property
    def kept(self) -> tuple[ImportRow, ...]:
        return tuple(row for row in self.rows if row.status == KEPT)

    @property
    def conflicts(self) -> tuple[ImportRow, ...]:
        return tuple(row for row in self.rows if row.status == CONFLICT)

    @property
    def guessed_names(self) -> tuple[str, ...]:
        return tuple(row.name for row in self.rows if row.guessed)

    @property
    def unpinned_names(self) -> tuple[str, ...]:
        return tuple(row.name for row in self.rows if not row.pinned)


def parse_peer_extras(extras: object) -> dict[str, PeerExtra]:
    """Read handshake extras.

    A bare version string has a guessed ``dist``. An object with no
    ``version`` is unpinned — the peer published the name and withheld the
    pin — and its ``dist`` is not guessed, because there is nothing to
    install it with either way.
    """
    if extras is None:
        return {}
    if not isinstance(extras, dict):
        raise ValueError("peer extras is not an object")
    out: dict[str, PeerExtra] = {}
    for raw_name, item in extras.items():
        name = str(raw_name)
        if isinstance(item, str):
            out[name] = PeerExtra(
                name=name, version=item, dist=name, guessed=True
            )
            continue
        if not isinstance(item, dict):
            raise ValueError(f"peer extra {name!r} is not an object")
        version = str(item.get("version") or "")
        raw_dist = item.get("dist")
        dist = name if raw_dist is None or str(raw_dist) == "" else str(raw_dist)
        if not version:
            out[name] = PeerExtra(
                name=name, version="", dist=dist, guessed=False, pinned=False
            )
            continue
        out[name] = PeerExtra(
            name=name,
            version=version,
            dist=dist,
            guessed=dist == name and (raw_dist is None or str(raw_dist) == ""),
        )
    return out


def preview_import(
    local: EnvStamp,
    peer_extras: object,
    *,
    dist_overrides: Mapping[str, str] | None = None,
) -> ImportPreview:
    """Union the peer's extras with the local stamp. Does not install."""
    overrides = {str(k): str(v) for k, v in dict(dist_overrides or {}).items() if v}
    rows: list[ImportRow] = []
    for name, peer in sorted(parse_peer_extras(peer_extras).items()):
        local_rec = local.packages.get(name)
        dist = overrides.get(name, peer.dist)
        guessed = peer.guessed and name not in overrides
        if local_rec is not None and not peer.pinned:
            # Nothing to import and nothing to compare: this node already has
            # the package, and the peer did not say which version it runs.
            # Calling that a conflict against ``peer ''`` would refuse a
            # confirm over a value the peer never published.
            rows.append(
                ImportRow(
                    name=name,
                    version=local_rec.version,
                    dist=local_rec.dist,
                    status=KEPT,
                    pinned=True,
                    local_version=local_rec.version,
                    local_dist=local_rec.dist,
                )
            )
            continue
        if local_rec is None:
            rows.append(
                ImportRow(
                    name=name,
                    version=peer.version,
                    dist=dist,
                    status=ADDED,
                    guessed=guessed,
                    pinned=peer.pinned,
                )
            )
            continue
        if local_rec.version == peer.version:
            rows.append(
                ImportRow(
                    name=name,
                    version=local_rec.version,
                    dist=local_rec.dist,
                    status=KEPT,
                    guessed=False,
                    local_version=local_rec.version,
                    local_dist=local_rec.dist,
                )
            )
            continue
        rows.append(
            ImportRow(
                name=name,
                version=peer.version,
                dist=dist,
                status=CONFLICT,
                guessed=guessed,
                pinned=peer.pinned,
                local_version=local_rec.version,
                local_dist=local_rec.dist,
            )
        )
    return ImportPreview(rows=tuple(rows))


def confirm_blockers(preview: ImportPreview) -> list[str]:
    """Reasons confirm must not start the installer."""
    messages: list[str] = []
    for row in preview.conflicts:
        messages.append(
            f"{row.name}: local {row.local_version}, peer {row.version}"
        )
    for row in preview.added:
        if not row.pinned:
            messages.append(
                f"{row.name}: the peer published the name without a version. "
                "Its /info withholds pins from an anonymous caller — ask that "
                "node for a registry key and import again with it"
            )
            continue
        if row.guessed:
            messages.append(
                f"{row.name}: dist {row.dist!r} was guessed from the import "
                "name; set dist to the PyPI distribution before confirm"
            )
    return messages


def peer_source(url: str, name: str | None = None) -> str:
    if name:
        return f"peer:{name}"
    return f"peer:{url}"

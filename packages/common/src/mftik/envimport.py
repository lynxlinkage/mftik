"""Preview a peer's advertised extras before they can write this node's stamp.

Connect compares names. Import is the Owner action that would add the missing
ones. The preview is a diff; confirm is apply. A guessed ``dist`` — the
legacy flat handshake only gave a version — is listed, not installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from mftik.envapply import ApplySpec
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


@dataclass(frozen=True, slots=True)
class ImportRow:
    name: str
    version: str
    dist: str
    status: Literal["added", "kept", "conflict"]
    guessed: bool = False
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


def parse_peer_extras(extras: object) -> dict[str, PeerExtra]:
    """Read handshake extras. A bare version string has a guessed ``dist``."""
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
        if raw_dist is None or str(raw_dist) == "":
            out[name] = PeerExtra(
                name=name, version=version, dist=name, guessed=True
            )
        else:
            out[name] = PeerExtra(
                name=name,
                version=version,
                dist=str(raw_dist),
                guessed=False,
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
        if local_rec is None:
            rows.append(
                ImportRow(
                    name=name,
                    version=peer.version,
                    dist=dist,
                    status=ADDED,
                    guessed=guessed,
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
        if row.guessed:
            messages.append(
                f"{row.name}: dist {row.dist!r} was guessed from the import "
                "name; set dist to the PyPI distribution before confirm"
            )
    return messages


def union_specs(
    local: EnvStamp,
    preview: ImportPreview,
    *,
    source: str,
) -> dict[str, ApplySpec]:
    """Local rows keep their source; newly added rows are tagged ``source``."""
    packages = {
        name: ApplySpec(version=rec.version, dist=rec.dist, source=rec.source)
        for name, rec in local.packages.items()
    }
    for row in preview.added:
        packages[row.name] = ApplySpec(
            version=row.version,
            dist=row.dist,
            source=source,
        )
    return packages


def peer_source(url: str, name: str | None = None) -> str:
    if name:
        return f"peer:{name}"
    return f"peer:{url}"

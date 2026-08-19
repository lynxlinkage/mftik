"""Pull another node's published strategies into ``pulled/{name}/``."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryError
from mftik.registry.files import normalize_files
from mftik.registry.protocol import (
    check_handshake,
    check_remote_extras,
    extra_names,
    extras_version_warnings,
    handshake_info,
)
from mftik.registry.store import AddedStrategy, RegistryStore

_TIMEOUT = 15.0


@dataclass(frozen=True, slots=True)
class ConnectResult:
    name: str
    url: str
    pulled: tuple[AddedStrategy, ...]


@dataclass(frozen=True, slots=True)
class SyncRow:
    name: str
    type: str
    local_digest: str | None
    remote_digest: str | None
    status: str


@dataclass(frozen=True, slots=True)
class DiffResult:
    name: str
    url: str
    reachable: bool
    error: str | None
    rows: tuple[SyncRow, ...]
    extras_warnings: tuple[str, ...] = ()


def _auth(token: str | None) -> dict[str, str]:
    """The peer's registry key, if it issued us one.

    Absent for a node that publishes openly — ``/info`` is public either way,
    so the handshake still tells you whether you are talking to a registry
    before the key is ever needed.
    """
    return {"Authorization": f"Bearer {token}"} if token else {}


async def fetch_handshake(
    url: str,
    *,
    token: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> object:
    """GET ``{url}/registry/v1/info``. Does not write ``remotes.toml``."""
    base = _normalize_url(url)
    own = client is None
    http = client or httpx.AsyncClient(timeout=_TIMEOUT)
    try:
        info = await _get_json(
            http, f"{base}/registry/v1/info", headers=_auth(token)
        )
        check_handshake(info)
        return info
    finally:
        if own:
            await http.aclose()


async def connect_remote(
    store: RegistryStore,
    *,
    name: str,
    url: str,
    token: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> ConnectResult:
    """Handshake, remember the remote, and copy every strategy it publishes."""
    base = _normalize_url(url)
    own = client is None
    http = client or httpx.AsyncClient(timeout=_TIMEOUT)
    headers = _auth(token)
    try:
        info = await _get_json(http, f"{base}/registry/v1/info")
        check_handshake(info)
        check_remote_extras(
            info, extra_names(handshake_info(data_dir=store.data_dir))
        )
        # Remembered only once the peer has answered as a registry, so a typo
        # does not leave a broken remote behind — and only after the listing
        # below succeeds would be worse: the token is what makes it succeed.
        # Extras names are checked first so a missing extra cannot write
        # remotes.toml and then fail halfway through the copy.
        store.put_remote(name, base, token)
        listed = await _get_json(
            http, f"{base}/registry/v1/strategies", headers=headers
        )
        items = _strategy_list(listed)
        pulled: list[AddedStrategy] = []
        for item in items:
            short = item["name"]
            detail = await _get_json(
                http, f"{base}/registry/v1/strategies/{short}", headers=headers
            )
            contents = _contents(detail)
            expected = item.get("digest") or detail.get("digest")
            got = digest_files(normalize_files(contents))
            if expected and got != expected:
                raise RegistryError(
                    f"{short} digest mismatch: listed {expected}, got {got}"
                )
            pulled.append(
                store.add(contents, replace=True, origin=name)
            )
        return ConnectResult(name=name, url=base, pulled=tuple(pulled))
    finally:
        if own:
            await http.aclose()


async def diff_remote(
    store: RegistryStore,
    *,
    name: str,
    client: httpx.AsyncClient | None = None,
) -> DiffResult:
    """Compare this node's pulled copy with what the peer currently publishes."""
    remote = store.get_remote(name)
    if remote is None:
        raise RegistryError(f"unknown remote: {name}")
    pulled = {rec.name: rec for rec in store.list_pulled_from(name)}
    own = client is None
    http = client or httpx.AsyncClient(timeout=_TIMEOUT)
    try:
        try:
            listed = await _get_json(
                http,
                f"{remote.url}/registry/v1/strategies",
                headers=_auth(remote.token),
            )
            items = {row["name"]: row for row in _strategy_list(listed)}
        except (RegistryError, httpx.HTTPError) as exc:
            rows = tuple(
                SyncRow(
                    name=rec.name,
                    type=rec.type,
                    local_digest=rec.digest,
                    remote_digest=None,
                    status="unknown",
                )
                for rec in pulled.values()
            )
            return DiffResult(
                name=remote.name,
                url=remote.url,
                reachable=False,
                error=str(exc),
                rows=rows,
            )
        names = sorted(set(pulled) | set(items))
        rows = tuple(
            _sync_row(short, pulled.get(short), items.get(short)) for short in names
        )
        warnings: tuple[str, ...] = ()
        try:
            info = await _get_json(http, f"{remote.url}/registry/v1/info")
            warnings = extras_version_warnings(
                info, handshake_info(data_dir=store.data_dir)
            )
        except (RegistryError, httpx.HTTPError):
            warnings = ()
        return DiffResult(
            name=remote.name,
            url=remote.url,
            reachable=True,
            error=None,
            rows=rows,
            extras_warnings=warnings,
        )
    finally:
        if own:
            await http.aclose()


def _sync_row(
    short: str,
    local: AddedStrategy | None,
    remote: dict[str, str] | None,
) -> SyncRow:
    local_digest = local.digest if local is not None else None
    remote_digest = remote.get("digest") if remote is not None else None
    type_name = (
        (remote.get("type") if remote is not None else None)
        or (local.type if local is not None else short)
    )
    if local is None:
        status = "remote_only"
    elif remote is None:
        status = "local_only"
    elif local_digest == remote_digest:
        status = "synced"
    else:
        status = "diverged"
    return SyncRow(
        name=short,
        type=type_name,
        local_digest=local_digest,
        remote_digest=remote_digest,
        status=status,
    )


def _normalize_url(url: str) -> str:
    raw = url.strip().rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RegistryError("remote url must be http(s) with a host")
    return raw


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    headers: Mapping[str, str] | None = None,
) -> object:
    try:
        response = await client.get(url, headers=dict(headers or {}))
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise RegistryError(
            f"remote {exc.request.url} returned {exc.response.status_code}"
        ) from exc
    try:
        return response.json()
    except ValueError as exc:
        raise RegistryError(f"remote {url} did not return JSON") from exc


def _strategy_list(body: object) -> list[dict[str, str]]:
    if not isinstance(body, dict):
        raise RegistryError("remote /strategies is not an object")
    raw = body.get("strategies")
    if not isinstance(raw, list):
        raise RegistryError("remote /strategies is missing strategies")
    out: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise RegistryError("remote /strategies has a malformed entry")
        row = {str(k): str(v) for k, v in item.items() if isinstance(v, str)}
        out.append(row)
    return out


def _contents(detail: object) -> Mapping[str, str]:
    if not isinstance(detail, dict):
        raise RegistryError("remote strategy detail is not an object")
    contents = detail.get("contents")
    if not isinstance(contents, dict) or not contents:
        raise RegistryError("remote strategy has no contents")
    out: dict[str, str] = {}
    for path, body in contents.items():
        if not isinstance(path, str) or not isinstance(body, str):
            raise RegistryError("remote strategy contents must be path → text")
        out[path] = body
    return out

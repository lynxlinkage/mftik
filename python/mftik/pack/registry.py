"""A minimal OCI distribution push client (E04-D071, E09-D033, E09-D010).

Blob and manifest upload, Docker config.json credentials, and basic or bearer
token exchange. Redirects are not followed, and Authorization is not sent to
a different host. Error text does not include a credential.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import overload

from mftik.pack.error import PackError

CONFIG_MEDIA = "application/vnd.mftiko.worker.config.v1+json"
LAYER_MEDIA = "application/vnd.oci.image.layer.v1.tar+gzip"
MANIFEST_MEDIA = "application/vnd.oci.image.manifest.v1+json"
_CLIENT_ID = "mftik-pack"
_TOKEN_USER = "<token>"
_HELPER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,63}")


@dataclass(frozen=True)
class Blob:
    """One config or layer blob."""

    media_type: str
    digest: str
    data: bytes


@dataclass(frozen=True)
class Image:
    """A single OCI image manifest and the blobs it names. ``digest`` is ``StrategyRef.code``."""

    digest: str
    manifest: bytes
    config: Blob
    layers: tuple[Blob, ...]


@dataclass(frozen=True)
class Credential:
    """A registry credential. Empty fields mean that method is not configured."""

    username: str = ""
    secret: str = ""
    refresh_token: str = ""
    access_token: str = ""

    def __bool__(self) -> bool:
        return bool(self.username or self.secret or self.refresh_token or self.access_token)


@dataclass(frozen=True)
class _Response:
    status: int
    headers: dict[str, str]
    body: bytes
    url: str


class _NoRedirect(urllib.request.HTTPErrorProcessor):
    """Return 3xx and 4xx to the caller. The distribution client must not follow them."""

    def http_response(self, request, response):  # noqa: ARG002
        return response

    https_response = http_response


def sha256_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical_json(value: object) -> bytes:
    """Compact JSON. Object key order is the insertion order of ``value``."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def make_image(config: bytes, layers: list[bytes]) -> Image:
    """An OCI image manifest with no annotations and no index (E09-D034)."""
    config_blob = Blob(CONFIG_MEDIA, sha256_digest(config), config)
    layer_blobs = tuple(Blob(LAYER_MEDIA, sha256_digest(layer), layer) for layer in layers)
    document = {
        "schemaVersion": 2,
        "mediaType": MANIFEST_MEDIA,
        "config": _descriptor(config_blob),
        "layers": [_descriptor(layer) for layer in layer_blobs],
    }
    manifest = canonical_json(document)
    return Image(sha256_digest(manifest), manifest, config_blob, layer_blobs)


def connect_host(host: str) -> str:
    """``docker.io`` is served at ``registry-1.docker.io``."""
    if host == "docker.io":
        return "registry-1.docker.io"
    return host


def server_address(host: str) -> str:
    """The Docker config key for ``host``. Docker Hub is stored under the index URL."""
    if host in {"docker.io", "registry-1.docker.io"}:
        return "https://index.docker.io/v1/"
    return host


def split_repository(repo: str) -> tuple[str, str]:
    """``registry/repository`` into the host (with port) and the repository path."""
    if not repo or "://" in repo or "/" not in repo or repo.startswith("/"):
        raise PackError("repo must be registry/repository")
    host, name = repo.split("/", 1)
    if not host or not name or any(ord(char) < 32 for char in repo):
        raise PackError("repo must be registry/repository")
    return host, name


def load_credential(host: str) -> Credential | None:
    """The Docker credential for ``host``, or None when the push is anonymous.

    ``credsStore`` and ``credHelpers`` win over ``auths`` and there is no
    fallback. The platform default helper is not detected.
    """
    path = _config_path()
    if path is None:
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PackError("docker config cannot be read") from exc
    if not isinstance(raw, dict):
        raise PackError("docker config cannot be read")
    address = server_address(host)
    helper = _helper_name(raw, address)
    if helper is not None:
        return _helper_get(helper, address)
    return _auth_credential(raw, address)


class RegistryClient:
    """Pushes one image to one repository. One client caches one bearer token."""

    def __init__(self, host: str, repository: str, plain_http: bool, credential: Credential | None):
        self.repository = repository
        self.credential = credential or Credential()
        self._connect = connect_host(host)
        scheme = "http" if plain_http else "https"
        self._base = f"{scheme}://{self._connect}"
        self._netloc = self._connect.lower()
        self._authorization: str | None = None
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _NoRedirect,
        )

    def push(self, image: Image, tag: str) -> None:
        self._push_blob(image.config)
        for layer in image.layers:
            self._push_blob(layer)
        ref = tag if tag else image.digest
        self._push_manifest(ref, image.manifest)

    def _push_blob(self, blob: Blob) -> None:
        head = self._call("HEAD", self._blob_url(blob.digest), None, {})
        if head.status == 200:
            return
        if head.status != 404:
            raise _status("HEAD", head)
        post = self._call("POST", f"{self._v2()}/blobs/uploads/", b"", {})
        if post.status not in (200, 201, 202):
            raise _status("POST", post)
        posted = post.headers.get("docker-content-digest", "")
        if post.status in (200, 201) and posted == blob.digest:
            return
        location = post.headers.get("location", "")
        if not location:
            raise PackError("registry upload did not return a location")
        target = _with_digest(urllib.parse.urljoin(post.url, location), blob.digest)
        put = self._call("PUT", target, blob.data, {"Content-Type": "application/octet-stream"})
        if put.status not in (200, 201, 202):
            raise _status("PUT", put)
        got = put.headers.get("docker-content-digest", "")
        if got and got != blob.digest:
            raise PackError("registry digest mismatch")

    def _push_manifest(self, ref: str, manifest: bytes) -> None:
        quoted = urllib.parse.quote(ref, safe=":")
        url = f"{self._v2()}/manifests/{quoted}"
        put = self._call("PUT", url, manifest, {"Content-Type": MANIFEST_MEDIA})
        if put.status not in (200, 201, 202):
            raise _status("PUT", put)

    def _blob_url(self, digest: str) -> str:
        quoted = urllib.parse.quote(digest, safe=":")
        return f"{self._v2()}/blobs/{quoted}"

    def _v2(self) -> str:
        repo = urllib.parse.quote(self.repository, safe="/")
        return f"{self._base}/v2/{repo}"

    def _call(
        self,
        method: str,
        url: str,
        body: bytes | None,
        headers: dict[str, str],
    ) -> _Response:
        response = self._once(method, url, body, headers, self._auth_for(url))
        if response.status != 401:
            return response
        challenge = response.headers.get("www-authenticate", "")
        if not challenge:
            return response
        self._authorization = self._exchange(challenge)
        return self._once(method, url, body, headers, self._auth_for(url))

    def _auth_for(self, url: str) -> str | None:
        if not self._authorization:
            return None
        if urllib.parse.urlsplit(url).netloc.lower() != self._netloc:
            return None
        return self._authorization

    def _exchange(self, header: str) -> str:
        scheme, params = _choose(_parse_challenges(header))
        if scheme == "basic":
            if not self.credential.username or not self.credential.secret:
                raise PackError("registry asked for basic credentials")
            raw = f"{self.credential.username}:{self.credential.secret}".encode()
            return "Basic " + base64.b64encode(raw).decode("ascii")
        token = self._bearer(params)
        return "Bearer " + token

    def _bearer(self, params: dict[str, str]) -> str:
        cred = self.credential
        if cred.access_token:
            return cred.access_token
        realm = params.get("realm", "")
        if not realm:
            raise PackError("registry bearer challenge has no realm")
        service = params.get("service", "")
        scope = params.get("scope", "")
        scopes = scope.split(" ") if scope else [f"repository:{self.repository}:pull,push"]
        if cred.refresh_token:
            return self._oauth(realm, service, scopes, cred.refresh_token)
        return self._distribution(realm, service, scopes)

    def _distribution(self, realm: str, service: str, scopes: list[str]) -> str:
        url = _token_url(realm, service, scopes)
        auth = None
        cred = self.credential
        if cred.username or cred.secret:
            raw = f"{cred.username}:{cred.secret}".encode()
            auth = "Basic " + base64.b64encode(raw).decode("ascii")
        response = self._once("GET", url, None, {}, auth)
        return _token_from(response, ("access_token", "token"))

    def _oauth(self, realm: str, service: str, scopes: list[str], refresh: str) -> str:
        form = {
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "service": service,
            "client_id": _CLIENT_ID,
        }
        if scopes:
            form["scope"] = " ".join(scopes)
        body = urllib.parse.urlencode(form).encode("ascii")
        response = self._once(
            "POST",
            realm,
            body,
            {"Content-Type": "application/x-www-form-urlencoded"},
            None,
        )
        return _token_from(response, ("access_token",))

    def _once(
        self,
        method: str,
        url: str,
        body: bytes | None,
        headers: dict[str, str],
        auth: str | None,
    ) -> _Response:
        hdrs = {"User-Agent": "mftik-pack"}
        hdrs.update(headers)
        if auth:
            hdrs["Authorization"] = auth
        request = urllib.request.Request(url, data=body, headers=hdrs, method=method)
        try:
            raw = self._opener.open(request, timeout=180)
        except urllib.error.URLError as exc:
            raise PackError(f"registry {method} failed") from exc
        try:
            payload = raw.read()
            www = raw.headers.get_all("WWW-Authenticate") or []
            mapped = {key.lower(): value for key, value in raw.headers.items()}
            if www:
                mapped["www-authenticate"] = ", ".join(www)
            status = getattr(raw, "status", None) or raw.getcode()
            final = getattr(raw, "url", None) or url
        finally:
            raw.close()
        if status in (301, 302, 303, 307, 308):
            raise PackError(f"registry {method} returned {status}")
        return _Response(status, mapped, payload, final)


def _descriptor(blob: Blob) -> dict[str, object]:
    return {"mediaType": blob.media_type, "digest": blob.digest, "size": len(blob.data)}


def _status(method: str, response: _Response) -> PackError:
    path = urllib.parse.urlsplit(response.url).path
    return PackError(f"registry {method} {path} returned {response.status}")


def _token_url(realm: str, service: str, scopes: list[str]) -> str:
    parts = urllib.parse.urlsplit(realm)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    if service:
        query.append(("service", service))
    for scope in scopes:
        query.append(("scope", scope))
    encoded = urllib.parse.urlencode(query)
    return urllib.parse.urlunsplit(parts._replace(query=encoded, fragment=""))


@overload
def _quote_digest(
    string: str,
    safe: str | bytes = "",
    encoding: str | None = None,
    errors: str | None = None,
) -> str: ...


@overload
def _quote_digest(
    string: bytes,
    safe: str | bytes = "",
    encoding: str | None = None,
    errors: str | None = None,
) -> str: ...


def _quote_digest(
    string: str | bytes,
    safe: str | bytes = "",
    encoding: str | None = None,
    errors: str | None = None,
) -> str:
    """``quote_plus`` that keeps ``:`` so an OCI digest stays ``sha256:<hex>``."""
    del safe
    text = string.decode("utf-8", "strict") if isinstance(string, bytes) else string
    return urllib.parse.quote_plus(text, safe=":", encoding=encoding, errors=errors)


def _with_digest(url: str, digest: str) -> str:
    parts = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    query.append(("digest", digest))
    encoded = urllib.parse.urlencode(query, quote_via=_quote_digest)
    return urllib.parse.urlunsplit(parts._replace(query=encoded))


def _token_from(response: _Response, fields: tuple[str, ...]) -> str:
    if response.status != 200:
        raise PackError(f"token endpoint returned {response.status}")
    try:
        payload = json.loads(response.body[:1_000_000])
    except json.JSONDecodeError as exc:
        raise PackError("token endpoint returned an empty token") from exc
    if not isinstance(payload, dict):
        raise PackError("token endpoint returned an empty token")
    for field in fields:
        token = payload.get(field)
        if isinstance(token, str) and token:
            return token
    raise PackError("token endpoint returned an empty token")


def _config_path() -> Path | None:
    directory = os.environ.get("DOCKER_CONFIG")
    if directory:
        path = Path(directory) / "config.json"
    else:
        path = Path.home() / ".docker" / "config.json"
    if not path.is_file():
        return None
    return path


def _helper_name(config: dict, address: str) -> str | None:
    helpers = config.get("credHelpers") or {}
    if helpers and not isinstance(helpers, dict):
        raise PackError("docker config cannot be read")
    if isinstance(helpers, dict):
        for key in (address, _hostname(address)):
            helper = helpers.get(key)
            if isinstance(helper, str) and helper:
                return helper
    store = config.get("credsStore") or ""
    if isinstance(store, str) and store:
        return store
    return None


def _helper_get(helper: str, address: str) -> Credential | None:
    if not _HELPER_NAME.fullmatch(helper):
        raise PackError("credential helper name is not allowed")
    try:
        proc = subprocess.run(
            [f"docker-credential-{helper}", "get"],
            input=address.encode("utf-8"),
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PackError("credential helper failed") from exc
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", "replace")
        if "credentials not found" in err:
            return None
        raise PackError("credential helper failed")
    if len(proc.stdout) > 1_000_000:
        raise PackError("credential helper failed")
    try:
        payload = json.loads(proc.stdout.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise PackError("credential helper failed") from exc
    if not isinstance(payload, dict):
        raise PackError("credential helper failed")
    username = payload.get("Username", "")
    secret = payload.get("Secret", "")
    if not isinstance(username, str) or not isinstance(secret, str):
        raise PackError("credential helper failed")
    if username == _TOKEN_USER:
        return Credential(refresh_token=secret) if secret else None
    if not username and not secret:
        return None
    return Credential(username=username, secret=secret)


def _auth_credential(config: dict, address: str) -> Credential | None:
    auths = config.get("auths") or {}
    if not isinstance(auths, dict):
        raise PackError("docker config cannot be read")
    entry = auths.get(address)
    if entry is None:
        host = _hostname(address)
        matches = [
            key for key in auths if _hostname(str(key)) == address or _hostname(str(key)) == host
        ]
        if matches:
            entry = auths[sorted(matches)[0]]
    if entry is None:
        return None
    if not isinstance(entry, dict):
        raise PackError("docker config cannot be read")
    return _credential_from_entry(entry)


def _credential_from_entry(entry: dict) -> Credential | None:
    username = entry.get("username") or ""
    secret = entry.get("password") or ""
    refresh = entry.get("identitytoken") or ""
    access = entry.get("registrytoken") or ""
    if not all(isinstance(item, str) for item in (username, secret, refresh, access)):
        raise PackError("docker config cannot be read")
    auth = entry.get("auth") or ""
    if auth:
        if not isinstance(auth, str):
            raise PackError("docker config cannot be read")
        username, secret = _decode_auth(auth)
    cred = Credential(username=username, secret=secret, refresh_token=refresh, access_token=access)
    return cred if cred else None


def _decode_auth(auth: str) -> tuple[str, str]:
    try:
        decoded = base64.b64decode(auth, validate=True)
        text = decoded.decode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise PackError("docker config auth field cannot be read") from exc
    username, sep, secret = text.partition(":")
    if not sep:
        raise PackError("docker config auth field cannot be read")
    return username, secret


def _hostname(addr: str) -> str:
    addr = addr.removeprefix("http://").removeprefix("https://")
    return addr.split("/", 1)[0]


def _parse_challenges(header: str) -> list[tuple[str, dict[str, str]]]:
    challenges: list[tuple[str, dict[str, str]]] = []
    scheme = ""
    params: dict[str, str] = {}
    for part in _split_commas(header):
        part = part.strip()
        if not part:
            continue
        key, sep, value = part.partition("=")
        if sep and " " not in key.strip():
            if scheme:
                params[key.strip().lower()] = _unquote(value)
            continue
        if scheme:
            challenges.append((scheme, params))
        bits = part.split(None, 1)
        scheme = bits[0].lower()
        params = {}
        if len(bits) == 2 and "=" in bits[1]:
            param, _, raw = bits[1].partition("=")
            params[param.strip().lower()] = _unquote(raw)
    if scheme:
        challenges.append((scheme, params))
    return challenges


def _choose(challenges: list[tuple[str, dict[str, str]]]) -> tuple[str, dict[str, str]]:
    for scheme in ("bearer", "basic"):
        for name, params in challenges:
            if name == scheme:
                return name, params
    raise PackError("registry authentication challenge is not supported")


def _split_commas(header: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    quoted = False
    escaped = False
    for char in header:
        if escaped:
            buf.append(char)
            escaped = False
            continue
        if char == "\\" and quoted:
            buf.append(char)
            escaped = True
            continue
        if char == '"':
            quoted = not quoted
            buf.append(char)
            continue
        if char == "," and not quoted:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(char)
    parts.append("".join(buf))
    return parts


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        inner = value[1:-1]
        return inner.replace('\\"', '"').replace("\\\\", "\\")
    return value

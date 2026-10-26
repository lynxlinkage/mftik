"""Download the pinned CPython and linux wheels into ``~/.cache/mftik/pack`` (E04-D075).

A blob is stored under its sha256. Dependency downloads are also indexed by
the interpreter, the platform, the requirements text, and the sha256 of any
local file the requirements name. pip is invoked as a subprocess; this module
does not import it (E04-D071, E04-D072).
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from mftik.pack.error import PackError

PLATFORM = "manylinux_2_28_x86_64"


def cache_dir() -> Path:
    path = Path.home() / ".cache" / "mftik" / "pack"
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_blob(url: str, sha256: str, *, cache: Path | None = None) -> bytes:
    """Return the bytes at ``url``, checking sha256. A mismatch is not cached."""
    if len(sha256) != 64 or any(char not in "0123456789abcdef" for char in sha256):
        raise PackError("download checksum is not sha256")
    directory = cache if cache is not None else cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    dest = directory / sha256
    if dest.is_file():
        cached = dest.read_bytes()
        if hashlib.sha256(cached).hexdigest() == sha256:
            return cached
        dest.unlink()
    request = urllib.request.Request(url, headers={"User-Agent": "mftik-pack"})
    try:
        with _open(request) as response:
            data = response.read()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PackError(f"download failed for {url}") from exc
    got = hashlib.sha256(data).hexdigest()
    if got != sha256:
        raise PackError(f"sha256 mismatch for {url}")
    temporary = directory / f".{sha256}.tmp"
    temporary.write_bytes(data)
    os.replace(temporary, dest)
    return data


def pip_download_command(python: str, dest: Path, requirements: Path) -> list[str]:
    """``pip download`` for manylinux wheels of CPython ``python`` (``3.12``)."""
    return _pip_command(python, dest) + ["-r", str(requirements)]


def requirements_are_empty(text: str) -> bool:
    for raw in text.splitlines():
        if raw.split("#", 1)[0].strip():
            return False
    return True


def require_wheels(paths: list[Path]) -> list[Path]:
    """Keep wheels. Anything else is an sdist, which is not compiled (E04-D072)."""
    if any(path.suffix != ".whl" for path in paths):
        raise PackError("sdist-only builds are not compiled")
    if not paths:
        raise PackError("pip download produced no wheels")
    return sorted(paths, key=lambda path: path.name)


def dependency_wheels(
    python: str,
    requirements: Path,
    *,
    cache: Path | None = None,
) -> list[tuple[str, bytes]]:
    """Wheels for ``requirements``, from the cache when the request is unchanged."""
    text = requirements.read_text(encoding="utf-8")
    if requirements_are_empty(text):
        return []
    directory = cache if cache is not None else cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    key = _request_key(python, text, _local_files(requirements, text))
    index = directory / f"idx-{key}"
    cached = _read_index(directory, index)
    if cached is not None:
        return cached
    import tempfile

    with tempfile.TemporaryDirectory(prefix="mftik-pack-pip-") as tmp:
        dest = Path(tmp)
        _run(pip_download_command(python, dest, requirements))
        wheels = require_wheels([path for path in dest.iterdir() if path.is_file()])
        stored = [_store(directory, path.name, path.read_bytes()) for path in wheels]
    _write_index(index, stored)
    return [(name, _read_cached(directory, digest)) for digest, name in stored]


def fetch_mftik_wheel(version: str, python: str, *, cache: Path | None = None) -> tuple[str, bytes]:
    """The ``mftik==version`` wheel from PyPI, without its dependencies."""
    directory = cache if cache is not None else cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    spec = f"mftik=={version}"
    key = _request_key(python, spec, [])
    index = directory / f"idx-{key}"
    cached = _read_index(directory, index)
    if cached is not None:
        if len(cached) != 1:
            raise PackError("cached mftik wheel is incomplete")
        return cached[0]
    import tempfile

    with tempfile.TemporaryDirectory(prefix="mftik-pack-pip-") as tmp:
        dest = Path(tmp)
        _run(_pip_command(python, dest) + ["--no-deps", spec])
        wheels = require_wheels([path for path in dest.iterdir() if path.is_file()])
        if len(wheels) != 1:
            raise PackError("pip download produced more than one mftik wheel")
        stored = _store(directory, wheels[0].name, wheels[0].read_bytes())
    _write_index(index, [stored])
    digest, name = stored
    return name, _read_cached(directory, digest)


def _pip_command(python: str, dest: Path) -> list[str]:
    abi = "cp" + python.replace(".", "")
    return [
        sys.executable,
        "-m",
        "pip",
        "download",
        "--disable-pip-version-check",
        "--no-input",
        "--no-cache-dir",
        "--only-binary=:all:",
        "--platform",
        PLATFORM,
        "--python-version",
        python,
        "--implementation",
        "cp",
        "--abi",
        abi,
        "--abi",
        "abi3",
        "--abi",
        "none",
        "--dest",
        str(dest),
    ]


def _run(cmd: list[str]) -> None:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PackError("pip download failed") from exc
    if proc.returncode == 0:
        return
    lines = [line.strip() for line in proc.stderr.splitlines() if line.strip()]
    tail = " ".join(lines[-4:])
    if len(tail) > 400:
        tail = tail[-400:]
    raise PackError(f"pip download failed: {tail}" if tail else "pip download failed")


def _request_key(python: str, text: str, files: list[tuple[str, str]]) -> str:
    payload = json.dumps(
        {"python": python, "platform": PLATFORM, "requirements": text, "files": files},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _local_files(requirements: Path, text: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    base = requirements.parent
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        tokens = line.split()
        candidates = tokens[1:] if tokens[0].startswith("-") else tokens[:1]
        for token in candidates:
            path = _as_file(token, base)
            if path is not None:
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                found.append((str(path), digest))
    found.sort()
    return found


def _as_file(token: str, base: Path) -> Path | None:
    if "://" in token:
        if not token.startswith("file:"):
            return None
        token = urllib.parse.unquote(urllib.parse.urlsplit(token).path)
    candidate = Path(token)
    options = [candidate] if candidate.is_absolute() else [base / candidate, Path.cwd() / candidate]
    for option in options:
        if option.is_file():
            return option.resolve()
    return None


def _store(directory: Path, name: str, blob: bytes) -> tuple[str, str]:
    digest = hashlib.sha256(blob).hexdigest()
    dest = directory / digest
    if not dest.is_file() or hashlib.sha256(dest.read_bytes()).hexdigest() != digest:
        temporary = directory / f".{digest}.tmp"
        temporary.write_bytes(blob)
        os.replace(temporary, dest)
    return digest, name


def _write_index(path: Path, stored: list[tuple[str, str]]) -> None:
    body = json.dumps(
        [{"sha256": digest, "name": name} for digest, name in stored],
        separators=(",", ":"),
    ).encode("ascii")
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(body)
    os.replace(temporary, path)


def _read_index(directory: Path, path: Path) -> list[tuple[str, bytes]] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, list):
        return None
    out: list[tuple[str, bytes]] = []
    for item in payload:
        if not isinstance(item, dict):
            return None
        digest = item.get("sha256")
        name = item.get("name")
        if not isinstance(digest, str) or not isinstance(name, str):
            return None
        blob_path = directory / digest
        if not blob_path.is_file():
            return None
        data = blob_path.read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            return None
        out.append((name, data))
    return out


def _read_cached(directory: Path, digest: str) -> bytes:
    return (directory / digest).read_bytes()


def _open(request: urllib.request.Request):
    host = urllib.parse.urlsplit(request.full_url).hostname
    if host in {"127.0.0.1", "localhost"}:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        return opener.open(request, timeout=180)
    return urllib.request.urlopen(request, timeout=180)

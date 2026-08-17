"""In-place stack updater. Holds the Docker socket; the API only triggers it.

Stdlib only — this image is docker:cli plus Python, not the trading one.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

COMPOSE_DIR = os.environ.get("MFTIK_COMPOSE_DIR", "/opt/mftik/deploy")
TOKEN = os.environ.get("MFTIK_UPDATER_TOKEN", "")
APP_IMAGE = os.environ.get(
    "MFTIK_APP_IMAGE", "ghcr.io/lynxlinkage/mftik"
)
LISTEN = ("0.0.0.0", int(os.environ.get("MFTIK_UPDATER_PORT", "8080")))
MD_READY_TIMEOUT_S = float(os.environ.get("MFTIK_MD_READY_TIMEOUT_S", "60"))
API_HEALTH_TIMEOUT_S = float(os.environ.get("MFTIK_API_HEALTH_TIMEOUT_S", "60"))

_lock = threading.Lock()
_status: dict[str, Any] = {
    "state": "idle",
    "step": "done",
    "from_version": None,
    "to_version": None,
    "feeds_published": 0,
    "feeds_total": 0,
    "error": None,
    "updated_at": time.time(),
}


def _set(**fields: Any) -> None:
    with _lock:
        _status.update(fields)
        _status["updated_at"] = time.time()


def snapshot() -> dict[str, Any]:
    with _lock:
        return dict(_status)


def current_version(env_path: str) -> str:
    for line in _read_lines(env_path):
        if line.startswith("MFTIK_VERSION="):
            return line.split("=", 1)[1].strip()
    return "latest"


def rewrite_version(env_path: str, version: str) -> None:
    """Replace only the version line. Secrets stay where they are."""
    lines = _read_lines(env_path)
    out: list[str] = []
    found = False
    for line in lines:
        if line.startswith("MFTIK_VERSION="):
            out.append(f"MFTIK_VERSION={version}\n")
            found = True
        else:
            out.append(line if line.endswith("\n") else line + "\n")
    if not found:
        out.append(f"MFTIK_VERSION={version}\n")
    with open(env_path, "w", encoding="utf-8") as fh:
        fh.writelines(out)


def pick_version(tags: list[str]) -> str:
    """Newest ``v*`` semver-ish tag, else ``latest`` if present, else first."""
    versions: list[tuple[tuple[int, ...], str]] = []
    for tag in tags:
        if not tag.startswith("v"):
            continue
        body = tag[1:]
        parts = re.split(r"[.-]", body)
        nums: list[int] = []
        ok = True
        for part in parts:
            if part.isdigit():
                nums.append(int(part))
            else:
                ok = False
                break
        if ok and nums:
            versions.append((tuple(nums), tag))
    if versions:
        versions.sort()
        return versions[-1][1]
    if "latest" in tags:
        return "latest"
    if tags:
        return tags[0]
    raise RuntimeError("GHCR returned no tags")


def _read_lines(path: str) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        return fh.readlines()


def _env_value(name: str, env_path: str) -> str:
    for line in _read_lines(env_path):
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get(name, "")


def _compose(*args: str) -> None:
    cmd = ["docker", "compose", *args]
    print("+", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=COMPOSE_DIR, check=True)


def _ghcr_tags(image: str) -> list[str]:
    """Public GHCR tag list via the anonymous pull token."""
    name = image.removeprefix("ghcr.io/")
    token_url = (
        f"https://ghcr.io/token?service=ghcr.io&scope=repository:{name}:pull"
    )
    with urllib.request.urlopen(token_url, timeout=30) as resp:
        token = json.loads(resp.read().decode())["token"]
    req = urllib.request.Request(
        f"https://ghcr.io/v2/{name}/tags/list",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode())
    tags = payload.get("tags") or []
    if not isinstance(tags, list):
        raise RuntimeError(f"unexpected GHCR tags payload: {payload!r}")
    return [str(tag) for tag in tags]


def _wait_http(url: str, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    last: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                if 200 <= resp.status < 300:
                    return
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = exc
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for {url}: {last}")


class _Redis:
    """Tiny RESP client. Enough for GET / SMEMBERS / DEL / AUTH."""

    def __init__(self, url: str) -> None:
        parsed = urlparse(url)
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or 6379
        self.password = parsed.password
        self.db = 0
        if parsed.path and parsed.path not in ("", "/"):
            self.db = int(parsed.path.lstrip("/").split("/")[0] or 0)

    def _session(self, *parts: str) -> Any:
        sock = socket.create_connection((self.host, self.port), timeout=5)
        try:
            def send(*args: str) -> None:
                chunks = [f"*{len(args)}\r\n".encode()]
                for part in args:
                    raw = part.encode()
                    chunks.append(f"${len(raw)}\r\n".encode() + raw + b"\r\n")
                sock.sendall(b"".join(chunks))

            def recv() -> Any:
                return _RespReader(sock).read()

            if self.password:
                send("AUTH", self.password)
                recv()
            if self.db:
                send("SELECT", str(self.db))
                recv()
            send(*parts)
            return recv()
        finally:
            sock.close()

    def get(self, key: str) -> str | None:
        value = self._session("GET", key)
        return None if value is None else str(value)

    def smembers(self, key: str) -> set[str]:
        value = self._session("SMEMBERS", key)
        if not value:
            return set()
        return {str(item) for item in value}

    def delete(self, *keys: str) -> None:
        if keys:
            self._session("DEL", *keys)


class _RespReader:
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = b""

    def _line(self) -> bytes:
        while b"\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise RuntimeError("redis closed")
            self._buf += chunk
        row, self._buf = self._buf.split(b"\r\n", 1)
        return row

    def read(self) -> Any:
        row = self._line()
        kind, rest = row[:1], row[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self._buf) < n + 2:
                chunk = self._sock.recv(4096)
                if not chunk:
                    raise RuntimeError("redis closed")
                self._buf += chunk
            data = self._buf[:n]
            self._buf = self._buf[n + 2 :]
            return data.decode()
        if kind == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise RuntimeError(f"bad redis reply {row!r}")


def _redis(env_path: str) -> _Redis:
    url = _env_value("REDIS_URL", env_path) or os.environ.get(
        "REDIS_URL", "redis://127.0.0.1:6379/0"
    )
    return _Redis(url)


def _prefix(env_path: str) -> str:
    return _env_value("BROKER_KEY_PREFIX", env_path) or "mftik"


def _wait_md_ready(env_path: str, role: str, timeout_s: float) -> None:
    redis = _redis(env_path)
    prefix = _prefix(env_path)
    ready = f"{prefix}:md:ready:{role}"
    published = f"{prefix}:md:published:{role}"
    pinned = f"{prefix}:md:pinned:{role}"
    deadline = time.time() + timeout_s
    last = "not checked"
    while time.time() < deadline:
        try:
            if redis.get(ready) == "1":
                members = redis.smembers(published)
                _set(feeds_published=len(members), feeds_total=len(members))
                return
            have = redis.smembers(published)
            want = redis.smembers(pinned)
            _set(feeds_published=len(have), feeds_total=len(want) or len(have))
            last = f"{len(have)}/{len(want) or '?'} feeds"
        except Exception as exc:
            last = str(exc)
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for md {role} ready ({last})")


def _wait_primary_covers_mirror(env_path: str, timeout_s: float) -> None:
    redis = _redis(env_path)
    prefix = _prefix(env_path)
    want = redis.smembers(f"{prefix}:md:pinned:mirror")
    if not want:
        return
    deadline = time.time() + timeout_s
    last = "not checked"
    while time.time() < deadline:
        try:
            have = redis.smembers(f"{prefix}:md:published:primary")
            _set(feeds_published=len(have & want), feeds_total=len(want))
            if want <= have:
                return
            last = f"{len(have & want)}/{len(want)} feeds"
        except Exception as exc:
            last = str(exc)
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for primary md to publish ({last})")


def _stop_quiet(service: str) -> None:
    try:
        _compose("--profile", "update", "stop", service)
    except Exception as exc:
        print(f"updater: failed to stop {service} after error: {exc}", flush=True)


def run_update() -> None:
    env_path = os.path.join(COMPOSE_DIR, ".env")
    from_version = current_version(env_path)
    api_next_up = False
    api_cutover = False
    md_next_up = False
    md_cutover = False
    _set(
        state="running",
        step="resolve",
        from_version=from_version,
        to_version=None,
        error=None,
        feeds_published=0,
        feeds_total=0,
    )
    try:
        tags = _ghcr_tags(APP_IMAGE)
        version = pick_version(tags)
        _set(to_version=version, step="pull")
        rewrite_version(env_path, version)
        # Same image as api_next / md_next; do not pull the local updater.
        _compose("pull", "api", "frontend", "sts", "td", "paper", "sym", "md")
        _set(step="migrate")
        _compose("--profile", "tools", "run", "--rm", "migrate")

        _set(step="api_next")
        _compose("--profile", "update", "up", "-d", "api_next")
        api_next_up = True
        _set(step="wait_api_next")
        _wait_http("http://api_next:8000/health", API_HEALTH_TIMEOUT_S)
        _set(step="recreate_api")
        _compose("up", "-d", "--no-deps", "api")
        _wait_http("http://api:8000/health", API_HEALTH_TIMEOUT_S)
        api_cutover = True
        _set(step="stop_api_next")
        _compose("--profile", "update", "stop", "api_next")
        api_next_up = False

        _set(step="md_next")
        _compose("--profile", "update", "up", "-d", "md_next")
        md_next_up = True
        _set(step="wait_md_next")
        _wait_md_ready(env_path, "mirror", MD_READY_TIMEOUT_S)
        _set(step="stop_md")
        _compose("stop", "md")
        md_cutover = True
        prefix = _prefix(env_path)
        _redis(env_path).delete(f"{prefix}:md:published:primary")

        _set(step="stop_sts")
        _compose("stop", "sts")
        _set(step="stop_td")
        _compose("stop", "td")
        _set(step="recreate")
        # Named services only. --remove-orphans would kill md_next (profile
        # update is off here), and listing api would bounce the one we just
        # cut over.
        _compose(
            "up",
            "-d",
            "--no-deps",
            "frontend",
            "sts",
            "td",
            "paper",
            "sym",
            "md",
        )
        _set(step="wait_md")
        _wait_primary_covers_mirror(env_path, MD_READY_TIMEOUT_S)
        _set(step="stop_md_next")
        _compose("--profile", "update", "stop", "md_next")
        md_next_up = False
        _set(state="idle", step="done", error=None)
    except Exception as exc:
        if api_next_up and not api_cutover:
            _stop_quiet("api_next")
        if md_next_up and not md_cutover:
            _stop_quiet("md_next")
        _set(state="failed", error=str(exc))
        raise


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"updater: {fmt % args}", flush=True)

    def _auth(self) -> bool:
        if not TOKEN:
            self._json(500, {"error": "MFTIK_UPDATER_TOKEN is not set"})
            return False
        header = self.headers.get("Authorization", "")
        if header != f"Bearer {TOKEN}":
            self._json(401, {"error": "unauthorized"})
            return False
        return True

    def _json(self, code: int, body: dict[str, Any]) -> None:
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") != "/status":
            self._json(404, {"error": "not found"})
            return
        if not self._auth():
            return
        self._json(200, snapshot())

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") != "/update":
            self._json(404, {"error": "not found"})
            return
        if not self._auth():
            return
        with _lock:
            if _status["state"] == "running":
                self._json(409, {"error": "an update is already running"})
                return
            # Claim the run before the thread starts, or GET /status can still
            # say idle while pull is already underway.
            _status["state"] = "running"
            _status["step"] = "resolve"
            _status["error"] = None
            _status["updated_at"] = time.time()
        thread = threading.Thread(target=_run_safe, name="mftik-update", daemon=True)
        thread.start()
        self._json(202, snapshot())


def _run_safe() -> None:
    try:
        run_update()
    except Exception as exc:
        print(f"updater failed: {exc}", flush=True)


def main() -> None:
    if not TOKEN:
        raise SystemExit("MFTIK_UPDATER_TOKEN must be set")
    server = ThreadingHTTPServer(LISTEN, Handler)
    print(f"updater listening on {LISTEN[0]}:{LISTEN[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

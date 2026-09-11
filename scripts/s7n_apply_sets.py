"""Register artifacts and apply the AssignmentSets of one deployment layer.

Two layers, two spec files, one code path:

    # planes — run by the release workflow on a final tag
    python3 scripts/s7n_apply_sets.py --spec deployment/sets/planes.json \
        --version v0.8.0 --tar /tmp/mftik.tar --wait-seconds 240

    # infra — run by hand, and rarely. NATS and Redis do not move with a tag.
    python3 scripts/s7n_apply_sets.py --spec deployment/sets/infra.json \
        --binary nats=/tmp/nats-server   --config nats=deployment/nats/nats.conf \
        --binary redis=/tmp/redis-server --config redis=deployment/redis/redis.conf

Sets rather than plain assignments, because per-member identity is what the
control plane expands for us: ``${member.name}`` becomes ``MFTIK_INSTANCE`` and
the WorkDir, and ``${member.vars.X}`` carries the site's NATS address. One set
per role rather than one set for every plane, because ``template.env`` is shared
by a set's members — a single set would have to hand ``REDIS_URL`` to STS, which
is the one thing the tape design forbids (docs/JetStreamRemoval.md).

Each set needs its own artifact family: ApplyAssignmentSet reserves both the
member slots and the family slot on every member's machine, so two sets sharing
a family collide on the second apply. That is why the tag registers one image
under five names instead of uploading it five times.

ApplyAssignmentSet replaces env entirely, so DATABASE_URL* must arrive through
the process environment (``secretKeysFromProdEnv``) and never live in the file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_URL = "https://s7n.lynkora.com/strategyplatform.v1.ControlPlaneService"
OCI = "ARTIFACT_TYPE_OCI_IMAGE"
BINARY = "ARTIFACT_TYPE_BINARY"

# The control plane serialises its own writes and occasionally loses a race with
# itself: applying eight assignments in a row has returned "deadlock detected
# (SQLSTATE 40P01)" mid-loop. Retrying the same call succeeds. An artifact whose
# ingest is still PENDING is the same shape of problem — true a moment later.
RETRY_ON = ("40P01", "deadlock", "is PENDING")
RETRIES = 4
RETRY_SLEEP_S = 3.0


class RpcError(RuntimeError):
    def __init__(self, method: str, code: int, detail: str) -> None:
        super().__init__(f"{method} HTTP {code}: {detail}")
        self.detail = detail


def rpc(base: str, token: str, method: str, body: dict[str, Any]) -> dict[str, Any]:
    """One Connect call, retried only for the transient failures named above."""
    last: RpcError | None = None
    for attempt in range(RETRIES):
        try:
            return _rpc_once(base, token, method, body)
        except RpcError as e:
            if not any(marker in e.detail for marker in RETRY_ON):
                raise SystemExit(str(e)) from e
            last = e
            print(f"  retry {attempt + 1}/{RETRIES}: {e}", flush=True)
            time.sleep(RETRY_SLEEP_S)
    raise SystemExit(str(last))


def _rpc_once(
    base: str, token: str, method: str, body: dict[str, Any]
) -> dict[str, Any]:
    req = urllib.request.Request(
        f"{base.rstrip('/')}/{method}",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Connect-Protocol-Version": "1",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raise RpcError(method, e.code, e.read().decode()) from e


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def put_file(put_url: str, path: Path) -> None:
    size = path.stat().st_size
    with path.open("rb") as f:
        req = urllib.request.Request(
            put_url,
            data=f,
            method="PUT",
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(size),
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                code = resp.status
        except urllib.error.HTTPError as e:
            raise SystemExit(f"PUT artifact HTTP {e.code}: {e.read().decode()}") from e
    if code not in (200, 201):
        raise SystemExit(f"PUT artifact HTTP {code}")


def upload(
    base: str,
    token: str,
    path: Path,
    *,
    names: list[str],
    version: str,
    kind: str,
) -> None:
    """Upload one file once, then answer to every name in ``names``.

    The upload is created under the first name because the object only has to
    live somewhere; RegisterArtifact then points each family at that same URI.
    Five plane families and one image is the whole reason this is split.
    """
    digest = sha256_file(path)
    print(f"{path.name} {digest} ({path.stat().st_size} bytes)", flush=True)
    created = rpc(
        base,
        token,
        "CreateArtifactUpload",
        {"name": names[0], "version": version, "digest": digest, "type": kind},
    )
    put_file(created["putUrl"], path)
    uri = created["s3Uri"]
    for name in names:
        rpc(
            base,
            token,
            "RegisterArtifact",
            {
                "artifact": {
                    "type": kind,
                    "name": name,
                    "version": version,
                    "digest": digest,
                    "uri": uri,
                }
            },
        )
        print(f"  registered {name} {version}", flush=True)


def resolve_env(spec: dict[str, Any], template: dict[str, Any]) -> dict[str, str]:
    """commonEnv, then the secrets from this process, then the set's own env.

    The set wins on a key collision: a role that needs a different value for a
    common key says so next to the rest of its configuration, rather than in a
    file-level block that reads as if it applied to everything.
    """
    env = {str(k): str(v) for k, v in (spec.get("commonEnv") or {}).items()}
    for key in spec.get("secretKeysFromProdEnv") or ():
        value = os.environ.get(key, "")
        if not value:
            raise SystemExit(f"missing environment variable {key}")
        env[key] = value
    env.update({str(k): str(v) for k, v in (template.get("env") or {}).items()})
    return env


def render_sets(spec: dict[str, Any], version: str | None) -> list[dict[str, Any]]:
    """Fold the file-level defaults into each set, and return what to apply."""
    out = []
    for entry in spec["sets"]:
        body = json.loads(json.dumps(entry))  # never mutate the parsed file
        s = body["spec"]
        template = s.setdefault("template", {})
        template["env"] = resolve_env(spec, template)
        for key in ("deployPolicy", "limits"):
            if key not in template and key in spec:
                template[key] = spec[key]
        if "update" not in s and "update" in spec:
            s["update"] = spec["update"]
        declared = s.get("artifactVersion") or spec.get("artifactVersion") or ""
        if version:
            s["artifactVersion"] = version
        elif declared and declared != "set-by-tag":
            s["artifactVersion"] = declared
        else:
            raise SystemExit(
                f"set {body['metadata']['name']}: artifactVersion is "
                f"{declared!r} and --version was not given"
            )
        out.append(body)
    return out


def apply_sets(base: str, token: str, sets: list[dict[str, Any]]) -> dict[str, str]:
    """Apply each set; return the generation the controller must converge on."""
    want: dict[str, str] = {}
    for body in sets:
        name = body["metadata"]["name"]
        out = rpc(base, token, "ApplyAssignmentSet", {"set": body})
        meta = (out.get("set") or {}).get("metadata") or {}
        want[name] = str(meta.get("generation") or "")
        members = [m["name"] for m in body["spec"]["members"]]
        print(
            f"applied set {name} gen={want[name]} "
            f"artifact={body['spec']['artifactVersion']} members={members}",
            flush=True,
        )
    return want


def wait_ready(
    base: str, token: str, want: dict[str, str], timeout: int
) -> None:
    """Block until every applied set is Ready at the generation we applied.

    Phase alone is not enough: a set that has not started rolling still reports
    the previous Ready, so the generation is what says the controller has seen
    this spec.
    """
    deadline = time.time() + timeout
    while True:
        live = {
            s["metadata"]["name"]: s
            for s in (rpc(base, token, "ListAssignmentSets", {}).get("sets") or [])
        }
        pending = []
        for name, generation in want.items():
            found = live.get(name)
            if found is None:
                pending.append(f"{name} (missing)")
                continue
            status = found.get("status") or {}
            observed = str(status.get("observedGeneration") or "")
            phase = status.get("phase") or "?"
            unready = [
                m.get("name")
                for m in (status.get("members") or [])
                if not m.get("ready")
            ]
            if phase == "Ready" and observed == generation and not unready:
                continue
            detail = f"{name} phase={phase} gen={observed}/{generation}"
            if unready:
                detail += f" unready={unready}"
            if status.get("message"):
                detail += f" msg={status['message']}"
            pending.append(detail)
        if not pending:
            print("all sets ready", flush=True)
            return
        for line in pending:
            print(f"wait {line}", flush=True)
        if time.time() >= deadline:
            raise SystemExit(f"timed out waiting for {', '.join(pending)}")
        time.sleep(10)


def pairs(values: list[str] | None, flag: str) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for raw in values or ():
        family, _, path = raw.partition("=")
        if not family or not path:
            raise SystemExit(f"{flag} expects FAMILY=PATH, got {raw!r}")
        p = Path(path)
        if not p.is_file():
            raise SystemExit(f"{flag} {family}: file not found: {p}")
        out[family] = p
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument(
        "--version",
        help="artifact version to apply; overrides the file's artifactVersion",
    )
    parser.add_argument(
        "--tar",
        type=Path,
        help="docker-save tar, registered as an OCI image under every set family",
    )
    parser.add_argument(
        "--binary",
        action="append",
        metavar="FAMILY=PATH",
        help="binary artifact for one family, at that set's artifactVersion",
    )
    parser.add_argument(
        "--config",
        action="append",
        metavar="FAMILY=PATH",
        help="config file for one family, at that set's configVersion",
    )
    parser.add_argument("--url", default=os.environ.get("STRATEGON_URL", DEFAULT_URL))
    parser.add_argument("--wait-seconds", type=int, default=240)
    args = parser.parse_args()

    token = os.environ.get("STRATEGON_API_KEY") or os.environ.get("S7N_TOKEN")
    if not token:
        raise SystemExit("STRATEGON_API_KEY is not set")

    spec = json.loads(args.spec.read_text())
    sets = render_sets(spec, args.version)
    by_family = {b["spec"]["strategy"]: b["spec"] for b in sets}

    if args.tar:
        if not args.tar.is_file():
            raise SystemExit(f"tar not found: {args.tar}")
        versions = {s["artifactVersion"] for s in by_family.values()}
        if len(versions) != 1:
            raise SystemExit(f"--tar needs one artifact version, got {versions}")
        upload(
            args.url,
            token,
            args.tar,
            names=sorted(by_family),
            version=versions.pop(),
            kind=OCI,
        )

    for family, path in pairs(args.binary, "--binary").items():
        if family not in by_family:
            raise SystemExit(f"--binary {family}: no set with that strategy")
        upload(
            args.url,
            token,
            path,
            names=[family],
            version=by_family[family]["artifactVersion"],
            kind=BINARY,
        )

    for family, path in pairs(args.config, "--config").items():
        if family not in by_family:
            raise SystemExit(f"--config {family}: no set with that strategy")
        version = by_family[family].get("configVersion")
        if not version:
            raise SystemExit(f"--config {family}: the set declares no configVersion")
        upload(
            args.url,
            token,
            path,
            names=[f"{family}-config"],
            version=version,
            kind=BINARY,
        )

    want = apply_sets(args.url, token, sets)
    if args.wait_seconds > 0:
        wait_ready(args.url, token, want, args.wait_seconds)


if __name__ == "__main__":
    sys.exit(main())

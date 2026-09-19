"""Strategon: apply a deployment layer, and manage what it depends on.

    # planes — run by the release workflow on a final tag
    python3 scripts/s7n.py apply deployment/sets/planes.json \
        --version v0.9.5 --tar /tmp/mftik.tar

    # the same tag again, or an older one: the image is already in the catalog
    python3 scripts/s7n.py apply deployment/sets/planes.json --version v0.9.4

    # infra — run by hand, and rarely. NATS and Redis do not move with a tag.
    python3 scripts/s7n.py apply deployment/sets/infra.json \
        --binary nats=/tmp/nats-server   --config nats=deployment/nats/nats.conf \
        --binary redis=/tmp/redis-server --config redis=deployment/redis/redis.conf

    # what a set's env will say, without touching anything
    python3 scripts/s7n.py apply deployment/sets/planes.json --version v0.9.5 --dry-run

    # secrets never live in a file or in argv; the value comes from env, a
    # file, or stdin
    python3 scripts/s7n.py secrets put mftik-database-url --from-env DATABASE_URL
    python3 scripts/s7n.py secrets list

    python3 scripts/s7n.py status
    python3 scripts/s7n.py volumes list

Sets rather than plain assignments, because per-member identity is what the
control plane expands for us: ``${member.name}`` becomes ``MFTIK_INSTANCE`` and
the WorkDir, and ``${member.vars.X}`` carries the site's NATS address. One set
per role, because ``template.env`` is shared by a set's members — a single set
would have to hand ``REDIS_URL`` to STS, which is the one thing the tape design
forbids (docs/JetStreamRemoval.md).

Secrets are ``secret.<name>`` tokens in the spec file. The control plane
resolves them when it writes the assignment, so the file is complete on its
own and safe to print — ``--dry-run`` shows exactly what will be applied.
``apply`` refuses a token the catalog does not have rather than letting the
member start and fail closed.

Volumes are declared per machine in the spec and created before the sets are
applied. A mount that names an undeclared volume is an error: the alternative
— creating whatever a template asks for — turns a typo into a plane booting on
an empty registry, which for STS is the one directory that cannot be rebuilt.
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
SECRET_PREFIX = "secret."

# The control plane serialises its own writes and occasionally loses a race with
# itself: applying several sets in a row has returned "deadlock detected
# (SQLSTATE 40P01)" mid-loop. Retrying the same call succeeds.
RETRY_ON = ("40P01", "deadlock")
RETRIES = 4
RETRY_SLEEP_S = 3.0
POLL_S = 10


class RpcError(RuntimeError):
    def __init__(self, method: str, code: int, detail: str) -> None:
        super().__init__(f"{method} HTTP {code}: {detail}")
        self.detail = detail


class Client:
    """One Connect endpoint and one token; every call goes through ``call``."""

    def __init__(self, base: str, token: str) -> None:
        self.base = base.rstrip("/")
        self.token = token

    def call(self, method: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        last: RpcError | None = None
        for attempt in range(RETRIES):
            try:
                return self._once(method, body or {})
            except RpcError as e:
                if not any(marker in e.detail for marker in RETRY_ON):
                    raise SystemExit(str(e)) from e
                last = e
                print(f"  retry {attempt + 1}/{RETRIES}: {e}", flush=True)
                time.sleep(RETRY_SLEEP_S)
        raise SystemExit(str(last))

    def _once(self, method: str, body: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(
            f"{self.base}/{method}",
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {self.token}",
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

    # -- read-side helpers, one per catalog ---------------------------------

    def machines(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        token = ""
        while True:
            page = self.call("ListMachines", {"pageSize": 100, "pageToken": token})
            for m in page.get("machines") or []:
                out[m["metadata"]["name"]] = m
            token = page.get("nextPageToken") or ""
            if not token:
                return out

    def secrets(self) -> set[str]:
        return {s["name"] for s in self.call("ListSecrets").get("secrets") or []}

    def volumes(self, machine: str) -> dict[str, dict[str, Any]]:
        out = self.call("ListVolumes", {"machineId": machine})
        return {v["name"]: v for v in out.get("volumes") or []}

    def artifacts(self, name: str) -> list[dict[str, Any]]:
        return self.call("ListArtifacts", {"name": name}).get("entries") or []

    def set(self, name: str) -> dict[str, Any] | None:
        try:
            return self._once("GetAssignmentSet", {"name": name})
        except RpcError as e:
            if e.code == 404 or "not_found" in e.detail or "NotFound" in e.detail:
                return None
            raise SystemExit(str(e)) from e


# -- artifacts --------------------------------------------------------------


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


def registered(cp: Client, name: str, version: str) -> dict[str, Any] | None:
    for entry in cp.artifacts(name):
        ref = entry.get("artifact") or {}
        if ref.get("version") == version:
            return entry
    return None


def upload(
    cp: Client, path: Path, *, names: list[str], version: str, kind: str
) -> None:
    """Upload one file once, then answer to every name in ``names``.

    A name that already has this version at this digest is left alone, so
    re-running a tag is a no-op here and a rollback to an older tag needs no
    tar at all. The same version at a different digest is refused: a version
    is content-addressed, and quietly re-pointing it is how two machines end
    up disagreeing about what ``v0.9.5`` is.
    """
    digest = sha256_file(path)
    print(f"{path.name} {digest} ({path.stat().st_size} bytes)", flush=True)
    todo = []
    for name in names:
        have = registered(cp, name, version)
        if have is None:
            todo.append(name)
            continue
        have_digest = (have.get("artifact") or {}).get("digest")
        if have_digest != digest:
            raise SystemExit(
                f"{name} {version} is already registered at {have_digest}; "
                f"this file is {digest}. Versions are immutable — tag a new one."
            )
        print(f"  {name} {version} already registered ({have.get('state')})")
    if not todo:
        return
    created = cp.call(
        "CreateArtifactUpload",
        {"name": todo[0], "version": version, "digest": digest, "type": kind},
    )
    put_file(created["putUrl"], path)
    uri = created["s3Uri"]
    for name in todo:
        cp.call(
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


def wait_artifacts(cp: Client, wanted: dict[str, str], timeout: int) -> None:
    """Every (name, version) must be in the catalog and past ingest.

    Checked before apply rather than retried through it: a set applied
    against a PENDING artifact used to bounce with an error string we had to
    pattern-match, and a version that was never registered would have rolled
    every member into a start failure.
    """
    deadline = time.time() + timeout
    while True:
        pending = []
        for name, version in sorted(wanted.items()):
            entry = registered(cp, name, version)
            if entry is None:
                raise SystemExit(
                    f"{name} {version} is not registered — pass --tar / --binary"
                )
            state = entry.get("state") or "READY"
            if state == "FAILED":
                raise SystemExit(
                    f"{name} {version} ingest failed: {entry.get('stateReason')}"
                )
            if state != "READY":
                pending.append(f"{name} {version} {state}")
        if not pending:
            return
        for line in pending:
            print(f"wait {line}", flush=True)
        if time.time() >= deadline:
            raise SystemExit(f"timed out waiting for {', '.join(pending)}")
        time.sleep(POLL_S)


# -- spec rendering --------------------------------------------------------------


def render_sets(spec: dict[str, Any], version: str | None) -> list[dict[str, Any]]:
    """Fold the file-level defaults into each set, and return what to apply.

    The set wins on every collision: a role that needs a different value for a
    common key says so next to the rest of its configuration, rather than in a
    file-level block that reads as if it applied to everything.
    """
    out = []
    for entry in spec["sets"]:
        body = json.loads(json.dumps(entry))  # never mutate the parsed file
        meta = body["metadata"]
        s = body["spec"]
        template = s.setdefault("template", {})

        labels = dict(spec.get("labels") or {})
        labels.update(meta.get("labels") or {})
        if labels:
            meta["labels"] = labels

        env = {str(k): str(v) for k, v in (spec.get("commonEnv") or {}).items()}
        env.update({str(k): str(v) for k, v in (template.get("env") or {}).items()})
        template["env"] = env

        for key in ("deployPolicy", "limits", "captureStdio"):
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
                f"set {meta['name']}: artifactVersion is {declared!r} "
                f"and --version was not given"
            )
        out.append(body)
    return out


def secret_refs(sets: list[dict[str, Any]]) -> dict[str, list[str]]:
    """``secret.<name>`` → the sets that reference it, from env and member vars."""
    refs: dict[str, list[str]] = {}

    def note(value: str, where: str) -> None:
        if value.startswith(SECRET_PREFIX):
            refs.setdefault(value[len(SECRET_PREFIX) :], []).append(where)

    for body in sets:
        name = body["metadata"]["name"]
        for v in body["spec"]["template"].get("env", {}).values():
            note(v, name)
        for m in body["spec"]["members"]:
            for v in (m.get("vars") or {}).values():
                note(v, f"{name}/{m['name']}")
    return refs


def volume_refs(sets: list[dict[str, Any]]) -> dict[tuple[str, str], list[str]]:
    """(machine, volume) → the members that mount it."""
    refs: dict[tuple[str, str], list[str]] = {}
    for body in sets:
        mounts = body["spec"]["template"].get("volumeMounts") or []
        for m in body["spec"]["members"]:
            for mount in mounts:
                refs.setdefault((m["machine"], mount["name"]), []).append(m["name"])
    return refs


# -- preflight --------------------------------------------------------------


def preflight(cp: Client, spec: dict[str, Any], sets: list[dict[str, Any]]) -> None:
    """Fail before the first ApplyAssignmentSet, with the thing that is missing.

    A set that rolls partway auto-rolls back per member, but only after each
    member has taken its turn failing to start — and a missing machine never
    fails, it waits until the deadline. Everything here is a read.
    """
    version = cp.call("GetControlPlaneVersion")
    print(f"control plane {version.get('version') or version}", flush=True)

    machines = cp.machines()
    problems = []
    for body in sets:
        for m in body["spec"]["members"]:
            found = machines.get(m["machine"])
            if found is None:
                problems.append(f"member {m['name']}: no machine {m['machine']!r}")
            elif not found.get("reachable"):
                problems.append(
                    f"member {m['name']}: machine {m['machine']} unreachable"
                )

    have = cp.secrets()
    for name, users in sorted(secret_refs(sets).items()):
        if name not in have:
            problems.append(
                f"secret {name!r} (used by {', '.join(users)}) is not in the "
                f"catalog — scripts/s7n.py secrets put {name}"
            )

    declared = {(v["machine"], v["name"]) for v in spec.get("volumes") or []}
    for (machine, name), users in sorted(volume_refs(sets).items()):
        if (machine, name) not in declared:
            problems.append(
                f"volume {name!r} on {machine} (mounted by {', '.join(users)}) "
                f"is not declared in the spec's volumes"
            )

    if problems:
        raise SystemExit("preflight:\n  " + "\n  ".join(problems))


def ensure_volumes(cp: Client, spec: dict[str, Any], *, dry_run: bool) -> None:
    by_machine: dict[str, dict[str, dict[str, Any]]] = {}
    for v in spec.get("volumes") or []:
        machine, name = v["machine"], v["name"]
        live = by_machine.setdefault(machine, cp.volumes(machine))
        found = live.get(name)
        if found is None:
            if dry_run:
                print(f"would create volume {name} on {machine}")
                continue
            cp.call("CreateVolume", {"machineId": machine, "name": name})
            print(f"created volume {name} on {machine}", flush=True)
        elif found.get("lastError"):
            raise SystemExit(f"volume {name} on {machine}: {found['lastError']}")
        else:
            print(
                f"volume {name} on {machine}: ready={found.get('ready')} "
                f"size={found.get('sizeBytes', '?')} "
                f"mountedBy={found.get('mountedBy') or []}"
            )


# -- apply --------------------------------------------------------------


def apply_sets(cp: Client, sets: list[dict[str, Any]]) -> dict[str, str]:
    """Apply each set; return the generation the controller must converge on."""
    want: dict[str, str] = {}
    for body in sets:
        name = body["metadata"]["name"]
        out = cp.call("ApplyAssignmentSet", {"set": body})
        meta = (out.get("set") or {}).get("metadata") or {}
        want[name] = str(meta.get("generation") or "")
        members = [m["name"] for m in body["spec"]["members"]]
        print(
            f"applied set {name} gen={want[name]} "
            f"artifact={body['spec']['artifactVersion']} members={members}",
            flush=True,
        )
    return want


def describe(found: dict[str, Any], generation: str) -> str | None:
    """None when the set is Ready at ``generation``; otherwise why not."""
    status = found.get("status") or {}
    observed = str(status.get("observedGeneration") or "")
    phase = status.get("phase") or "?"
    unready = [
        m.get("name") for m in (status.get("members") or []) if not m.get("ready")
    ]
    if phase == "Ready" and observed == generation and not unready:
        return None
    detail = f"phase={phase} gen={observed}/{generation}"
    if unready:
        detail += f" unready={unready}"
    if status.get("message"):
        detail += f" msg={status['message']}"
    return detail


def wait_ready(cp: Client, want: dict[str, str], timeout: int) -> None:
    """Block until every applied set is Ready at the generation we applied.

    Phase alone is not enough: a set that has not started rolling still reports
    the previous Ready, so the generation is what says the controller has seen
    this spec.
    """
    deadline = time.time() + timeout
    while True:
        pending = []
        for name, generation in want.items():
            found = cp.set(name)
            if found is None:
                pending.append(f"{name} (missing)")
                continue
            why = describe(found, generation)
            if why is None:
                continue
            if (found.get("status") or {}).get("phase") == "Failed":
                raise SystemExit(f"set {name} failed: {why}")
            pending.append(f"{name} {why}")
        if not pending:
            print("all sets ready", flush=True)
            return
        for line in pending:
            print(f"wait {line}", flush=True)
        if time.time() >= deadline:
            raise SystemExit(f"timed out waiting for {', '.join(pending)}")
        time.sleep(POLL_S)


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


def cmd_apply(cp: Client, args: argparse.Namespace) -> None:
    spec = json.loads(args.spec.read_text())
    sets = render_sets(spec, args.version)
    by_family = {b["spec"]["strategy"]: b["spec"] for b in sets}

    preflight(cp, spec, sets)
    ensure_volumes(cp, spec, dry_run=args.dry_run)

    if args.dry_run:
        for family, s in sorted(by_family.items()):
            have = registered(cp, family, s["artifactVersion"])
            state = have.get("state") if have else "not registered"
            print(f"artifact {family} {s['artifactVersion']}: {state}")
        print("would apply:")
        print(json.dumps(sets, indent=2))
        return

    if args.tar:
        if not args.tar.is_file():
            raise SystemExit(f"tar not found: {args.tar}")
        versions = {s["artifactVersion"] for s in by_family.values()}
        if len(versions) != 1:
            raise SystemExit(f"--tar needs one artifact version, got {versions}")
        upload(cp, args.tar, names=sorted(by_family), version=versions.pop(), kind=OCI)

    for family, path in pairs(args.binary, "--binary").items():
        if family not in by_family:
            raise SystemExit(f"--binary {family}: no set with that strategy")
        upload(
            cp,
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
        upload(cp, path, names=[f"{family}-config"], version=version, kind=BINARY)

    wanted = {f: s["artifactVersion"] for f, s in by_family.items()}
    for family, s in by_family.items():
        if s.get("configVersion"):
            wanted[f"{family}-config"] = s["configVersion"]
    wait_artifacts(cp, wanted, args.wait_seconds)

    want = apply_sets(cp, sets)
    if args.wait_seconds > 0:
        wait_ready(cp, want, args.wait_seconds)


# -- status --------------------------------------------------------------


def cmd_status(cp: Client, args: argparse.Namespace) -> None:
    sets = cp.call("ListAssignmentSets").get("sets") or []
    if args.names:
        sets = [s for s in sets if s["metadata"]["name"] in args.names]
    for s in sorted(sets, key=lambda s: s["metadata"]["name"]):
        meta, spec, status = s["metadata"], s.get("spec") or {}, s.get("status") or {}
        gen = str(meta.get("generation") or "")
        why = describe(s, gen)
        print(
            f"{meta['name']:<8} {spec.get('artifactVersion', '?'):<10} "
            f"{status.get('phase', '?'):<9} gen={gen} "
            f"key={status.get('assignmentKey') or 'family'}"
            + (f"  {why}" if why else "")
        )
        for m in status.get("members") or []:
            flag = "ok" if m.get("ready") else "--"
            print(
                f"  {flag} {m.get('name'):<10} {m.get('machine'):<6} "
                f"{m.get('phase', '')}"
            )


# -- secrets --------------------------------------------------------------


def read_secret_value(args: argparse.Namespace) -> str:
    """From env, a file, or stdin — never from argv, where ``ps`` can read it."""
    if args.from_env:
        value = os.environ.get(args.from_env, "")
        if not value:
            raise SystemExit(f"environment variable {args.from_env} is empty or unset")
        return value
    if args.from_file:
        return Path(args.from_file).read_text().rstrip("\n")
    if sys.stdin.isatty():
        raise SystemExit("pass --from-env, --from-file, or pipe the value on stdin")
    value = sys.stdin.read().rstrip("\n")
    if not value:
        raise SystemExit("empty value on stdin")
    return value


def cmd_secrets(cp: Client, args: argparse.Namespace) -> None:
    if args.action == "list":
        rows = cp.call("ListSecrets").get("secrets") or []
        for s in sorted(rows, key=lambda s: s["name"]):
            print(
                f"{s['token']:<40} {s.get('lengthBytes', '?'):>6} bytes  "
                f"key={s.get('keyId', '')}"
            )
        if not rows:
            print("(no secrets)")
    elif args.action == "put":
        if args.name.startswith(SECRET_PREFIX):
            bare = args.name[len(SECRET_PREFIX) :]
            raise SystemExit(f"name is the bare name, not the token: {bare}")
        value = read_secret_value(args)
        out = cp.call("PutSecret", {"name": args.name, "value": value})
        print(f"{out.get('token')} ({len(value.encode())} bytes)")
    elif args.action == "delete":
        cp.call("DeleteSecret", {"name": args.name})
        print(
            f"deleted {args.name} — a set still naming secret.{args.name} "
            f"fails closed on its next roll"
        )


# -- volumes --------------------------------------------------------------


def cmd_volumes(cp: Client, args: argparse.Namespace) -> None:
    machines = [args.machine] if args.machine else sorted(cp.machines())
    for machine in machines:
        for name, v in sorted(cp.volumes(machine).items()):
            print(
                f"{machine:<6} {name:<20} ready={v.get('ready')} "
                f"size={v.get('sizeBytes', '?')} mountedBy={v.get('mountedBy') or []} "
                f"pinnedBy={v.get('pinnedBy') or []}"
                + (f" error={v['lastError']}" if v.get("lastError") else "")
            )


# -- main --------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default=os.environ.get("STRATEGON_URL", DEFAULT_URL))
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("apply", help="register artifacts and apply one layer's sets")
    p.add_argument("spec", type=Path)
    p.add_argument(
        "--version", help="artifact version; overrides the file's artifactVersion"
    )
    p.add_argument(
        "--tar", type=Path, help="docker-save tar, registered under every set family"
    )
    p.add_argument("--binary", action="append", metavar="FAMILY=PATH")
    p.add_argument("--config", action="append", metavar="FAMILY=PATH")
    p.add_argument("--wait-seconds", type=int, default=240)
    p.add_argument(
        "--dry-run", action="store_true", help="preflight and print; apply nothing"
    )

    p = sub.add_parser("status", help="phase and members of every set")
    p.add_argument("names", nargs="*")

    p = sub.add_parser("secrets", help="the secret catalog")
    s = p.add_subparsers(dest="action", required=True)
    s.add_parser("list")
    q = s.add_parser("put")
    q.add_argument("name", help='bare name, e.g. "mftik-database-url"')
    q.add_argument("--from-env", metavar="VAR")
    q.add_argument("--from-file", metavar="PATH")
    q = s.add_parser("delete")
    q.add_argument("name")

    p = sub.add_parser("volumes", help="machine-level volumes")
    s = p.add_subparsers(dest="action", required=True)
    q = s.add_parser("list")
    q.add_argument("--machine")

    args = parser.parse_args()
    token = os.environ.get("STRATEGON_API_KEY") or os.environ.get("S7N_TOKEN")
    if not token:
        raise SystemExit("STRATEGON_API_KEY is not set")
    cp = Client(args.url, token)
    {
        "apply": cmd_apply,
        "status": cmd_status,
        "secrets": cmd_secrets,
        "volumes": cmd_volumes,
    }[args.command](cp, args)


if __name__ == "__main__":
    sys.exit(main())

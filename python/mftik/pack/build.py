"""Build a three-layer Python strategy image and push it (PROTOCOL §8.1)."""

from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

from mftik._version import package_version
from mftik.pack.archive import (
    Entry,
    check_dependency_wheel,
    check_mftik_wheel,
    compile_sources,
    compiler_binary,
    entries_from_tar,
    extract_tree,
    has_native_module,
    merge_entries,
    pack_entries,
    prefix_root,
    strategy_entries,
    wheel_entries,
)
from mftik.pack.error import PackError
from mftik.pack.fetch import dependency_wheels, ensure_blob, fetch_mftik_wheel
from mftik.pack.pins import CPYTHON, CPythonPin
from mftik.pack.registry import (
    Credential,
    Image,
    RegistryClient,
    canonical_json,
    connect_host,
    load_credential,
    make_image,
    split_repository,
)

_RESERVED = ("PYTHONPATH", "MFTIK_RELEASE", "PYTHONDONTWRITEBYTECODE")
_TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")


@dataclass(frozen=True)
class Options:
    """One ``python -m mftik.pack`` invocation."""

    directory: Path
    requirements: Path | None
    target: str
    name: str
    repository: str
    tag: str
    env: dict[str, str]
    plain_http: bool
    python: str
    wheel: Path | None


def parse_env(items: list[str]) -> dict[str, str]:
    """``K=V`` items. Platform variables and the packer's own keys are rejected."""
    env: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise PackError(f"env {item!r} is not K=V")
        key, value = item.split("=", 1)
        _check_env_item(key, value)
        if key in env:
            raise PackError(f"env {key} is set twice")
        if key in _RESERVED:
            raise PackError(f"env {key} is set by the packer")
        env[key] = value
    return env


def manifest_env(version: str, user: dict[str, str]) -> dict[str, str]:
    """Reserved keys first, then ``-env``.

    ``PYTHONDONTWRITEBYTECODE`` keeps the rootfs read-only.
    """
    env = {
        "PYTHONPATH": "app:site-packages",
        "MFTIK_RELEASE": version,
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    for key, value in user.items():
        _check_env_item(key, value)
        if key in env:
            raise PackError(f"env {key} is set by the packer")
        env[key] = value
    return env


def load_contracts() -> list[str]:
    """``Hello.contracts``, from the native module rather than a second copy."""
    try:
        from mftik._mftik import session_contracts
    except ImportError as exc:
        raise PackError("mftik._mftik is not installed") from exc
    names = [str(name) for name in session_contracts()]
    if not names:
        raise PackError("session contracts are empty")
    return names


def config_bytes(
    name: str,
    entrypoint: list[str],
    env: dict[str, str],
    version: str,
    contracts: list[str],
) -> bytes:
    ordered = {key: env[key] for key in sorted(env)}
    return canonical_json(
        {
            "schema": "mftiko.worker.v1",
            "kind": "strategy",
            "name": name,
            "runtime": "python",
            "os": "linux",
            "arch": "amd64",
            "entrypoint": entrypoint,
            "env": ordered,
            "sdk": {"lang": "python", "version": version},
            "contracts": list(contracts),
        }
    )


def build_image(options: Options) -> Image:
    """Pack ``options`` into an image. This does not push."""
    pin = _pin(options.python)
    _check_name(options.name)
    if options.tag:
        _check_tag(options.tag)
    split_repository(options.repository)
    if not options.directory.is_dir():
        raise PackError("strategy directory is not a directory")
    require_module(options.directory, options.target)
    if options.requirements is not None and not options.requirements.is_file():
        raise PackError("requirements file is not a file")
    version = package_version()
    contracts = load_contracts()
    minor = int(pin.python.split(".")[1])
    wheel_name, wheel_bytes = _sdk_wheel(options, version, pin.python)
    check_mftik_wheel(wheel_name, wheel_bytes, version, minor)
    runtime_blob = ensure_blob(pin.url, pin.sha256)
    sdk_root = f"python/lib/python{pin.python}/site-packages"
    sdk_entries = wheel_entries(wheel_name, wheel_bytes, sdk_root, "python/bin")
    if not has_native_module(sdk_entries):
        raise PackError("mftik wheel has no native module")
    dep_entries = _dependency_entries(options, minor)
    strategy = prefix_root(strategy_entries(options.directory), "app")
    compiled = _compile_groups(runtime_blob, pin, [sdk_entries, dep_entries, strategy])
    runtime = merge_entries(entries_from_tar(runtime_blob), sdk_entries, replace=False)
    runtime = merge_entries(runtime, compiled[0], replace=False)
    deps = merge_entries(dep_entries, compiled[1], replace=False)
    if not deps:
        deps = [Entry("site-packages", 0o755, "dir", b"", "")]
    app = merge_entries(strategy, compiled[2], replace=False)
    layers = [pack_entries(runtime), pack_entries(deps), pack_entries(app)]
    env = manifest_env(version, options.env)
    entrypoint = ["python/bin/python3", "-m", "mftik.run", options.target]
    _check_launch(entrypoint, env)
    return make_image(config_bytes(options.name, entrypoint, env, version, contracts), layers)


def push_image(image: Image, repository: str, tag: str, plain_http: bool) -> None:
    """Push ``image``. Credentials come from Docker's config.json."""
    host, name = split_repository(repository)
    credential: Credential | None = load_credential(connect_host(host))
    RegistryClient(host, name, plain_http, credential).push(image, tag)


def build_and_push(options: Options) -> str:
    """Build and push. The return value is the image manifest digest."""
    image = build_image(options)
    push_image(image, options.repository, options.tag, options.plain_http)
    return image.digest


def require_module(directory: Path, spec: str) -> None:
    """``module:Class`` whose top-level module file is in ``directory``."""
    module, sep, class_name = spec.partition(":")
    if sep != ":" or not module or not class_name or ":" in class_name:
        raise PackError(f"want module:Class, got {spec!r}")
    if not all(part.isidentifier() for part in module.split(".")):
        raise PackError(f"want module:Class, got {spec!r}")
    if not class_name.isidentifier():
        raise PackError(f"want module:Class, got {spec!r}")
    top = module.split(".", 1)[0]
    if (directory / f"{top}.py").is_file() or (directory / top / "__init__.py").is_file():
        return
    raise PackError(f"strategy directory has no module {top}")


def _pin(python: str) -> CPythonPin:
    pin = CPYTHON.get(python)
    if pin is None:
        raise PackError("python must be 3.11, 3.12, or 3.13")
    return pin


def _sdk_wheel(options: Options, version: str, python: str) -> tuple[str, bytes]:
    if options.wheel is None:
        return fetch_mftik_wheel(version, python)
    if not options.wheel.is_file():
        raise PackError("wheel file is not a file")
    return options.wheel.name, options.wheel.read_bytes()


def _dependency_entries(options: Options, minor: int) -> list[Entry]:
    if options.requirements is None:
        return []
    merged: list[Entry] = []
    wheels = dependency_wheels(options.python, options.requirements)
    for name, blob in wheels:
        check_dependency_wheel(name, blob, minor)
        merged = merge_entries(
            merged,
            wheel_entries(name, blob, "site-packages", "bin"),
            replace=True,
        )
    return merged


def _compile_groups(
    blob: bytes,
    pin: CPythonPin,
    groups: list[list[Entry]],
) -> list[list[Entry]]:
    indexed: list[tuple[int, str, bytes]] = []
    for index, group in enumerate(groups):
        for entry in group:
            if entry.kind == "file" and entry.path.endswith(".py"):
                indexed.append((index, entry.path, entry.data))
    compiled: list[list[Entry]] = [[] for _ in groups]
    if not indexed:
        return compiled
    with tempfile.TemporaryDirectory(prefix="mftik-pack-rt-") as tmp:
        root = Path(tmp)
        extract_tree(blob, root)
        python = compiler_binary(root, pin)
        produced = compile_sources(python, [(rel, data) for _, rel, data in indexed])
    if len(produced) != len(indexed):
        raise PackError("bytecode compile failed")
    for (index, _rel, _data), (pyc_rel, pyc) in zip(indexed, produced, strict=True):
        compiled[index].append(Entry(pyc_rel, 0o644, "file", pyc, ""))
    return compiled


def _check_name(name: str) -> None:
    if not name or any(ord(char) < 32 for char in name):
        raise PackError("name is empty")


def _check_tag(tag: str) -> None:
    if _TAG.fullmatch(tag) is None:
        raise PackError(f"tag {tag!r} is not allowed")


def _check_env_item(key: str, value: str) -> None:
    if key == "" or "=" in key or "\x00" in key or "\n" in key or "\r" in key:
        raise PackError("env name is not allowed")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise PackError("env value is not allowed")
    if key.startswith("MFTIKO_"):
        raise PackError(f"env sets {key}: MFTIKO_ variables are the platform's")


def _check_launch(entrypoint: list[str], env: dict[str, str]) -> None:
    if not entrypoint or not entrypoint[0]:
        raise PackError("no entrypoint")
    exe = entrypoint[0]
    if exe.startswith("/") or ".." in exe.split("/"):
        raise PackError("entrypoint is not a path inside the rootfs")
    for key in env:
        _check_env_item(key, env[key])

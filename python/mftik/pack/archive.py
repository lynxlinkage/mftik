"""Reproducible rootfs entries and wheel layout (PROTOCOL §8.1, §8.3).

Layers contain only regular files, directories, and relative symlinks that
stay inside the rootfs. python-build-standalone ships terminfo aliases whose
link text contains ``..``; those are stored as copies of the target file so
unpack accepts them. Hardlinks are stored as independent files. Bytes come
from the archive, not from a post-extract stat, so the digest does not follow
the host umask.
"""

from __future__ import annotations

import io
import os
import posixpath
import re
import stat
import subprocess
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from mftik.pack.error import PackError
from mftik.pack.pins import CPythonPin

_FILE = "file"
_DIR = "dir"
_LINK = "symlink"
_COMPILE = """
import importlib.util
import pathlib
import py_compile
import sys

root = pathlib.Path(sys.argv[1])
listing = pathlib.Path(sys.argv[2])
rels = [line for line in listing.read_text(encoding="utf-8").splitlines() if line]
for rel in rels:
    src = root / rel
    cfile = importlib.util.cache_from_source(str(src), optimization="")
    py_compile.compile(
        str(src),
        cfile=cfile,
        dfile=rel,
        doraise=True,
        optimize=0,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
        quiet=2,
    )
    print(pathlib.Path(cfile).relative_to(root).as_posix())
"""

_GLIBC_ALIAS = {
    "manylinux1_x86_64": (2, 5),
    "manylinux2010_x86_64": (2, 12),
    "manylinux2014_x86_64": (2, 17),
}


@dataclass(frozen=True)
class Entry:
    """One path inside a rootfs layer. ``data`` is empty for directories and symlinks."""

    path: str
    mode: int
    kind: str
    data: bytes = b""
    link: str = ""


def safe_rel(name: str) -> str:
    """The entry path inside the rootfs. Absolute paths, ``..``, and whiteouts are rejected."""
    if name == "" or "\x00" in name or "\n" in name or "\r" in name or "\\" in name:
        raise PackError(f"path not allowed: {name!r}")
    if name.startswith("/"):
        raise PackError(f"path not allowed: {name}")
    parts: list[str] = []
    for part in name.split("/"):
        if part in ("", "."):
            continue
        if part == ".." or part.startswith(".wh."):
            raise PackError(f"path not allowed: {name}")
        parts.append(part)
    if not parts:
        return "."
    return "/".join(parts)


def symlink_target_ok(target: str) -> bool:
    """True when unpack would accept ``target`` as a relative link inside the rootfs."""
    if target == "" or "\x00" in target or "\\" in target or target.startswith("/"):
        return False
    for part in target.split("/"):
        if part in ("", "."):
            continue
        if part == ".." or part.startswith(".wh."):
            return False
    return True


def pack_entries(entries: list[Entry]) -> bytes:
    """One gzip-compressed ustar layer. Parents are added, then every member is sorted by path."""
    members = _with_parents(entries)
    members.sort(key=lambda entry: entry.path)
    blob = io.BytesIO()
    with (
        gzip_open(blob) as gz,
        tarfile.open(fileobj=gz, mode="w", format=tarfile.USTAR_FORMAT) as tar,
    ):
        for entry in members:
            _add_member(tar, entry)
    return blob.getvalue()


def gzip_open(blob: io.BytesIO):
    """gzip with a fixed header: no name, mtime 0, compression level 6, OS byte 255."""
    import gzip

    return gzip.GzipFile(fileobj=blob, mode="wb", compresslevel=6, mtime=0)


def entries_from_tar(blob: bytes) -> list[Entry]:
    """Read a gzip tar. Unsafe symlinks and hardlinks become regular files."""
    files: dict[str, tuple[int, bytes]] = {}
    links: dict[str, str] = {}
    dirs: set[str] = set()
    hard: dict[str, str] = {}

    def drop(path: str) -> None:
        files.pop(path, None)
        links.pop(path, None)
        dirs.discard(path)
        hard.pop(path, None)

    with gzip_open_read(blob) as gz, tarfile.open(fileobj=gz, mode="r|") as tar:
        for member in tar:
            if member.mode & 0o6000:
                raise PackError(f"{member.name}: setuid or setgid")
            rel = safe_rel(member.name)
            if rel == ".":
                continue
            drop(rel)
            mode = member.mode & 0o777
            if member.isreg():
                handle = tar.extractfile(member)
                if handle is None:
                    raise PackError(f"{rel}: not a file")
                data = handle.read()
                if member.size >= 0 and len(data) != member.size:
                    raise PackError(f"{rel}: read {len(data)} bytes, header says {member.size}")
                files[rel] = (mode or 0o644, data)
            elif member.isdir():
                dirs.add(rel)
            elif member.issym():
                links[rel] = member.linkname
            elif member.islnk():
                hard[rel] = safe_rel(member.linkname)
            else:
                raise PackError(f"{member.name}: unsupported type")

    for path, target in hard.items():
        if target not in files:
            raise PackError(f"{path}: hardlink target is not a file")
        files[path] = files[target]

    entries = [Entry(path, mode, _FILE, data, "") for path, (mode, data) in files.items()]
    entries.extend(Entry(path, 0o755, _DIR, b"", "") for path in dirs if path not in files)
    for path, target in links.items():
        if path in files:
            continue
        if symlink_target_ok(target):
            entries.append(Entry(path, 0o777, _LINK, b"", target))
            continue
        mode, data = _materialize(path, target, links, files, set())
        entries.append(Entry(path, mode, _FILE, data, ""))
    return entries


def gzip_open_read(blob: bytes):
    import gzip

    return gzip.GzipFile(fileobj=io.BytesIO(blob), mode="rb")


def extract_tree(blob: bytes, dest: Path) -> None:
    """Extract for the compiler. Link text is kept, including ``..``, so the interpreter can run."""
    dest.mkdir(parents=True, exist_ok=True)
    with gzip_open_read(blob) as gz, tarfile.open(fileobj=gz, mode="r|") as tar:
        for member in tar:
            rel = safe_rel(member.name)
            if rel == ".":
                continue
            target = dest.joinpath(*rel.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_symlink() or target.exists():
                if target.is_dir() and not target.is_symlink():
                    continue
                target.unlink()
            if member.isdir():
                target.mkdir(exist_ok=True)
            elif member.issym():
                os.symlink(member.linkname, target)
            elif member.isreg():
                handle = tar.extractfile(member)
                if handle is None:
                    raise PackError(f"{rel}: not a file")
                target.write_bytes(handle.read())
                mode = member.mode & 0o777
                if mode:
                    os.chmod(target, mode)
            elif member.islnk():
                source = safe_rel(member.linkname)
                data = dest.joinpath(*source.split("/")).read_bytes()
                target.write_bytes(data)
                mode = member.mode & 0o777
                if mode:
                    os.chmod(target, mode)
            else:
                raise PackError(f"{member.name}: unsupported type")


def compiler_binary(root: Path, pin: CPythonPin) -> Path:
    """The pinned linux interpreter when it runs, else the host at that exact patch."""
    import sys

    candidate = root / "python" / "bin" / "python3"
    if _interpreter_runs(candidate):
        return candidate
    want = tuple(int(part) for part in pin.patch.split("."))
    if sys.version_info[:3] == want:
        return Path(sys.executable)
    raise PackError(f"cannot run the pinned CPython {pin.patch} to compile bytecode")


def compile_sources(python: Path, sources: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    """Compile each ``(rootfs path, source)`` to unchecked-hash pyc bytes, in order."""
    import tempfile

    if not sources:
        return []
    with tempfile.TemporaryDirectory(prefix="mftik-pack-pyc-") as tmp:
        stage = Path(tmp)
        listing = stage / "sources.txt"
        lines: list[str] = []
        for rel, data in sources:
            rel = safe_rel(rel)
            dest = stage.joinpath(*rel.split("/"))
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
            lines.append(rel)
        listing.write_text("\n".join(lines) + "\n", encoding="utf-8")
        try:
            proc = subprocess.run(
                [str(python), "-I", "-c", _COMPILE, str(stage), str(listing)],
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise PackError("bytecode compile failed") from exc
        if proc.returncode != 0:
            tail = " ".join(line for line in proc.stderr.splitlines() if line.strip())[-400:]
            detail = f": {tail}" if tail else ""
            raise PackError(f"bytecode compile failed{detail}")
        rels = [line for line in proc.stdout.splitlines() if line]
        if len(rels) != len(sources):
            raise PackError("bytecode compile failed")
        out: list[tuple[str, bytes]] = []
        for rel in rels:
            rel = safe_rel(rel)
            out.append((rel, stage.joinpath(*rel.split("/")).read_bytes()))
        return out


def strategy_entries(root: Path) -> list[Entry]:
    """The strategy tree. ``__pycache__`` and ``*.pyc`` are omitted.

    Executables are 0755, other files are 0644.
    """
    root = root.resolve()
    if not root.is_dir():
        raise PackError(f"strategy directory {root} is not a directory")
    entries: list[Entry] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        kept: list[str] = []
        for name in sorted(dirnames):
            if name == "__pycache__":
                continue
            path = Path(dirpath) / name
            if path.is_symlink():
                raise PackError(f"{_display(root, path)}: directory symlink is not allowed")
            kept.append(name)
        dirnames[:] = kept
        current = Path(dirpath)
        if current != root:
            rel = safe_rel(current.relative_to(root).as_posix())
            entries.append(Entry(rel, 0o755, _DIR, b"", ""))
        for name in sorted(filenames):
            if name.endswith(".pyc"):
                continue
            path = Path(dirpath) / name
            rel = safe_rel(path.relative_to(root).as_posix())
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode):
                link = os.readlink(path)
                if not symlink_target_ok(link):
                    raise PackError(f"{rel}: symlink is not inside the strategy")
                parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
                joined = posixpath.normpath(posixpath.join(parent, link) if parent else link)
                if joined.startswith(".."):
                    raise PackError(f"{rel}: symlink is not inside the strategy")
                entries.append(Entry(rel, 0o777, _LINK, b"", link))
            elif stat.S_ISREG(info.st_mode):
                mode = 0o755 if info.st_mode & 0o111 else 0o644
                entries.append(Entry(rel, mode, _FILE, path.read_bytes(), ""))
            else:
                raise PackError(f"{rel}: unsupported file")
    return entries


def prefix_root(entries: list[Entry], root: str) -> list[Entry]:
    """Place ``entries`` under ``root`` and include that directory."""
    root = safe_rel(root)
    prefixed = [Entry(root, 0o755, _DIR, b"", "")]
    for entry in entries:
        prefixed.append(
            Entry(f"{root}/{entry.path}", entry.mode, entry.kind, entry.data, entry.link)
        )
    return prefixed


def merge_entries(base: list[Entry], extra: list[Entry], *, replace: bool) -> list[Entry]:
    """Combine entries. A repeated path is an error unless ``replace`` keeps the later one."""
    merged: dict[str, Entry] = {}
    for entry in base:
        if entry.path in merged:
            raise PackError(f"duplicate path {entry.path}")
        merged[entry.path] = entry
    for entry in extra:
        if entry.path in merged and not replace:
            raise PackError(f"duplicate path {entry.path}")
        merged[entry.path] = entry
    return list(merged.values())


def wheel_entries(name: str, blob: bytes, root: str, scripts: str) -> list[Entry]:
    """Install a wheel under ``root``. ``.data/scripts`` land in ``scripts`` as mode 0755."""
    entries: list[Entry] = []
    try:
        zf = zipfile.ZipFile(io.BytesIO(blob))
    except zipfile.BadZipFile as exc:
        raise PackError(f"{name} is not a wheel") from exc
    with zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            mapped = _map_wheel_path(name, info.filename, root, scripts)
            if mapped is None:
                continue
            dest, is_script = mapped
            try:
                data = zf.read(info)
            except zipfile.BadZipFile as exc:
                raise PackError(f"{name} is not a wheel") from exc
            mode = 0o755 if is_script else _zip_mode(name, info)
            entries.append(Entry(dest, mode, _FILE, data, ""))
    return merge_entries(entries, [], replace=False)


def wheel_tags(name: str, blob: bytes) -> list[tuple[str, str, str]]:
    """Filename tag plus every ``Tag:`` line. Any one of them may make the wheel acceptable."""
    tags = list(_expand_tag(*_filename_tag(name)))
    text = _dist_info_text(name, blob, "WHEEL")
    if text is not None:
        for line in text.splitlines():
            if not line.startswith("Tag:"):
                continue
            raw = line.split(":", 1)[1].strip()
            tags.extend(_expand_tag(*_split_tag(raw)))
    if not tags:
        raise PackError(f"{name} has no wheel tag")
    return tags


def tag_compatible(python: str, abi: str, platform: str, minor: int, *, sdk: bool) -> bool:
    """Whether one tag may be unpacked for CPython 3.``minor`` on linux x86_64."""
    if sdk:
        platform_ok = platform == "linux_x86_64" or _glibc(platform) is not None
    else:
        glibc = _glibc(platform)
        platform_ok = platform == "any" or (glibc is not None and glibc <= (2, 28))
    if not platform_ok:
        return False
    if abi == "abi3":
        got = _minor_of(python)
        return got is not None and got <= minor
    if abi == "none":
        if python == "py3":
            return True
        got = _minor_of(python)
        return got is not None and got == minor and python.startswith("py")
    return abi == f"cp3{minor}" and python == f"cp3{minor}"


def check_dependency_wheel(name: str, blob: bytes, minor: int) -> None:
    """A dependency must be pure or manylinux x86_64 with glibc at most 2.28 (E04-D072)."""
    tags = wheel_tags(name, blob)
    if any(tag_compatible(*tag, minor, sdk=False) for tag in tags):
        return
    raise PackError(f"{name} is not a manylinux x86_64 wheel for CPython 3.{minor}")


def check_mftik_wheel(name: str, blob: bytes, version: str, minor: int) -> None:
    """The SDK wheel is this package's version and a linux x86_64 build of it."""
    meta = _dist_info_text(name, blob, "METADATA")
    if meta is None:
        raise PackError(f"{name} has no METADATA")
    dist = _meta_value(meta, "Name")
    got = _meta_value(meta, "Version")
    if dist.casefold() != "mftik":
        raise PackError(f"{name} is not the mftik wheel")
    if got != version:
        raise PackError(f"{name} version is {got}, installed mftik is {version}")
    tags = wheel_tags(name, blob)
    if any(tag_compatible(*tag, minor, sdk=True) for tag in tags):
        return
    raise PackError(f"{name} is not a linux x86_64 wheel for CPython 3.{minor}")


def has_native_module(entries: list[Entry]) -> bool:
    """True when the installed tree contains ``mftik/*.so``."""
    for entry in entries:
        if entry.kind != _FILE or not entry.path.endswith(".so"):
            continue
        marker = "/mftik/"
        if marker not in f"/{entry.path}":
            continue
        tail = f"/{entry.path}".split(marker, 1)[1]
        if "/" not in tail:
            return True
    return False


def _with_parents(entries: list[Entry]) -> list[Entry]:
    by_path: dict[str, Entry] = {}
    for entry in entries:
        if entry.mode & 0o6000:
            raise PackError(f"{entry.path}: setuid or setgid")
        if entry.path in by_path:
            raise PackError(f"duplicate path {entry.path}")
        by_path[entry.path] = entry
    for path in list(by_path):
        parent = path
        while "/" in parent:
            parent = parent.rsplit("/", 1)[0]
            existing = by_path.get(parent)
            if existing is not None:
                if existing.kind != _DIR:
                    raise PackError(f"duplicate path {parent}")
                continue
            by_path[parent] = Entry(parent, 0o755, _DIR, b"", "")
    return list(by_path.values())


def _add_member(tar: tarfile.TarFile, entry: Entry) -> None:
    info = tarfile.TarInfo(entry.path)
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.mode = entry.mode & 0o777
    try:
        if entry.kind == _DIR:
            info.type = tarfile.DIRTYPE
            tar.addfile(info)
        elif entry.kind == _LINK:
            if not symlink_target_ok(entry.link):
                raise PackError(f"{entry.path}: symlink is not inside the rootfs")
            info.type = tarfile.SYMTYPE
            info.linkname = entry.link
            tar.addfile(info)
        elif entry.kind == _FILE:
            info.type = tarfile.REGTYPE
            info.size = len(entry.data)
            tar.addfile(info, io.BytesIO(entry.data))
        else:
            raise PackError(f"{entry.path}: unsupported type")
    except (ValueError, tarfile.TarError) as exc:
        raise PackError(f"{entry.path}: {exc}") from exc


def _materialize(
    path: str,
    target: str,
    links: dict[str, str],
    files: dict[str, tuple[int, bytes]],
    seen: set[str],
) -> tuple[int, bytes]:
    if path in seen:
        raise PackError(f"symlink loop at {path}")
    if "\x00" in target or "\\" in target or target.startswith("/"):
        raise PackError(f"{path}: symlink target is not inside the rootfs")
    for part in target.split("/"):
        if part.startswith(".wh."):
            raise PackError(f"{path}: symlink target is not inside the rootfs")
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    joined = posixpath.normpath(posixpath.join(parent, target) if parent else target)
    if joined in ("", ".") or joined == ".." or joined.startswith("../"):
        raise PackError(f"{path}: symlink target is not inside the rootfs")
    if joined in files:
        return files[joined]
    if joined in links:
        return _materialize(joined, links[joined], links, files, seen | {path})
    raise PackError(f"{path}: symlink target is not a file")


def _map_wheel_path(wheel: str, filename: str, root: str, scripts: str) -> tuple[str, bool] | None:
    if "\\" in filename or "\x00" in filename:
        raise PackError(f"{wheel}: path not allowed")
    parts = filename.split("/")
    data_at = next((i for i, part in enumerate(parts) if part.endswith(".data")), None)
    if data_at is not None and data_at + 1 < len(parts):
        category = parts[data_at + 1]
        rest = [part for part in parts[data_at + 2 :] if part not in ("", ".")]
        if not rest:
            return None
        if category in ("purelib", "platlib"):
            dest = "/".join([root, *rest]) if root else "/".join(rest)
            return safe_rel(dest), False
        if category == "scripts":
            return safe_rel("/".join([scripts, *rest])), True
        raise PackError(f"{wheel}: data category {category} is not packed")
    rest = [part for part in parts if part not in ("", ".")]
    if not rest:
        return None
    dest = "/".join([root, *rest]) if root else "/".join(rest)
    return safe_rel(dest), False


def _zip_mode(wheel: str, info: zipfile.ZipInfo) -> int:
    mode = (info.external_attr >> 16) & 0o7777
    if mode & 0o6000:
        raise PackError(f"{wheel}: {info.filename}: setuid or setgid")
    return (mode & 0o777) or 0o644


def _filename_tag(name: str) -> tuple[str, str, str]:
    if not name.endswith(".whl"):
        raise PackError(f"{name} is not a wheel")
    parts = name[:-4].split("-")
    if len(parts) < 5:
        raise PackError(f"{name} has no tag")
    return parts[-3], parts[-2], parts[-1]


def _split_tag(tag: str) -> tuple[str, str, str]:
    parts = tag.split("-")
    if len(parts) != 3 or not all(parts):
        raise PackError(f"wheel tag {tag} is not python-abi-platform")
    return parts[0], parts[1], parts[2]


def _expand_tag(python: str, abi: str, platform: str) -> list[tuple[str, str, str]]:
    return [
        (py, ab, plat)
        for py in python.split(".")
        if py
        for ab in abi.split(".")
        if ab
        for plat in platform.split(".")
        if plat
    ]


def _glibc(platform: str) -> tuple[int, int] | None:
    if platform in _GLIBC_ALIAS:
        return _GLIBC_ALIAS[platform]
    match = re.fullmatch(r"manylinux_(\d+)_(\d+)_x86_64", platform)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def _minor_of(tag: str) -> int | None:
    match = re.fullmatch(r"(?:cp|py)3(\d+)", tag)
    if match is None:
        return None
    return int(match.group(1))


def _dist_info_text(wheel: str, blob: bytes, leaf: str) -> str | None:
    try:
        zf = zipfile.ZipFile(io.BytesIO(blob))
    except zipfile.BadZipFile as exc:
        raise PackError(f"{wheel} is not a wheel") from exc
    with zf:
        matches = [
            name
            for name in zf.namelist()
            if name.endswith(f".dist-info/{leaf}") and name.count("/") == 1
        ]
        if len(matches) != 1:
            return None
        return zf.read(matches[0]).decode("utf-8", "replace")


def _meta_value(text: str, key: str) -> str:
    want = key.casefold()
    for line in text.splitlines():
        if not line or line[0] in " \t":
            continue
        name, sep, value = line.partition(":")
        if sep and name.casefold() == want:
            return value.strip()
    raise PackError(f"wheel metadata has no {key}")


def _interpreter_runs(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        proc = subprocess.run(
            [str(path), "-I", "-c", "import py_compile"],
            capture_output=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _display(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name

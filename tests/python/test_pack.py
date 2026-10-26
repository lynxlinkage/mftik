"""Offline checks for ``python -m mftik.pack`` (E04-D071–E04-D075)."""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import os
import sys
import tarfile
import tempfile
import threading
import unittest
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from mftik._mftik import session_contracts
from mftik.pack.archive import (
    Entry,
    check_dependency_wheel,
    check_mftik_wheel,
    compile_sources,
    entries_from_tar,
    has_native_module,
    pack_entries,
    strategy_entries,
    tag_compatible,
    wheel_entries,
)
from mftik.pack.build import config_bytes, manifest_env, parse_env, require_module
from mftik.pack.cli import execute
from mftik.pack.error import PackError
from mftik.pack.fetch import (
    ensure_blob,
    pip_download_command,
    require_wheels,
    requirements_are_empty,
)
from mftik.pack.registry import (
    Credential,
    RegistryClient,
    connect_host,
    load_credential,
    make_image,
    server_address,
    split_repository,
)

REPO = "mftik/pack-hello"


def gz_tar(members: list[tuple[tarfile.TarInfo, bytes | None]]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for info, data in members:
            tar.addfile(info, io.BytesIO(data) if data is not None else None)
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb", compresslevel=6, mtime=0) as gz:
        gz.write(raw.getvalue())
    return out.getvalue()


def file_info(name: str, data: bytes, mode: int = 0o644) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.type = tarfile.REGTYPE
    info.size = len(data)
    info.mode = mode
    info.uid = 1
    info.gid = 2
    info.mtime = 17
    return info, data


def link_info(name: str, target: str, kind: bytes) -> tuple[tarfile.TarInfo, None]:
    info = tarfile.TarInfo(name)
    info.type = kind
    info.linkname = target
    info.mode = 0o777
    return info, None


def wheel_bytes(name: str, files: dict[str, bytes], tags: list[str]) -> bytes:
    payload = dict(files)
    payload[f"{name}.dist-info/METADATA"] = (
        f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0.0\n\n".encode()
    )
    tag_lines = "".join(f"Tag: {tag}\n" for tag in tags)
    payload[f"{name}.dist-info/WHEEL"] = (
        "Wheel-Version: 1.0\nGenerator: mftik-pack-test\nRoot-Is-Purelib: true\n" + tag_lines
    ).encode()
    blob = io.BytesIO()
    with zipfile.ZipFile(blob, "w") as zf:
        for path, data in payload.items():
            zf.writestr(path, data)
    return blob.getvalue()


class LayerTests(unittest.TestCase):
    def test_layer_is_stable_and_owned_by_root(self):
        entries = [Entry("app/a.txt", 0o644, "file", b"hi\n", "")]
        first = pack_entries(entries)
        second = pack_entries(entries)
        self.assertEqual(first, second)
        self.assertEqual(first[:2], b"\x1f\x8b")
        self.assertEqual(first[4:8], b"\x00\x00\x00\x00")
        self.assertEqual(first[8], 0)
        self.assertEqual(first[9], 255)
        with gzip.GzipFile(fileobj=io.BytesIO(first), mode="rb") as gz:
            with tarfile.open(fileobj=gz, mode="r|") as tar:
                members = list(tar)
        names = [member.name for member in members]
        self.assertEqual(names, ["app", "app/a.txt"])
        text = members[1]
        self.assertEqual(text.uid, 0)
        self.assertEqual(text.gid, 0)
        self.assertEqual(text.mtime, 0)
        self.assertEqual(text.uname, "")
        self.assertEqual(text.gname, "")

    def test_unsafe_symlink_and_hardlink_become_files(self):
        body = b"pack-mark\n"
        blob = gz_tar(
            [
                file_info("file", body, 0o664),
                file_info("bin/python3.12", b"py", 0o775),
                link_info("pkg/alias", "../file", tarfile.SYMTYPE),
                link_info("bin/python3", "python3.12", tarfile.SYMTYPE),
                link_info("bin/python", "bin/python3.12", tarfile.LNKTYPE),
            ]
        )
        entries = {entry.path: entry for entry in entries_from_tar(blob)}
        self.assertEqual(entries["pkg/alias"].kind, "file")
        self.assertEqual(entries["pkg/alias"].data, body)
        self.assertEqual(entries["pkg/alias"].mode, 0o664)
        self.assertEqual(entries["bin/python3"].kind, "symlink")
        self.assertEqual(entries["bin/python3"].link, "python3.12")
        self.assertEqual(entries["bin/python"].kind, "file")
        self.assertEqual(entries["bin/python"].data, b"py")

    def test_setuid_whiteout_and_absolute_paths_are_rejected(self):
        info, data = file_info("bin/tool", b"x", 0o4755)
        with self.assertRaises(PackError):
            entries_from_tar(gz_tar([(info, data)]))
        info, data = file_info(".wh.file", b"x")
        with self.assertRaises(PackError):
            entries_from_tar(gz_tar([(info, data)]))
        info, data = file_info("/etc/passwd", b"x")
        with self.assertRaises(PackError):
            entries_from_tar(gz_tar([(info, data)]))

    def test_strategy_tree_normalizes_mode_and_skips_pyc(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "hello.py").write_text("VALUE = 'pack-mark'\n", encoding="utf-8")
            cache = root / "__pycache__"
            cache.mkdir()
            (cache / "hello.cpython-312.pyc").write_bytes(b"pyc")
            nested = root / "pkg"
            nested.mkdir()
            tool = nested / "tool.sh"
            tool.write_text("echo pack-mark\n", encoding="utf-8")
            tool.chmod(0o755)
            link = root / "alias.py"
            os.symlink("hello.py", link)
            entries = {entry.path: entry for entry in strategy_entries(root)}
        self.assertEqual(entries["hello.py"].mode, 0o644)
        self.assertEqual(entries["pkg/tool.sh"].mode, 0o755)
        self.assertNotIn("__pycache__/hello.cpython-312.pyc", entries)
        self.assertEqual(entries["alias.py"].kind, "symlink")
        self.assertEqual(entries["alias.py"].link, "hello.py")

    def test_unchecked_hash_pyc_is_stable(self):
        source = b"VALUE = 'pack-mark'\n"
        first = compile_sources(Path(sys.executable), [("app/hello.py", source)])
        second = compile_sources(Path(sys.executable), [("app/hello.py", source)])
        self.assertEqual(first, second)
        rel, data = first[0]
        self.assertIn("__pycache__", rel)
        flags = int.from_bytes(data[4:8], "little")
        self.assertEqual(flags & 0b11, 0b1)


class WheelTests(unittest.TestCase):
    def test_dependency_tags(self):
        minor = 12
        self.assertTrue(tag_compatible("py3", "none", "any", minor, sdk=False))
        self.assertTrue(tag_compatible("cp312", "cp312", "manylinux_2_28_x86_64", minor, sdk=False))
        self.assertTrue(tag_compatible("cp312", "abi3", "manylinux2014_x86_64", minor, sdk=False))
        self.assertFalse(
            tag_compatible("cp312", "cp312", "manylinux_2_34_x86_64", minor, sdk=False)
        )
        self.assertFalse(tag_compatible("cp312", "cp312", "linux_x86_64", minor, sdk=False))
        self.assertFalse(tag_compatible("cp312", "cp312", "macosx_11_0_arm64", minor, sdk=False))
        self.assertTrue(tag_compatible("cp311", "abi3", "manylinux_2_34_x86_64", minor, sdk=True))
        self.assertTrue(tag_compatible("cp312", "cp312", "linux_x86_64", minor, sdk=True))
        self.assertFalse(tag_compatible("py3", "none", "any", minor, sdk=True))

    def test_wheel_layout_and_rejection(self):
        blob = wheel_bytes(
            "packsample",
            {
                "packsample/__init__.py": b"def marker():\n    return 'pack-dep'\n",
                "packsample-1.0.0.data/scripts/demo": b"echo pack-mark\n",
            },
            ["py3-none-any"],
        )
        # The filename carries the tag the checker reads.
        filename = "packsample-1.0.0-py3-none-any.whl"
        check_dependency_wheel(filename, blob, 12)
        entries = {
            entry.path: entry for entry in wheel_entries(filename, blob, "site-packages", "bin")
        }
        self.assertEqual(entries["bin/demo"].mode, 0o755)
        self.assertIn("site-packages/packsample/__init__.py", entries)
        slipped = wheel_bytes("packsample", {"../outside.py": b"x"}, ["py3-none-any"])
        with self.assertRaises(PackError):
            wheel_entries(filename, slipped, "site-packages", "bin")
        headers = wheel_bytes(
            "packsample",
            {"packsample-1.0.0.data/headers/demo.h": b"int x;\n"},
            ["py3-none-any"],
        )
        with self.assertRaises(PackError):
            wheel_entries(filename, headers, "site-packages", "bin")

    def test_mftik_wheel_needs_the_native_module_and_version(self):
        filename = "mftik-1.0.0-cp311-abi3-manylinux_2_34_x86_64.whl"
        blob = wheel_bytes(
            "mftik",
            {"mftik/__init__.py": b"", "mftik/_mftik.abi3.so": b"so"},
            ["cp311-abi3-manylinux_2_34_x86_64"],
        )
        # wheel_bytes writes Version: 1.0.0 and Name from the dist name.
        check_mftik_wheel(filename, blob, "1.0.0", 12)
        entries = wheel_entries(filename, blob, "python/lib/python3.12/site-packages", "python/bin")
        self.assertTrue(has_native_module(entries))
        with self.assertRaises(PackError):
            check_mftik_wheel(filename, blob, "0.14.0", 12)
        with self.assertRaises(PackError):
            check_dependency_wheel(filename, blob, 12)


class ManifestTests(unittest.TestCase):
    def test_env_rejection_and_stable_config(self):
        with self.assertRaises(PackError):
            parse_env(["MFTIKO_CTL_FD=3"])
        with self.assertRaises(PackError):
            parse_env(["PYTHONPATH=other"])
        with self.assertRaises(PackError):
            parse_env(["=value"])
        with self.assertRaises(PackError):
            parse_env(["A=B", "A=C"])
        with self.assertRaises(PackError):
            parse_env(["BAD\nKEY=1"])
        env = manifest_env("0.14.0", parse_env(["PACK_MARK=pack-mark"]))
        blob = config_bytes(
            "pack-hello",
            ["python/bin/python3", "-m", "mftik.run", "hello:Hello"],
            env,
            "0.14.0",
            ["worker.v1", "stsctl.v1", "md.v1", "td.v1"],
        )
        self.assertEqual(
            blob,
            config_bytes(
                "pack-hello",
                ["python/bin/python3", "-m", "mftik.run", "hello:Hello"],
                env,
                "0.14.0",
                ["worker.v1", "stsctl.v1", "md.v1", "td.v1"],
            ),
        )
        text = blob.decode("ascii")
        positions = [
            text.index(key)
            for key in (
                "MFTIK_RELEASE",
                "PACK_MARK",
                "PYTHONDONTWRITEBYTECODE",
                "PYTHONPATH",
            )
        ]
        self.assertEqual(positions, sorted(positions))
        image = make_image(blob, [b"one", b"two", b"three"])
        self.assertEqual(image.digest, make_image(blob, [b"one", b"two", b"three"]).digest)
        self.assertTrue(image.digest.startswith("sha256:"))
        parsed = json.loads(image.manifest)
        self.assertEqual(parsed["schemaVersion"], 2)
        self.assertEqual(len(parsed["layers"]), 3)
        self.assertNotIn("annotations", parsed)
        self.assertNotIn("manifests", parsed)

    def test_module_spec_and_cli_usage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "hello.py").write_text("class Hello:\n    pass\n", encoding="utf-8")
            require_module(root, "hello:Hello")
            with self.assertRaises(PackError):
                require_module(root, "missing:Hello")
            with self.assertRaises(PackError):
                require_module(root, "hello")
        with self.assertRaises(PackError):
            execute([])

    def test_no_push_is_accepted_before_the_directory_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = str(Path(tmp) / "absent")
            with self.assertRaises(PackError) as caught:
                execute(
                    [
                        "-dir",
                        missing,
                        "-target",
                        "hello:Hello",
                        "-name",
                        "hello",
                        "-repo",
                        "127.0.0.1:9/mftik/hello",
                        "-no-push",
                    ]
                )
        self.assertIn("not a directory", str(caught.exception))

    def test_session_contracts_match_hello(self):
        self.assertEqual(
            session_contracts(),
            ["worker.v1", "stsctl.v1", "md.v1", "td.v1"],
        )


class FetchTests(unittest.TestCase):
    def test_pip_command_and_sdist_rejection(self):
        cmd = pip_download_command("3.12", Path("/tmp/pack-dest"), Path("/tmp/pack-req.txt"))
        self.assertIn("--only-binary=:all:", cmd)
        self.assertIn("--platform", cmd)
        self.assertEqual(cmd[cmd.index("--platform") + 1], "manylinux_2_28_x86_64")
        self.assertEqual(cmd[cmd.index("--python-version") + 1], "3.12")
        self.assertEqual(cmd.count("--abi"), 3)
        self.assertTrue(requirements_are_empty("# comment\n\n"))
        self.assertFalse(requirements_are_empty("packsample\n"))
        with tempfile.TemporaryDirectory() as tmp:
            sdist = Path(tmp) / "packsample-1.0.0.tar.gz"
            sdist.write_bytes(b"not-a-wheel")
            with self.assertRaises(PackError) as caught:
                require_wheels([sdist])
        self.assertIn("sdist-only", str(caught.exception))

    def test_checksum_mismatch_is_not_cached(self):
        body = b"not-the-bytes"
        good = b"pack-bytes"
        digest = hashlib.sha256(good).hexdigest()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                raw = body
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, fmt, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(PackError) as caught:
                    ensure_blob(f"http://127.0.0.1:{port}/blob", digest, cache=Path(tmp))
                self.assertIn("sha256 mismatch", str(caught.exception))
                self.assertEqual(list(Path(tmp).iterdir()), [])
        finally:
            server.shutdown()
            server.server_close()


class _Box:
    def __init__(self, mode: str, basic: str = ""):
        self.mode = mode
        self.basic = basic
        self.blobs: dict[str, bytes] = {}
        self.manifests: dict[str, bytes] = {}
        self.seen: list[tuple[str, str, str, bytes]] = []
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.httpd.box = self  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return int(self.httpd.server_address[1])

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):  # noqa: N802
        self._handle()

    def do_HEAD(self):  # noqa: N802
        self._handle()

    def do_POST(self):  # noqa: N802
        self._handle()

    def do_PUT(self):  # noqa: N802
        self._handle()

    def log_message(self, fmt, *args):
        return

    def _handle(self) -> None:
        box: _Box = self.server.box  # type: ignore[attr-defined]
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        auth = self.headers.get("Authorization", "")
        path = self.path.split("?", 1)[0]
        box.seen.append((self.command, path, auth, body))
        if box.mode == "redirect":
            self.send_response(302)
            self.send_header("Location", box.basic)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path == "/token":
            self._token(box, body)
            return
        if not self._allowed(box, auth):
            self._challenge(box)
            return
        if self.command == "HEAD" and "/blobs/sha256:" in path:
            digest = "sha256:" + path.rsplit("sha256:", 1)[1]
            if digest in box.blobs:
                self._empty(200)
            else:
                self._empty(404)
            return
        if self.command == "POST" and path.endswith("/blobs/uploads/"):
            self.send_response(202)
            loc = path[: -len("/blobs/uploads/")] + "/blobs/uploads/upload-1"
            self.send_header("Location", loc)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.command == "PUT" and "/blobs/uploads/" in path:
            query = self.path.split("?", 1)[1] if "?" in self.path else ""
            digest = ""
            for part in query.split("&"):
                if part.startswith("digest="):
                    digest = part.split("=", 1)[1]
            box.blobs[digest] = body
            self.send_response(201)
            self.send_header("Docker-Content-Digest", digest)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.command == "PUT" and "/manifests/" in path:
            ref = path.rsplit("/", 1)[1]
            box.manifests[ref] = body
            self._empty(201)
            return
        self._empty(404)

    def _token(self, box: _Box, body: bytes) -> None:
        if box.basic and self.headers.get("Authorization", "") != box.basic:
            if self.command != "POST":
                self._empty(401)
                return
        if self.command == "POST":
            if b"client_id=mftik-pack" not in body or b"grant_type=refresh_token" not in body:
                self._empty(401)
                return
            payload = b'{"access_token":"registry-token"}'
        else:
            payload = b'{"token":"registry-token"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _allowed(self, box: _Box, auth: str) -> bool:
        if box.mode == "open":
            return True
        if box.mode == "basic":
            return auth == box.basic
        if box.mode == "bearer":
            return auth == "Bearer registry-token"
        return False

    def _challenge(self, box: _Box) -> None:
        self.send_response(401)
        if box.mode == "basic":
            self.send_header("WWW-Authenticate", 'Basic realm="registry"')
        else:
            realm = f"http://127.0.0.1:{box.port}/token"
            scope = f"repository:{REPO}:pull,push"
            header = f'Bearer realm="{realm}",service="registry",scope="{scope}"'
            self.send_header("WWW-Authenticate", header)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()


class RegistryTests(unittest.TestCase):
    def test_repository_and_docker_hub_host(self):
        split = split_repository("127.0.0.1:5000/mftik/pack-hello")
        self.assertEqual(split, ("127.0.0.1:5000", REPO))
        self.assertEqual(connect_host("docker.io"), "registry-1.docker.io")
        self.assertEqual(server_address("registry-1.docker.io"), "https://index.docker.io/v1/")
        with self.assertRaises(PackError):
            split_repository("no-slash")

    def _push(self, box: _Box, credential: Credential | None = None, tag: str = "pack") -> None:
        image = make_image(b'{"schema":"mftiko.worker.v1"}', [b"layer-one", b"layer-two"])
        RegistryClient(f"127.0.0.1:{box.port}", REPO, True, credential).push(image, tag)
        self.assertEqual(box.manifests[tag], image.manifest)

    def test_anonymous_push_then_the_same_blobs_are_not_uploaded_again(self):
        box = _Box("open")
        try:
            self._push(box)
            puts = [item for item in box.seen if item[0] == "PUT" and "/blobs/" in item[1]]
            self.assertEqual(len(puts), 3)
            self._push(box)
            puts = [item for item in box.seen if item[0] == "PUT" and "/blobs/" in item[1]]
            self.assertEqual(len(puts), 3)
        finally:
            box.close()

    def test_basic_and_bearer_and_refresh(self):
        basic = "Basic " + base64.b64encode(b"registry-user:basic-value").decode("ascii")
        box = _Box("basic", basic)
        try:
            self._push(box, Credential(username="registry-user", secret="basic-value"))
            auths = [item[2] for item in box.seen]
            self.assertIn(basic, auths)
            self.assertNotIn("basic-value", box.seen[0][2])
        finally:
            box.close()

        box = _Box("bearer", basic)
        try:
            self._push(box, Credential(username="registry-user", secret="basic-value"))
            token = [item for item in box.seen if item[1] == "/token"]
            self.assertTrue(token)
            self.assertEqual(token[0][2], basic)
            puts = [item for item in box.seen if item[0] == "PUT"]
            self.assertTrue(all(item[2] == "Bearer registry-token" for item in puts))
        finally:
            box.close()

        box = _Box("bearer")
        try:
            self._push(box, None)
            token = [item for item in box.seen if item[1] == "/token"]
            self.assertEqual(token[0][2], "")
        finally:
            box.close()

        box = _Box("bearer")
        try:
            self._push(box, Credential(access_token="registry-token"))
            self.assertFalse([item for item in box.seen if item[1] == "/token"])
        finally:
            box.close()

        box = _Box("bearer")
        try:
            self._push(box, Credential(refresh_token="refresh-value"))
            posts = [item for item in box.seen if item[1] == "/token" and item[0] == "POST"]
            self.assertEqual(len(posts), 1)
            self.assertIn(b"client_id=mftik-pack", posts[0][3])
        finally:
            box.close()

    def test_redirect_is_not_followed(self):
        other = _Box("open")
        box = _Box("redirect", f"http://127.0.0.1:{other.port}/v2/followed")
        try:
            with self.assertRaises(PackError) as caught:
                self._push(box)
            self.assertIn("302", str(caught.exception))
            self.assertEqual(other.seen, [])
            self.assertNotIn("basic-value", str(caught.exception))
        finally:
            box.close()
            other.close()


class DockerConfigTests(unittest.TestCase):
    def setUp(self):
        self._env = os.environ.get("DOCKER_CONFIG")
        self._path = os.environ.get("PATH")

    def tearDown(self):
        if self._env is None:
            os.environ.pop("DOCKER_CONFIG", None)
        else:
            os.environ["DOCKER_CONFIG"] = self._env
        if self._path is not None:
            os.environ["PATH"] = self._path

    def test_missing_file_is_anonymous_and_auth_field_is_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["DOCKER_CONFIG"] = tmp
            self.assertIsNone(load_credential("127.0.0.1:9"))
            encoded = base64.b64encode(b"registry-user:basic-value").decode("ascii")
            Path(tmp, "config.json").write_text(
                json.dumps({"auths": {"127.0.0.1:9": {"auth": encoded}}}),
                encoding="utf-8",
            )
            cred = load_credential("127.0.0.1:9")
            self.assertIsNotNone(cred)
            assert cred is not None
            self.assertEqual(cred.username, "registry-user")
            self.assertEqual(cred.secret, "basic-value")

    def test_legacy_host_and_docker_hub_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["DOCKER_CONFIG"] = tmp
            encoded = base64.b64encode(b"registry-user:basic-value").decode("ascii")
            Path(tmp, "config.json").write_text(
                json.dumps(
                    {
                        "auths": {
                            "http://127.0.0.1:9/v2/": {"auth": encoded},
                            "https://index.docker.io/v1/": {"auth": encoded},
                        }
                    }
                ),
                encoding="utf-8",
            )
            local = load_credential("127.0.0.1:9")
            hub = load_credential("registry-1.docker.io")
            assert local is not None and hub is not None
            self.assertEqual(local.username, "registry-user")
            self.assertEqual(hub.username, "registry-user")

    def test_helper_wins_and_does_not_fall_back_to_auths(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["DOCKER_CONFIG"] = tmp
            bindir = Path(tmp) / "bin"
            bindir.mkdir()
            helper = bindir / "docker-credential-packtest"
            helper.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys\n"
                "data = sys.stdin.buffer.read()\n"
                "assert data == b'127.0.0.1:9'\n"
                "json.dump({'Username': 'registry-user', 'Secret': 'basic-value'}, sys.stdout)\n",
                encoding="utf-8",
            )
            helper.chmod(0o755)
            empty = bindir / "docker-credential-empty"
            empty.write_text(
                "#!/usr/bin/env python3\n"
                "import sys\n"
                "sys.stderr.write('credentials not found in native keychain\\n')\n"
                "sys.exit(1)\n",
                encoding="utf-8",
            )
            empty.chmod(0o755)
            os.environ["PATH"] = str(bindir) + os.pathsep + os.environ.get("PATH", "")
            encoded = base64.b64encode(b"registry-user:basic-value").decode("ascii")
            Path(tmp, "config.json").write_text(
                json.dumps(
                    {
                        "credHelpers": {"127.0.0.1:9": "packtest"},
                        "auths": {"127.0.0.1:9": {"auth": encoded}},
                    }
                ),
                encoding="utf-8",
            )
            cred = load_credential("127.0.0.1:9")
            assert cred is not None
            self.assertEqual(cred.username, "registry-user")
            Path(tmp, "config.json").write_text(
                json.dumps(
                    {
                        "credsStore": "empty",
                        "auths": {"127.0.0.1:9": {"auth": encoded}},
                    }
                ),
                encoding="utf-8",
            )
            self.assertIsNone(load_credential("127.0.0.1:9"))

    def test_helper_identity_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["DOCKER_CONFIG"] = tmp
            bindir = Path(tmp) / "bin"
            bindir.mkdir()
            helper = bindir / "docker-credential-packtest"
            helper.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys\n"
                "json.dump({'Username': '<token>', 'Secret': 'refresh-value'}, sys.stdout)\n",
                encoding="utf-8",
            )
            helper.chmod(0o755)
            os.environ["PATH"] = str(bindir) + os.pathsep + os.environ.get("PATH", "")
            Path(tmp, "config.json").write_text(
                json.dumps({"credHelpers": {"127.0.0.1:9": "packtest"}}),
                encoding="utf-8",
            )
            cred = load_credential("127.0.0.1:9")
            assert cred is not None
            self.assertEqual(cred.username, "")
            self.assertEqual(cred.refresh_token, "refresh-value")
            self.assertEqual(cred.access_token, "")


if __name__ == "__main__":
    unittest.main()

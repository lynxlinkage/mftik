# mftik

Phase 1 is an STS Python SDK, written in Rust and exposed with PyO3, that is compatible with [mftiko](https://github.com/BullionBear/mftiko). Acceptance is `mftiko-conformance` passing every check in mftiko PROTOCOL §9.

The Python package name is `mftik` (`import mftik`). Version 0.14.0 was the first Rust rewrite and is incompatible with the earlier API. Version 1.0.0 is the first stable release, after a Python strategy ran on the mftiko testnet (E05-06). Older releases already on PyPI, including 0.12.x, stay there. The tree before this rewrite is the `legacy-final` tag.

Decisions for this repo are in [`docs/DECISIONS.md`](docs/DECISIONS.md).

## Crates

| Crate | Path | Role |
| --- | --- | --- |
| `mftik-proto` | `crates/mftik-proto` | Prost types for the pinned mftiko protos. Generated code stays out of git. |
| `mftik-core` | `crates/mftik-core` | Pure Rust algorithms. No IO. |
| `mftik-py` | `crates/mftik-py` | PyO3 bindings. The Python package is `mftik` (`python/mftik`); the native module is `mftik._mftik`. |
| `mftik-rt` | `crates/mftik-rt` | Session worker runtime: fd 3, C4, the dispatcher, NATS, and timers. `Strategy` and `Context`. |
| `mftik-testkit` | `crates/mftik-testkit` | Dev-only nats-server and fake-shim harness (`publish = false`). |
| `mftik-conform` | `examples/conform` | The Rust standard conformance strategy (PROTOCOL §9.8), the `conform` binary. |

The Python API is snake_case and matches the Go SDK's hook and context names. The user guide and API reference are in [`docs/python`](docs/python/README.md).

## Proto

`contract/proto/mftiko` is a copy of BullionBear/mftiko `contract/proto/mftiko` at `5fbfd14ae61a5131326df67ce14a0a79f59c7fd1`. [`scripts/sync-proto`](scripts/sync-proto) reproduces that copy and writes the source commit to `contract/proto/SOURCE`.

```bash
scripts/sync-proto          # copy again and rewrite SOURCE
scripts/sync-proto --check  # fail if the tree or SOURCE drifts from the pin
scripts/sync-proto-negative # require --check to fail on a drifted temporary copy
```

## Build

[`rust-toolchain.toml`](rust-toolchain.toml) pins the current stable toolchain (1.99.1). Crates use edition 2024.

```bash
cargo build --workspace
```

`mftik-proto`'s `build.rs` runs `prost-build` with `protoc` from `protoc-bin-vendored`. Generated Rust is written under `target/` and is not committed.

Python uses [uv](https://docs.astral.sh/uv/) and [maturin](https://www.maturin.rs/). Lint with [ruff](https://docs.astral.sh/ruff/).

```bash
uv sync
uv run maturin develop
uv run ruff check python tests/python examples/python crates/mftik-py/tests/pystrategies
uv run ruff format --check python tests/python examples/python crates/mftik-py/tests/pystrategies
```

`mftik` requires Python 3.11 or newer (abi3). A session worker starts with `python -m mftik.run module:Class`. `Class` is constructed with no arguments and must be a `mftik.Strategy`.

## CI

GitHub Actions runs on `ubuntu-latest`, except the Linux release wheel and `conformance-rootfs-rust`, which run on `ubuntu-22.04` (below). `cargo clippy --workspace --all-targets -- -D warnings` fails the build on warnings. The workflow also checks formatting, tests, and that the proto tree still matches the pinned commit. [`scripts/sync-proto-negative`](scripts/sync-proto-negative) appends a line to a temporary copy of that tree and requires `scripts/sync-proto --check` to fail.

The Python job uses Python 3.11 (E04-D014). It syncs the locked uv tools, checks that [`docs/python`](docs/python/README.md) names every public `mftik` symbol and that any code excerpt matches [`examples/python`](examples/python), runs `ruff check` and `ruff format --check`, builds an `mftik` wheel with maturin, installs that wheel into a fresh environment, and runs [`tests/python/smoke_import.py`](tests/python/smoke_import.py), the Python unit tests, `mypy.stubtest` against `mftik._mftik`, and the Python session end-to-end tests.

The wheels job builds the two release wheels on every pull request and installs each into a clean virtual environment for the same smoke test (E04-D063). The supported platforms are Ubuntu 22.04 or later on x86_64, and macOS on arm64 (E06-D018). The Linux wheel is `manylinux_2_35_x86_64`: `maturin build --compatibility manylinux_2_35` runs directly on the pinned `ubuntu-22.04` runner, whose glibc is 2.35, with no manylinux container (E06-D019). The macOS wheel is built on `macos-latest`. Both are abi3 wheels for Python 3.11 (`cp311-abi3`). There is no source distribution. [`scripts/release-check`](scripts/release-check) checks that the workspace version, `pyproject.toml`, the smoke test's expected version, and `mftik.__version__` match.

A `v*` tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml). That workflow builds those wheels, smokes them, and uploads them to PyPI with trusted publishing from the GitHub environment `pypi`. The tag is `v` plus the package version, and the tagged commit is on `main`. The project owner registers that workflow filename and environment on the existing PyPI project `mftik` before the tag is pushed. The first tag was `v0.14.0`. Each tag is pushed after its release PR is merged and Yi Te confirms.

The `mftiko-conformance` job runs the suite on every pull request. [`scripts/install-mftiko-conformance`](scripts/install-mftiko-conformance) is the `go install` of `mftiko-conformance` from the pinned mftiko commit. The job runs it against the Rust standard strategy [`examples/conform`](examples/conform), keeps `report.json` as an artifact, and fails on any failed check. To run it locally (Go 1.26 or newer):

```bash
GOBIN="$PWD/bin" scripts/install-mftiko-conformance
cargo build --release -p mftik-conform
bin/mftiko-conformance -json report.json -- target/release/conform
```

`conformance-rootfs-rust` packs that binary with the pinned `mftiko-pack -bin` (E08-D001). Every binary that calls `mftik_rt::run` carries what the packer reads: the `mftiko.contracts=` marker, and on Linux a Go build info record that gives the manifest's `sdk.version` (E08-D005). [`tests/python/conformance_rootfs_rust.py`](tests/python/conformance_rootfs_rust.py) pushes the image to a registry on 127.0.0.1, pulls and unpacks it with the pinned mftiko artifact package, and runs the suite in the unpacked rootfs. The job builds and runs the binary on `ubuntu-22.04`, whose glibc is 2.35 (E08-D002). Locally it needs Docker, or `MFTIK_REGISTRY_BIN` pointing at a `registry` binary:

```bash
MFTIK_CONFORMANCE="$PWD/bin/mftiko-conformance" python3 tests/python/conformance_rootfs_rust.py
```

The Rust writer of the strategy event log (PROTOCOL §12) is checked against mftiko's own reader: [`scripts/install-mftiko-eventlog`](scripts/install-mftiko-eventlog) is the `go install` of `mftiko-eventlog` from the same pinned commit, and the rust job points `MFTIK_EVENTLOG` at it for `crates/mftik-rt/tests/eventlog_dump.rs`. Those tests fail when the tool is missing. Locally:

```bash
GOBIN="$PWD/bin" scripts/install-mftiko-eventlog
MFTIK_EVENTLOG="$PWD/bin/mftiko-eventlog" cargo test -p mftik-rt --test eventlog_dump
```

The Python standard strategy is [`examples/python/conform`](examples/python/conform), started with `python -m mftik.run conform:Conform`. `python-conform-wheel` builds one abi3 wheel. `mftiko-conformance-python` installs that wheel into a clean venv and runs the same suite on Python 3.11, 3.12, and 3.13 in parallel. Each report is an artifact. The pinned harness replaces the worker environment and does not forward `PYTHONPATH`, so the venv's `mftik-conform.pth` adds `examples/python` to `sys.path`. Locally, with the wheel installed in `.venv`:

```bash
site="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
printf '%s\n' "$PWD/examples/python" > "$site/mftik-conform.pth"
bin/mftiko-conformance -json report.json -- .venv/bin/python -m mftik.run conform:Conform
```

Example strategies are [`md_watch`](examples/python/md_watch) (market data), [`maker`](examples/python/maker) (orders), [`stateful`](examples/python/stateful) (a fill count kept in the volume), [`pretrained`](examples/python/pretrained) (a read-only input), and [`sampler`](examples/python/sampler) (`now` and `rand`). Each directory has an English README and `test_e2e.py`. The `examples` CI job installs the `mftik-abi3-wheel` artifact, runs those tests, and packs each strategy once without pushing.

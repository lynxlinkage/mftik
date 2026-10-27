# mftik

Phase 1 is an STS Python SDK, written in Rust and exposed with PyO3, that is compatible with [mftiko](https://github.com/BullionBear/mftiko). Acceptance is `mftiko-conformance` passing every check in mftiko PROTOCOL §9.

The Python package name is `mftik` (`import mftik`). Version 0.14.0 is a Rust rewrite and is incompatible with the previous API. Older releases already on PyPI, including 0.12.x, stay there. The tree before this rewrite is the `legacy-final` tag.

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

`contract/proto/mftiko` is a copy of BullionBear/mftiko `contract/proto/mftiko` at `7a8c644931095539a88c2db6cc6a0791c7bc70d8`. [`scripts/sync-proto`](scripts/sync-proto) reproduces that copy and writes the source commit to `contract/proto/SOURCE`.

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

GitHub Actions runs on `ubuntu-latest`. `cargo clippy --workspace --all-targets -- -D warnings` fails the build on warnings. The workflow also checks formatting, tests, and that the proto tree still matches the pinned commit. [`scripts/sync-proto-negative`](scripts/sync-proto-negative) appends a line to a temporary copy of that tree and requires `scripts/sync-proto --check` to fail.

The Python job uses Python 3.11 (E04-D014). It syncs the locked uv tools, checks that [`docs/python`](docs/python/README.md) names every public `mftik` symbol and that any code excerpt matches [`examples/python`](examples/python), runs `ruff check` and `ruff format --check`, builds an `mftik` wheel with maturin, installs that wheel into a fresh environment, and runs [`tests/python/smoke_import.py`](tests/python/smoke_import.py), the Python unit tests, `mypy.stubtest` against `mftik._mftik`, and the Python session end-to-end tests.

The wheels job builds the two release wheels on every pull request and installs each into a clean virtual environment for the same smoke test (E04-D063): manylinux_2_28 x86_64 and macOS arm64. Both are abi3 wheels for Python 3.11 (`cp311-abi3`). There is no source distribution. [`scripts/release-check`](scripts/release-check) checks that the workspace version, `pyproject.toml`, the smoke test's expected version, and `mftik.__version__` match.

A `v*` tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml). That workflow builds those wheels, smokes them, and uploads them to PyPI with trusted publishing from the GitHub environment `pypi`. The tag is `v` plus the package version, and the tagged commit is on `main`. The project owner registers that workflow filename and environment on the existing PyPI project `mftik` before the tag is pushed. The first tag is `v0.14.0`, after this work is merged and Yi Te confirms.

The `mftiko-conformance` job runs the suite on every pull request. [`scripts/install-mftiko-conformance`](scripts/install-mftiko-conformance) is the `go install` of `mftiko-conformance` from the pinned mftiko commit. The job runs it against the Rust standard strategy [`examples/conform`](examples/conform), keeps `report.json` as an artifact, and fails on any failed check. To run it locally (Go 1.26 or newer):

```bash
GOBIN="$PWD/bin" scripts/install-mftiko-conformance
cargo build --release -p mftik-conform
bin/mftiko-conformance -json report.json -- target/release/conform
```

The Python standard strategy is [`examples/python/conform`](examples/python/conform), started with `python -m mftik.run conform:Conform`. `python-conform-wheel` builds one abi3 wheel. `mftiko-conformance-python` installs that wheel into a clean venv and runs the same suite on Python 3.11, 3.12, and 3.13 in parallel. Each report is an artifact. The pinned harness replaces the worker environment and does not forward `PYTHONPATH`, so the venv's `mftik-conform.pth` adds `examples/python` to `sys.path`. Locally, with the wheel installed in `.venv`:

```bash
site="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
printf '%s\n' "$PWD/examples/python" > "$site/mftik-conform.pth"
bin/mftiko-conformance -json report.json -- .venv/bin/python -m mftik.run conform:Conform
```

Example strategies are [`md_watch`](examples/python/md_watch) (market data), [`maker`](examples/python/maker) (orders), and [`stateful`](examples/python/stateful) (a fill count kept in the volume). Each directory has an English README and `test_e2e.py`. The `examples` CI job installs the `mftik-abi3-wheel` artifact, runs those tests, and packs each strategy once without pushing.

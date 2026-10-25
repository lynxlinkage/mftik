# mftik

Phase 1 is an STS Python SDK, written in Rust and exposed with PyO3, that is compatible with [mftiko](https://github.com/BullionBear/mftiko). Acceptance is `mftiko-conformance` passing every check in mftiko PROTOCOL §9.

The Python package name is `mftik` (`import mftik`). The version is 0.12.0. Publishing waits until E04. The tree before this rewrite is the `legacy-final` tag.

Decisions for this repo are in [`docs/DECISIONS.md`](docs/DECISIONS.md).

## Crates

| Crate | Path | Role |
| --- | --- | --- |
| `mftik-proto` | `crates/mftik-proto` | Prost types for the pinned mftiko protos. Generated code stays out of git. |
| `mftik-core` | `crates/mftik-core` | Pure Rust algorithms. No IO. |
| `mftik-py` | `crates/mftik-py` | PyO3 module. The import name is `mftik`. |
| `mftik-rt` | `crates/mftik-rt` | Session worker runtime: fd 3, C4, the dispatcher, NATS, and timers. `Strategy` and `Context`. |
| `mftik-conform` | `examples/conform` | The Rust standard conformance strategy (PROTOCOL §9.8), the `conform` binary. |

The Python API will be snake_case and one-to-one with the Go SDK.

## Proto

`contract/proto/mftiko` is a copy of BullionBear/mftiko `contract/proto/mftiko` at `e7b3543e2269c4ae8c10631edd448bdb86c52e2c`. [`scripts/sync-proto`](scripts/sync-proto) reproduces that copy and writes the source commit to `contract/proto/SOURCE`.

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
uv run ruff check python
uv run ruff format --check python
```

`mftik` requires Python 3.11 or newer (abi3).

## CI

GitHub Actions runs on `ubuntu-latest`. `cargo clippy --workspace --all-targets -- -D warnings` fails the build on warnings. The workflow also checks formatting, tests, and that the proto tree still matches the pinned commit. [`scripts/sync-proto-negative`](scripts/sync-proto-negative) appends a line to a temporary copy of that tree and requires `scripts/sync-proto --check` to fail.

The Python job syncs the locked uv tools, runs `ruff check` and `ruff format --check`, builds an `mftik` wheel with maturin, installs that wheel into a fresh environment, and runs [`python/smoke_import.py`](python/smoke_import.py).

The `mftiko-conformance` job runs the suite on every pull request. [`scripts/install-mftiko-conformance`](scripts/install-mftiko-conformance) is the `go install` of `mftiko-conformance` from the pinned mftiko commit. The job runs it against the Rust standard strategy [`examples/conform`](examples/conform), keeps `report.json` as an artifact, and fails on any failed check. To run it locally (Go 1.26 or newer):

```bash
GOBIN="$PWD/bin" scripts/install-mftiko-conformance
cargo build --release -p mftik-conform
bin/mftiko-conformance -json report.json -- target/release/conform
```

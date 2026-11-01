# Install and run

`mftik-rt` is the Rust strategy SDK. A session worker is a binary that calls `mftik_rt::run`. The supported worker platform is Ubuntu 22.04 or later on x86_64 (`x86_64-unknown-linux-gnu`, glibc 2.35 or newer). There is no musl build. macOS on arm64 is for development; the packed image the platform runs is always linux amd64.

The crate is not on crates.io (E08-D004). Depend on it by git tag. The tag has to be a release after 1.2.0: v1.2.0 does not embed the `mftiko.contracts=` marker, the `mftiko.sdk=` record, or the Go build info that `mftiko-pack -bin` reads (E08-D001, E08-D005), so packing a binary linked to that tag fails.

```toml
[package]
name = "hello"
edition = "2024"
rust-version = "1.99.1"
publish = false

[[bin]]
name = "hello"
path = "src/main.rs"

[dependencies]
mftik-rt = { git = "https://github.com/lynxlinkage/mftik", tag = "vX.Y.Z", package = "mftik-rt" }
```

Replace `vX.Y.Z` with that later tag. `package` is required because the repository is a workspace. People working in this repository use a path dependency, as [`examples/rust/md_watch`](../../examples/rust/md_watch) does.

The toolchain is the stable channel pinned in [`rust-toolchain.toml`](../../rust-toolchain.toml) (1.99.1). Crates use edition 2024. `mftik-rt` pulls `mftik-core` and `mftik-proto`; `mftik-proto`'s build uses a vendored `protoc`.

## The process

The platform starts one process: the packed `bin/strategy`, with the control socket on fd 3 (`MFTIKO_CTL_FD`). `run` hosts the strategy and does not return. The process ends with the status in [Lifecycle](lifecycle.md).

```rust no_run
use mftik_rt::{Context, Handlers, HookError, Strategy, Trade, TradeHandler};

struct Hello {
    uid: u32,
}

impl Strategy for Hello {
    fn on_init(&mut self, ctx: &Context<Self>) -> Result<(), HookError> {
        self.uid = ctx.resolve("Ex_Spot_ABC")?.uid;
        Ok(())
    }

    fn on_start(&mut self, _ctx: &Context<Self>) -> Result<(), HookError> {
        Ok(())
    }

    fn on_stop(&mut self, _ctx: &Context<Self>) {}

    fn handlers(&self, h: &mut Handlers<Self>) {
        h.trade();
    }
}

impl TradeHandler for Hello {
    fn on_trade(&mut self, _ctx: &Context<Self>, ev: Trade) {
        let _ = (ev.md.uid, ev.data.price, ev.data.qty);
    }
}

fn main() {
    mftik_rt::run(Hello { uid: 0 }, None)
}
```

`run` takes the strategy value and an optional `release` string. `release` is sent as `Hello.release` and shown as the worker's release. `None` or an empty string uses `mftik_rt::RELEASE`, this crate's package version. Construct the value before `run`; configuration belongs in `on_init`, read from `params`. [`examples/rust/md_watch`](../../examples/rust/md_watch) reads JSON there.

```bash
cargo build --release
```

The binary started from a shell, with no control socket on fd 3, prints `control fd: control fd 3: ...` on stderr and exits 1. That is expected. See [Pack and deploy](pack.md).

Keep `panic = "unwind"` (Rust's default). `catch_unwind` turns a hook panic into `INIT_FAILED` or `CRASHED` so `on_stop` can run. `panic = "abort"` in a profile makes a hook panic a signal kill; the platform then records no outcome from the table in [Lifecycle](lifecycle.md).

## Logging

`mftik_rt::log::emit` writes one line to stderr. The level and the layout come from `MFTIKO_LOG_LEVEL` and `MFTIKO_LOG_FORMAT`. JSON is the default; a trimmed, case-insensitive `console` selects zerolog's console layout. Inside a hook, pass `Some(ctx.session_id())`. A `None` session id omits the key.

```rust
use mftik_rt::log::{emit, enabled, Field, Level, Value};

fn on_start_line(session_id: &str) {
    if !enabled(Level::Info) {
        return;
    }
    emit(
        Level::Info,
        "OnStart",
        &[Field {
            name: "hook".into(),
            value: Value::Str("OnStart".into()),
        }],
        Some(session_id),
    );
}
```

A JSON line is one object: `level`, `time`, `component` (`strategy`), `version`, `commit`, `sdk_version`, then `session_id` when it was `Some`, then the caller's fields, then `message`. `version` and `commit` are empty unless the process calls `mftik_rt::log::set_strategy_identity`. `sdk_version` is this crate's package version and git commit. `Value` is `Str`, `Int`, `Big` (decimal text of an integer that does not fit in `i64`), `Float`, `Bool`, or `Null`. Check `enabled` before building fields.

Do not put secrets in log lines or in `HookError` text. That text is copied into the session's error fields.

## What the strategy is given

`on_init` runs after the process is up and before market data or accounts are ready. Read `params` there, resolve the instruments the strategy trades, and read files from the volume. `on_start` runs once every market-data and account lease is ready. Orders may be placed from `on_start` onward. From then until the session ends, optional hooks deliver market data and account events. The rules for calling the SDK from those hooks are in [Context](context.md).

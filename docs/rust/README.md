# Rust SDK

`mftik-rt` is the strategy SDK for an mftiko session. A process that calls `mftik_rt::run` is one session worker. These pages are the user guide. They are hand-written Markdown. They are not generated from rustdoc, and there is no mdBook site.

| Page | What it covers |
| --- | --- |
| [Install and run](install.md) | The crate, `mftik_rt::run`, and what the process has to be |
| [Lifecycle](lifecycle.md) | `on_init`, `on_start`, `on_stop`, and the 17 optional hooks, with the Go names |
| [Context](context.md) | What a hook may call: instruments, orders, reads, timers, the volume |
| [Pack and deploy](pack.md) | `mftiko-pack -bin`, the image, and running a session with `mftiko` |

A hook runs on one thread, one at a time. It must return. The only object that may cross threads is a `Poster`. `Context` and `BookView` work only inside the hook that received them.

The full example is [`examples/rust/md_watch`](../../examples/rust/md_watch): it resolves one instrument, logs trades and the local book, and keeps counts in the volume. Rust snippets on these pages are compiled by `cargo test --doc -p mftik-rt` (E08-D008).
